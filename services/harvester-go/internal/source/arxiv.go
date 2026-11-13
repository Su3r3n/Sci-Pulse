package source

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/scipulse/scipulse/services/harvester-go/internal/cache"
	"github.com/scipulse/scipulse/services/harvester-go/internal/metrics"
	"github.com/scipulse/scipulse/services/harvester-go/internal/ratelimit"
)

const (
	defaultArxivBaseURL   = "https://export.arxiv.org/oai2"
	minArxivPoliteDelay   = 3 * time.Second
	defaultRetryAfterWait = 5 * time.Second
)

// ArxivConfig configures the arXiv OAI-PMH source.
type ArxivConfig struct {
	BaseURL        string
	Sets           []string
	MetadataPrefix string
	PoliteDelay    time.Duration
	Max503Retries  int
	Offline        bool
	HTTPClient     *http.Client
	Cache          cache.Store
	Limiter        *ratelimit.TokenBucket
	Metrics        *metrics.Metrics
	Logger         *slog.Logger
}

// ArxivSource fetches paper records via OAI-PMH ListRecords.
type ArxivSource struct {
	cfg ArxivConfig
}

// NewArxivSource creates an ArxivSource with polite defaults (>=3s delay).
func NewArxivSource(cfg ArxivConfig) *ArxivSource {
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultArxivBaseURL
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.MetadataPrefix == "" {
		cfg.MetadataPrefix = "arXiv"
	}
	if cfg.PoliteDelay == 0 {
		cfg.PoliteDelay = minArxivPoliteDelay
	}
	if cfg.Max503Retries <= 0 {
		cfg.Max503Retries = 5
	}
	if cfg.Limiter == nil && cfg.PoliteDelay > 0 {
		cfg.Limiter = ratelimit.NewIntervalLimiter(cfg.PoliteDelay, 1)
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 45 * time.Second}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &ArxivSource{cfg: cfg}
}

// Name returns "arxiv".
func (s *ArxivSource) Name() string {
	return "arxiv"
}

type oaiEnvelope struct {
	XMLName     xml.Name        `xml:"OAI-PMH"`
	Error       *oaiError       `xml:"error"`
	ListRecords *oaiListRecords `xml:"ListRecords"`
}

type oaiError struct {
	Code    string `xml:"code,attr"`
	Message string `xml:",chardata"`
}

type oaiListRecords struct {
	Records         []oaiRecord         `xml:"record"`
	ResumptionToken *oaiResumptionToken `xml:"resumptionToken"`
}

type oaiResumptionToken struct {
	Value string `xml:",chardata"`
}

type oaiRecord struct {
	XMLName  xml.Name `xml:"record"`
	Header   oaiHeader `xml:"header"`
	InnerXML []byte   `xml:",innerxml"`
}

type oaiHeader struct {
	Status     string `xml:"status,attr"`
	Identifier string `xml:"identifier"`
	Datestamp  string `xml:"datestamp"`
}

// Fetch streams arXiv OAI-PMH records as RawRecords.
func (s *ArxivSource) Fetch(ctx context.Context, job Job) (<-chan RawRecord, error) {
	out := make(chan RawRecord, 64)
	go func() {
		defer close(out)
		if s.cfg.Offline {
			s.fetchOffline(ctx, out)
			return
		}
		s.fetchOnline(ctx, job, out)
	}()
	return out, nil
}

func (s *ArxivSource) fetchOffline(ctx context.Context, out chan<- RawRecord) {
	if s.cfg.Cache == nil {
		s.cfg.Logger.Warn("offline mode enabled without cache store", "source", "arxiv")
		return
	}
	pages, err := s.cfg.Cache.List("arxiv")
	if err != nil {
		s.cfg.Logger.Error("arxiv offline cache list failed", "error", err)
		return
	}
	now := time.Now().UTC()
	for idx, page := range pages {
		if ctx.Err() != nil {
			return
		}
		s.cfg.Metrics.IncCacheHit("arxiv")
		s.emitPage(ctx, page, now, fmt.Sprintf("offline-%d", idx), out)
	}
}

func (s *ArxivSource) fetchOnline(ctx context.Context, job Job, out chan<- RawRecord) {
	sets := s.cfg.Sets
	if len(sets) == 0 && len(job.Topics) > 0 {
		sets = job.Topics
	}
	if len(sets) == 0 {
		sets = []string{""}
	}

	for _, set := range sets {
		if ctx.Err() != nil {
			return
		}
		s.fetchSet(ctx, set, job.Since, out)
	}
}

func (s *ArxivSource) fetchSet(ctx context.Context, set, since string, out chan<- RawRecord) {
	token := ""
	for {
		if ctx.Err() != nil {
			return
		}
		reqURL := s.buildURL(set, since, token)
		body, ok := s.getWithRetryAfter(ctx, reqURL)
		if !ok {
			return
		}

		if s.cfg.Cache != nil {
			if err := s.cfg.Cache.Put("arxiv", reqURL, body); err != nil {
				s.cfg.Logger.Warn("arxiv cache put failed", "error", err)
			}
		}

		nextToken, count := s.emitPage(ctx, body, time.Now().UTC(), reqURL, out)
		if nextToken == "" || count == 0 {
			return
		}
		token = nextToken
	}
}

func (s *ArxivSource) getWithRetryAfter(ctx context.Context, reqURL string) ([]byte, bool) {
	for attempt := 0; attempt <= s.cfg.Max503Retries; attempt++ {
		if s.cfg.Limiter != nil {
			if err := s.cfg.Limiter.Wait(ctx); err != nil {
				return nil, false
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			s.cfg.Logger.Error("arxiv build request failed", "error", err)
			return nil, false
		}
		req.Header.Set("User-Agent", "SciPulse-Harvester/1.0")

		resp, err := s.cfg.HTTPClient.Do(req)
		if err != nil {
			s.cfg.Logger.Error("arxiv request failed", "url", reqURL, "error", err)
			return nil, false
		}

		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		_ = resp.Body.Close()
		s.cfg.Metrics.IncHTTPRequest("arxiv", resp.StatusCode)

		if readErr != nil {
			s.cfg.Logger.Error("arxiv read body failed", "error", readErr)
			return nil, false
		}

		if resp.StatusCode == http.StatusServiceUnavailable {
			wait := parseRetryAfter(resp.Header.Get("Retry-After"))
			s.cfg.Logger.Warn("arxiv 503 service unavailable, respecting Retry-After", "wait", wait, "attempt", attempt+1)
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, false
			case <-timer.C:
			}
			continue
		}

		if resp.StatusCode != http.StatusOK {
			s.cfg.Logger.Error("arxiv unexpected HTTP status", "status", resp.StatusCode, "url", reqURL)
			return nil, false
		}
		return body, true
	}

	s.cfg.Logger.Error("arxiv exhausted 503 retries", "url", reqURL)
	return nil, false
}

func (s *ArxivSource) emitPage(ctx context.Context, page []byte, fetchedAt time.Time, fallbackID string, out chan<- RawRecord) (nextToken string, count int) {
	var env oaiEnvelope
	if err := xml.Unmarshal(page, &env); err != nil {
		rec := RawRecord{
			Source:     "arxiv",
			ID:         fallbackID,
			Body:       page,
			Fetched:    fetchedAt,
			ParseError: fmt.Errorf("arxiv xml decode: %w", err),
		}
		select {
		case out <- rec:
		case <-ctx.Done():
		}
		return "", 0
	}

	if env.Error != nil {
		if env.Error.Code == "noRecordsMatch" {
			return "", 0
		}
		s.cfg.Logger.Warn("arxiv OAI-PMH error", "code", env.Error.Code, "message", strings.TrimSpace(env.Error.Message))
		return "", 0
	}

	if env.ListRecords == nil {
		return "", 0
	}

	if env.ListRecords.ResumptionToken != nil {
		nextToken = strings.TrimSpace(env.ListRecords.ResumptionToken.Value)
	}

	for idx, rec := range env.ListRecords.Records {
		if strings.EqualFold(rec.Header.Status, "deleted") {
			continue
		}
		recID := strings.TrimSpace(rec.Header.Identifier)
		if recID == "" {
			recID = fmt.Sprintf("%s#%d", fallbackID, idx)
		}
		rawRec := append([]byte("<record>"), rec.InnerXML...)
		rawRec = append(rawRec, []byte("</record>")...)

		outRec := RawRecord{
			Source:  "arxiv",
			ID:      recID,
			Body:    rawRec,
			Fetched: fetchedAt,
			Cursor:  strings.TrimSpace(rec.Header.Datestamp),
		}
		select {
		case out <- outRec:
			count++
		case <-ctx.Done():
			return "", count
		}
	}

	return nextToken, count
}

func (s *ArxivSource) buildURL(set, since, resumptionToken string) string {
	u, _ := url.Parse(s.cfg.BaseURL)
	q := u.Query()
	q.Set("verb", "ListRecords")
	if resumptionToken != "" {
		q.Set("resumptionToken", resumptionToken)
	} else {
		q.Set("metadataPrefix", s.cfg.MetadataPrefix)
		if strings.TrimSpace(set) != "" {
			q.Set("set", strings.TrimSpace(set))
		}
		if date := normalizeOAIDate(since); date != "" {
			q.Set("from", date)
		}
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func normalizeOAIDate(since string) string {
	since = strings.TrimSpace(since)
	if since == "" {
		return ""
	}
	if len(since) >= 10 {
		if _, err := time.Parse("2006-01-02", since[:10]); err == nil {
			return since[:10]
		}
	}
	return ""
}

func parseRetryAfter(val string) time.Duration {
	val = strings.TrimSpace(val)
	if val == "" {
		return defaultRetryAfterWait
	}
	if secs, err := strconv.Atoi(val); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	if ts, err := http.ParseTime(val); err == nil {
		dur := time.Until(ts)
		if dur > 0 {
			return dur
		}
		return time.Second
	}
	return defaultRetryAfterWait
}
