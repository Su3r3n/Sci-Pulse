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
	XMLName  xml.Name  `xml:"record"`
	Header   oaiHeader `xml:"header"`
	InnerXML []byte    `xml:",innerxml"`
}

type oaiHeader struct {
	Status     string `xml:"status,attr"`
	Identifier string `xml:"identifier"`
	Datestamp  string `xml:"datestamp"`
}

// OrderedCursor reports false: OAI-PMH does not order records by datestamp, so the cursor
// of an arXiv set moves only after the whole set has been acknowledged.
func (s *ArxivSource) OrderedCursor() bool {
	return false
}

// SplitJobs returns one Job per OAI set, so every set keeps its own cursor scope (§5.4).
// A job that already names sets is split by those names; otherwise the configured sets are used.
//
// Offline, the cache holds every set and fetchOffline replays it in full, so the job is not split:
// splitting would replay the same cache once per set and multiply the records (R3).
func (s *ArxivSource) SplitJobs(job Job) []Job {
	if s.cfg.Offline {
		return []Job{job}
	}
	sets := job.Topics
	if len(sets) == 0 {
		sets = s.cfg.Sets
	}
	out := make([]Job, 0, len(sets))
	for _, set := range sets {
		set = strings.TrimSpace(set)
		if set == "" {
			continue
		}
		j := job
		j.Project = "" // the scope of an arXiv job is its set; a project slug does not apply here
		j.Topics = []string{set}
		out = append(out, j)
	}
	if len(out) == 0 {
		return []Job{job}
	}
	return out
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
		// Broken cached pages are emitted as ParseError records (DLQ); offline runs continue.
		if _, _, err := s.emitPage(ctx, page, now, fmt.Sprintf("offline-%d", idx), out); err != nil && ctx.Err() == nil {
			s.cfg.Logger.Warn("arxiv offline page framing error", "page", idx, "error", err)
		}
	}
}

func (s *ArxivSource) fetchOnline(ctx context.Context, job Job, out chan<- RawRecord) {
	sets := job.Topics
	if len(sets) == 0 {
		sets = s.cfg.Sets
	}
	if len(sets) == 0 {
		sets = []string{""}
	}

	for _, set := range sets {
		if ctx.Err() != nil {
			return
		}
		if err := s.fetchSet(ctx, set, job.Since, out); err != nil {
			if ctx.Err() == nil {
				sendFatal(ctx, out, "arxiv", err)
			}
			return
		}
	}
}

// fetchSet walks every resumptionToken page of one set. It returns an error when a page
// cannot be fetched or parsed, because the set is then incomplete and must not be treated as done.
func (s *ArxivSource) fetchSet(ctx context.Context, set, since string, out chan<- RawRecord) error {
	token := ""
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		reqURL := s.buildURL(set, since, token)
		body, err := s.getWithRetryAfter(ctx, reqURL)
		if err != nil {
			return fmt.Errorf("arxiv set %q: %w", set, err)
		}

		if s.cfg.Cache != nil {
			if err := s.cfg.Cache.Put("arxiv", reqURL, body); err != nil {
				s.cfg.Logger.Warn("arxiv cache put failed", "error", err)
			}
		}

		// Pagination depends only on the resumptionToken, never on how many records were emitted:
		// a page full of deleted records must not end the set early.
		next, _, err := s.emitPage(ctx, body, time.Now().UTC(), reqURL, out)
		if err != nil {
			return fmt.Errorf("arxiv set %q: %w", set, err)
		}
		if next == "" {
			return nil
		}
		if next == token {
			return fmt.Errorf("arxiv set %q: resumptionToken did not advance", set)
		}
		token = next
	}
}

// getWithRetryAfter performs one OAI request, honouring 503 + Retry-After up to Max503Retries times.
// Any other non-200 status is an error.
func (s *ArxivSource) getWithRetryAfter(ctx context.Context, reqURL string) ([]byte, error) {
	for attempt := 0; attempt <= s.cfg.Max503Retries; attempt++ {
		if s.cfg.Limiter != nil {
			if err := s.cfg.Limiter.Wait(ctx); err != nil {
				return nil, err
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, fmt.Errorf("arxiv build request: %w", err)
		}
		req.Header.Set("User-Agent", "SciPulse-Harvester/1.0")

		resp, err := s.cfg.HTTPClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("arxiv request: %w", err)
		}

		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		_ = resp.Body.Close()
		s.cfg.Metrics.IncHTTPRequest("arxiv", resp.StatusCode)

		if readErr != nil {
			return nil, fmt.Errorf("arxiv read body: %w", readErr)
		}

		if resp.StatusCode == http.StatusServiceUnavailable {
			wait := parseRetryAfter(resp.Header.Get("Retry-After"))
			s.cfg.Logger.Warn("arxiv 503 service unavailable, respecting Retry-After", "wait", wait, "attempt", attempt+1)
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			continue
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("arxiv HTTP %d for %s", resp.StatusCode, reqURL)
		}
		return body, nil
	}

	return nil, fmt.Errorf("arxiv: 503 retries exhausted (%d attempts) for %s", s.cfg.Max503Retries+1, reqURL)
}

// emitPage sends the records of one OAI-PMH response. It returns the resumptionToken ("" when the
// set is finished) and the number of records sent. A page-level failure (broken XML, OAI error
// other than noRecordsMatch) is returned as an error; a broken page is also emitted as a ParseError
// record so that its raw bytes reach the DLQ.
func (s *ArxivSource) emitPage(ctx context.Context, page []byte, fetchedAt time.Time, fallbackID string, out chan<- RawRecord) (nextToken string, emitted int, err error) {
	var env oaiEnvelope
	if decErr := xml.Unmarshal(page, &env); decErr != nil {
		err = fmt.Errorf("arxiv xml decode: %w", decErr)
		_ = sendRecord(ctx, out, RawRecord{
			Source:     "arxiv",
			ID:         fallbackID,
			Body:       page,
			Fetched:    fetchedAt,
			ParseError: err,
		})
		return "", 0, err
	}

	if env.Error != nil {
		if env.Error.Code == "noRecordsMatch" {
			return "", 0, nil
		}
		return "", 0, fmt.Errorf("arxiv OAI-PMH error %s: %s", env.Error.Code, strings.TrimSpace(env.Error.Message))
	}

	if env.ListRecords == nil {
		return "", 0, fmt.Errorf("arxiv response has neither ListRecords nor error")
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

		if sendErr := sendRecord(ctx, out, RawRecord{
			Source:  "arxiv",
			ID:      recID,
			Body:    rawRec,
			Fetched: fetchedAt,
			Cursor:  strings.TrimSpace(rec.Header.Datestamp),
		}); sendErr != nil {
			return "", emitted, sendErr
		}
		emitted++
	}

	return nextToken, emitted, nil
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
