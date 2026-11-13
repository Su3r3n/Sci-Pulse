package source

import (
	"context"
	"encoding/json"
	"errors"
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

const defaultOpenAlexBaseURL = "https://api.openalex.org"

// OpenAlexConfig configures the OpenAlex REST source.
type OpenAlexConfig struct {
	BaseURL      string
	APIKey       string
	Email        string
	SelectFields string
	PerPage      int
	DailyBudget  *int
	Offline      bool
	HTTPClient   *http.Client
	Cache        cache.Store
	Limiter      *ratelimit.TokenBucket
	Budget       *ratelimit.BudgetTracker
	Metrics      *metrics.Metrics
	Logger       *slog.Logger
}

// OpenAlexSource fetches scholarly works from the OpenAlex REST API.
type OpenAlexSource struct {
	cfg OpenAlexConfig
}

// NewOpenAlexSource creates an OpenAlexSource with sensible defaults.
func NewOpenAlexSource(cfg OpenAlexConfig) *OpenAlexSource {
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultOpenAlexBaseURL
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.PerPage <= 0 || cfg.PerPage > 200 {
		cfg.PerPage = 100
	}
	if cfg.SelectFields == "" {
		cfg.SelectFields = "id,doi,ids,title,abstract_inverted_index,authorships,topics,publication_date,updated_date,cited_by_count,primary_location,open_access,locations"
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &OpenAlexSource{cfg: cfg}
}

// Name returns "openalex".
func (s *OpenAlexSource) Name() string {
	return "openalex"
}

type openAlexResponse struct {
	Meta struct {
		Count      int     `json:"count"`
		NextCursor *string `json:"next_cursor"`
	} `json:"meta"`
	Results []json.RawMessage `json:"results"`
}

type openAlexWorkMeta struct {
	ID          string `json:"id"`
	UpdatedDate string `json:"updated_date"`
}

type openAlexRateLimitResponse struct {
	RateLimit struct {
		DailyLimit     int `json:"daily_limit"`
		DailyUsed      int `json:"daily_used"`
		DailyRemaining int `json:"daily_remaining"`
		// Alternative field names returned by some OpenAlex rate-limit responses.
		DailyBudgetUSD    float64 `json:"daily_budget_usd"`
		DailyRemainingUSD float64 `json:"daily_remaining_usd"`
		CallsRemaining    int     `json:"calls_remaining"`
		CallsLimit        int     `json:"calls_limit"`
	} `json:"rate_limit"`
}

// Fetch streams OpenAlex works as RawRecords.
func (s *OpenAlexSource) Fetch(ctx context.Context, job Job) (<-chan RawRecord, error) {
	out := make(chan RawRecord, 64)

	if s.cfg.Offline {
		go func() {
			defer close(out)
			s.fetchOffline(ctx, out)
		}()
		return out, nil
	}

	if s.cfg.Budget != nil {
		if err := s.checkRateLimit(ctx); err != nil {
			if errors.Is(err, ratelimit.ErrBudgetExhausted) {
				s.cfg.Logger.Warn("openalex daily budget exhausted, stopping fetch", "source", "openalex", "scope", job.Scope())
				close(out)
				return out, nil
			}
			close(out)
			return nil, err
		}
	}

	go func() {
		defer close(out)
		s.fetchOnline(ctx, job, out)
	}()

	return out, nil
}

func (s *OpenAlexSource) fetchOffline(ctx context.Context, out chan<- RawRecord) {
	if s.cfg.Cache == nil {
		s.cfg.Logger.Warn("offline mode enabled without cache store", "source", "openalex")
		return
	}
	pages, err := s.cfg.Cache.List("openalex")
	if err != nil {
		s.cfg.Logger.Error("openalex offline cache list failed", "error", err)
		return
	}
	now := time.Now().UTC()
	for idx, page := range pages {
		if ctx.Err() != nil {
			return
		}
		s.cfg.Metrics.IncCacheHit("openalex")
		s.emitPage(ctx, page, now, fmt.Sprintf("offline-%d", idx), out)
	}
}

func (s *OpenAlexSource) fetchOnline(ctx context.Context, job Job, out chan<- RawRecord) {
	cursor := "*"
	for {
		if ctx.Err() != nil {
			return
		}
		if s.cfg.Budget != nil {
			rem, capped, err := s.cfg.Budget.Check(ctx, "openalex")
			if errors.Is(err, ratelimit.ErrBudgetExhausted) {
				s.cfg.Logger.Warn("openalex daily budget exhausted, stopping pagination", "source", "openalex", "scope", job.Scope())
				return
			}
			if err != nil {
				s.cfg.Logger.Error("openalex budget check failed", "error", err)
				return
			}
			if capped {
				s.cfg.Metrics.SetBudgetRemaining("openalex", rem)
			}
		}

		if s.cfg.Limiter != nil {
			if err := s.cfg.Limiter.Wait(ctx, ); err != nil {
				return
			}
		}

		reqURL := s.buildWorksURL(job, cursor)
		body, statusCode, err := s.doGET(ctx, reqURL)
		if statusCode > 0 {
			s.cfg.Metrics.IncHTTPRequest("openalex", statusCode)
		}
		if err != nil {
			s.cfg.Logger.Error("openalex request failed", "url", redactURL(reqURL), "error", err)
			return
		}

		if s.cfg.Budget != nil {
			rem, capped, err := s.cfg.Budget.Consume(ctx, "openalex", 1, s.cfg.DailyBudget)
			if err != nil && !errors.Is(err, ratelimit.ErrBudgetExhausted) {
				s.cfg.Logger.Warn("openalex budget consume error", "error", err)
			} else if capped {
				s.cfg.Metrics.SetBudgetRemaining("openalex", rem)
			}
		}

		if statusCode == http.StatusTooManyRequests || statusCode >= 500 {
			s.cfg.Logger.Warn("openalex non-200 status", "status", statusCode)
			return
		}
		if statusCode != http.StatusOK {
			s.cfg.Logger.Error("openalex unexpected status", "status", statusCode, "body", string(body))
			return
		}

		if s.cfg.Cache != nil {
			if err := s.cfg.Cache.Put("openalex", reqURL, body); err != nil {
				s.cfg.Logger.Warn("openalex cache put failed", "error", err)
			}
		}

		nextCursor, count := s.emitPage(ctx, body, time.Now().UTC(), reqURL, out)
		if nextCursor == "" || count == 0 {
			return
		}
		cursor = nextCursor
	}
}

func (s *OpenAlexSource) emitPage(ctx context.Context, page []byte, fetchedAt time.Time, fallbackID string, out chan<- RawRecord) (nextCursor string, count int) {
	var resp openAlexResponse
	if err := json.Unmarshal(page, &resp); err != nil {
		rec := RawRecord{
			Source:     "openalex",
			ID:         fallbackID,
			Body:       page,
			Fetched:    fetchedAt,
			ParseError: fmt.Errorf("openalex json decode: %w", err),
		}
		select {
		case out <- rec:
		case <-ctx.Done():
		}
		return "", 0
	}

	// Support single-work JSON fixtures in offline cache as well as list responses.
	if resp.Results == nil {
		var single openAlexWorkMeta
		if err := json.Unmarshal(page, &single); err == nil && single.ID != "" {
			rec := RawRecord{
				Source:  "openalex",
				ID:      single.ID,
				Body:    page,
				Fetched: fetchedAt,
				Cursor:  single.UpdatedDate,
			}
			select {
			case out <- rec:
			case <-ctx.Done():
			}
			return "", 1
		}
	}

	if resp.Meta.NextCursor != nil {
		nextCursor = *resp.Meta.NextCursor
	}

	for idx, rawItem := range resp.Results {
		var meta openAlexWorkMeta
		if err := json.Unmarshal(rawItem, &meta); err != nil {
			rec := RawRecord{
				Source:     "openalex",
				ID:         fmt.Sprintf("%s#%d", fallbackID, idx),
				Body:       rawItem,
				Fetched:    fetchedAt,
				ParseError: fmt.Errorf("openalex work json decode: %w", err),
			}
			select {
			case out <- rec:
			case <-ctx.Done():
				return "", count
			}
			continue
		}

		recID := meta.ID
		if recID == "" {
			recID = fmt.Sprintf("%s#%d", fallbackID, idx)
		}
		cursorVal := meta.UpdatedDate
		if cursorVal == "" {
			cursorVal = nextCursor
		}

		rec := RawRecord{
			Source:  "openalex",
			ID:      recID,
			Body:    rawItem,
			Fetched: fetchedAt,
			Cursor:  cursorVal,
		}
		select {
		case out <- rec:
			count++
		case <-ctx.Done():
			return "", count
		}
	}

	return nextCursor, count
}

func (s *OpenAlexSource) checkRateLimit(ctx context.Context) error {
	if s.cfg.Limiter != nil {
		if err := s.cfg.Limiter.Wait(ctx); err != nil {
			return err
		}
	}

	u, err := url.Parse(s.cfg.BaseURL + "/rate-limit")
	if err != nil {
		return err
	}
	q := u.Query()
	if s.cfg.APIKey != "" {
		q.Set("api_key", s.cfg.APIKey)
	}
	u.RawQuery = q.Encode()

	body, statusCode, err := s.doGET(ctx, u.String())
	if statusCode > 0 {
		s.cfg.Metrics.IncHTTPRequest("openalex", statusCode)
	}
	if err != nil {
		return fmt.Errorf("openalex rate-limit check: %w", err)
	}
	if statusCode == http.StatusNotFound {
		// Some test servers do not implement /rate-limit; rely on local tracker.
		_, _, err := s.cfg.Budget.Check(ctx, "openalex")
		return err
	}
	if statusCode != http.StatusOK {
		return nil
	}

	var rl openAlexRateLimitResponse
	if err := json.Unmarshal(body, &rl); err != nil {
		return nil
	}

	limit := rl.RateLimit.DailyLimit
	remaining := rl.RateLimit.DailyRemaining
	used := rl.RateLimit.DailyUsed
	if limit == 0 && rl.RateLimit.CallsLimit > 0 {
		limit = rl.RateLimit.CallsLimit
		remaining = rl.RateLimit.CallsRemaining
		used = limit - remaining
	}
	if limit > 0 {
		if err := s.cfg.Budget.Sync(ctx, "openalex", used, limit); err != nil {
			return err
		}
		s.cfg.Metrics.SetBudgetRemaining("openalex", remaining)
		if remaining <= 0 {
			return ratelimit.ErrBudgetExhausted
		}
	}
	return nil
}

func (s *OpenAlexSource) buildWorksURL(job Job, cursor string) string {
	u, _ := url.Parse(s.cfg.BaseURL + "/works")
	q := u.Query()
	q.Set("per_page", strconv.Itoa(s.cfg.PerPage))
	q.Set("select", s.cfg.SelectFields)
	q.Set("cursor", cursor)

	var filters []string
	if strings.TrimSpace(job.Since) != "" {
		filters = append(filters, "from_updated_date:"+strings.TrimSpace(job.Since))
	}
	if len(job.Topics) > 0 {
		filters = append(filters, "topics.id:"+strings.Join(job.Topics, "|"))
	}
	if len(filters) > 0 {
		q.Set("filter", strings.Join(filters, ","))
	}
	if len(job.Keywords) > 0 {
		q.Set("search", strings.Join(job.Keywords, " OR "))
	}
	if s.cfg.APIKey != "" {
		q.Set("api_key", s.cfg.APIKey)
	}
	if s.cfg.Email != "" {
		q.Set("mailto", s.cfg.Email)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func (s *OpenAlexSource) doGET(ctx context.Context, reqURL string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("User-Agent", "SciPulse-Harvester/1.0")
	resp, err := s.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return body, resp.StatusCode, nil
}

func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	q := u.Query()
	if q.Has("api_key") {
		q.Set("api_key", "REDACTED")
		u.RawQuery = q.Encode()
	}
	return u.String()
}
