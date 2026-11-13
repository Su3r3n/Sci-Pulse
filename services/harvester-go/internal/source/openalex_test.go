package source

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/scipulse/scipulse/services/harvester-go/internal/cache"
	"github.com/scipulse/scipulse/services/harvester-go/internal/metrics"
	"github.com/scipulse/scipulse/services/harvester-go/internal/ratelimit"
)

func TestOpenAlexPaginationAndCache(t *testing.T) {
	var pageCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rate-limit":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"rate_limit":{"daily_limit":1000,"daily_used":5,"daily_remaining":995}}`))
		case "/works":
			q := r.URL.Query()
			if q.Get("per_page") != "100" {
				t.Errorf("expected per_page=100, got %q", q.Get("per_page"))
			}
			if q.Get("api_key") != "secret-key" {
				t.Errorf("expected api_key=secret-key, got %q", q.Get("api_key"))
			}
			if q.Get("select") == "" {
				t.Errorf("expected select param to be set")
			}
			call := pageCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			if call == 1 {
				if q.Get("cursor") != "*" {
					t.Errorf("expected cursor=*, got %q", q.Get("cursor"))
				}
				_, _ = w.Write([]byte(`{
					"meta":{"count":2,"next_cursor":"c2"},
					"results":[{"id":"https://openalex.org/W1","updated_date":"2026-11-10T10:00:00Z"}]
				}`))
				return
			}
			if q.Get("cursor") != "c2" {
				t.Errorf("expected cursor=c2, got %q", q.Get("cursor"))
			}
			_, _ = w.Write([]byte(`{
				"meta":{"count":2,"next_cursor":null},
				"results":[{"id":"https://openalex.org/W2","updated_date":"2026-11-11T12:00:00Z"}]
			}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	cacheDir := t.TempDir()
	diskCache := cache.NewDiskCache(cacheDir)
	tracker := ratelimit.NewBudgetTracker(nil)
	m := metrics.New(prometheus.NewRegistry())

	src := NewOpenAlexSource(OpenAlexConfig{
		BaseURL: srv.URL,
		APIKey:  "secret-key",
		Cache:   diskCache,
		Budget:  tracker,
		Metrics: m,
	})

	ch, err := src.Fetch(context.Background(), Job{Since: "2026-11-01"})
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}

	var recs []RawRecord
	for r := range ch {
		recs = append(recs, r)
	}
	if len(recs) != 2 {
		t.Fatalf("expected 2 records, got %d", len(recs))
	}
	if recs[0].ID != "https://openalex.org/W1" || recs[1].ID != "https://openalex.org/W2" {
		t.Fatalf("unexpected record IDs: %+v", recs)
	}
	if recs[1].Cursor != "2026-11-11T12:00:00Z" {
		t.Fatalf("unexpected cursor on record 2: %q", recs[1].Cursor)
	}

	// Verify offline mode reads the 2 cached pages without hitting HTTP server
	srv.Close()
	offlineSrc := NewOpenAlexSource(OpenAlexConfig{
		Offline: true,
		Cache:   diskCache,
		Metrics: m,
	})
	chOff, err := offlineSrc.Fetch(context.Background(), Job{})
	if err != nil {
		t.Fatalf("offline Fetch failed: %v", err)
	}
	var offRecs []RawRecord
	for r := range chOff {
		offRecs = append(offRecs, r)
	}
	if len(offRecs) != 2 {
		t.Fatalf("expected 2 offline records, got %d", len(offRecs))
	}
}

func TestOpenAlexBudgetExhaustedStopsCleanly(t *testing.T) {
	var worksCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rate-limit":
			_, _ = w.Write([]byte(`{"rate_limit":{"daily_limit":100,"daily_used":100,"daily_remaining":0}}`))
		case "/works":
			worksCalls.Add(1)
		}
	}))
	defer srv.Close()

	src := NewOpenAlexSource(OpenAlexConfig{
		BaseURL: srv.URL,
		Budget:  ratelimit.NewBudgetTracker(nil),
	})
	ch, err := src.Fetch(context.Background(), Job{})
	if err != nil {
		t.Fatalf("expected graceful stop on exhausted budget, got err: %v", err)
	}
	for range ch {
	}
	if worksCalls.Load() != 0 {
		t.Fatalf("expected 0 /works calls when budget is 0, got %d", worksCalls.Load())
	}
}

func collectRecords(t *testing.T, ch <-chan RawRecord) []RawRecord {
	t.Helper()
	var out []RawRecord
	for r := range ch {
		out = append(out, r)
	}
	return out
}

func TestOpenAlexRequestsSortedByUpdatedDate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("sort"); got != "updated_date:asc" {
			t.Errorf("sort = %q, want updated_date:asc", got)
		}
		_, _ = w.Write([]byte(`{"meta":{"next_cursor":null},"results":[]}`))
	}))
	defer srv.Close()

	src := NewOpenAlexSource(OpenAlexConfig{BaseURL: srv.URL, Metrics: metrics.New(prometheus.NewRegistry())})
	ch, err := src.Fetch(context.Background(), Job{Since: "2026-11-01"})
	if err != nil {
		t.Fatal(err)
	}
	collectRecords(t, ch)
	if !src.OrderedCursor() {
		t.Fatal("openalex must report an ordered cursor")
	}
}

func TestOpenAlexHTTP5xxIsTerminalAndMasksAPIKey(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			_, _ = w.Write([]byte(`{"meta":{"next_cursor":"c2"},"results":[{"id":"https://openalex.org/W1","updated_date":"2026-11-10"}]}`))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	src := NewOpenAlexSource(OpenAlexConfig{
		BaseURL: srv.URL,
		APIKey:  "super-secret",
		Metrics: metrics.New(prometheus.NewRegistry()),
	})
	ch, err := src.Fetch(context.Background(), Job{})
	if err != nil {
		t.Fatal(err)
	}
	recs := collectRecords(t, ch)
	if len(recs) != 2 {
		t.Fatalf("expected 1 record then 1 terminal error, got %d records", len(recs))
	}
	if recs[0].Err != nil || recs[0].Cursor != "2026-11-10" {
		t.Fatalf("first record should be a normal work with its date cursor: %+v", recs[0])
	}
	if recs[1].Err == nil || !strings.Contains(recs[1].Err.Error(), "HTTP 500") {
		t.Fatalf("second record should carry the HTTP 500 error, got %+v", recs[1])
	}
	if strings.Contains(recs[1].Err.Error(), "super-secret") {
		t.Fatalf("error leaks the API key: %v", recs[1].Err)
	}
}

func TestOpenAlexRateLimitedIsTerminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	src := NewOpenAlexSource(OpenAlexConfig{BaseURL: srv.URL, Metrics: metrics.New(prometheus.NewRegistry())})
	ch, _ := src.Fetch(context.Background(), Job{})
	recs := collectRecords(t, ch)
	if len(recs) != 1 || recs[0].Err == nil {
		t.Fatalf("429 must surface as a terminal error, got %+v", recs)
	}
}

func TestOpenAlexCursorIsDateNotPagingToken(t *testing.T) {
	// A work without updated_date must not inherit the paging token as its cursor.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"meta":{"next_cursor":"IlsxNzYwMDAwMDAw"},"results":[{"id":"https://openalex.org/W9"}]}`))
	}))
	defer srv.Close()

	src := NewOpenAlexSource(OpenAlexConfig{BaseURL: srv.URL, Metrics: metrics.New(prometheus.NewRegistry())})
	ch, _ := src.Fetch(context.Background(), Job{})
	recs := collectRecords(t, ch)
	if len(recs) == 0 {
		t.Fatal("expected at least one record")
	}
	if recs[0].Cursor != "" {
		t.Fatalf("cursor = %q, want empty: paging token is not a date", recs[0].Cursor)
	}
}

func TestOpenAlexBrokenItemDoesNotStopPagination(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			// Every item on the first page is malformed; the paging token must still be followed.
			_, _ = w.Write([]byte(`{"meta":{"next_cursor":"c2"},"results":["bad-1","bad-2"]}`))
			return
		}
		_, _ = w.Write([]byte(`{"meta":{"next_cursor":null},"results":[{"id":"https://openalex.org/W3","updated_date":"2026-11-12"}]}`))
	}))
	defer srv.Close()

	src := NewOpenAlexSource(OpenAlexConfig{BaseURL: srv.URL, Metrics: metrics.New(prometheus.NewRegistry())})
	ch, _ := src.Fetch(context.Background(), Job{})
	recs := collectRecords(t, ch)
	if calls.Load() != 2 {
		t.Fatalf("pagination stopped after a page of broken items: %d requests", calls.Load())
	}
	if len(recs) != 3 || recs[0].ParseError == nil || recs[2].Cursor != "2026-11-12" {
		t.Fatalf("unexpected records: %+v", recs)
	}
}

func TestOpenAlexRepeatedCursorIsTerminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"meta":{"next_cursor":"same"},"results":[]}`))
	}))
	defer srv.Close()

	src := NewOpenAlexSource(OpenAlexConfig{BaseURL: srv.URL, Metrics: metrics.New(prometheus.NewRegistry())})
	ch, _ := src.Fetch(context.Background(), Job{})
	recs := collectRecords(t, ch)
	if len(recs) != 1 || recs[0].Err == nil {
		t.Fatalf("a repeated paging cursor must end the pass with an error, got %+v", recs)
	}
}
