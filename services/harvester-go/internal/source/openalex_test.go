package source

import (
	"context"
	"net/http"
	"net/http/httptest"
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
