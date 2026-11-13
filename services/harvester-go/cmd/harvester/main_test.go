package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/scipulse/scipulse/services/common-go/obs"
	"github.com/scipulse/scipulse/services/harvester-go/internal/cache"
	"github.com/scipulse/scipulse/services/harvester-go/internal/metrics"
	"github.com/scipulse/scipulse/services/harvester-go/internal/ratelimit"
	"github.com/scipulse/scipulse/services/harvester-go/internal/state"
)

func TestMetricsRegisteredAndServed(t *testing.T) {
	reg := obs.NewRegistry()
	m := metrics.New(reg)
	now := time.Unix(1794571200, 0).UTC()

	m.IncFetched("openalex")
	m.IncValid("openalex")
	m.IncDLQ("arxiv", "parse_error")
	m.IncHTTPRequest("openalex", 200)
	m.SetBudgetRemaining("openalex", 999)
	m.IncCacheHit("arxiv")
	m.ObserveProduceLatency("raw.papers.openalex", 25*time.Millisecond)
	m.SetLastSuccess("openalex", "default", now)
	m.SetRegistryProjects("active", 3)

	mux := obs.NewMux(reg, map[string]obs.HealthCheck{
		"state": func(context.Context) error { return nil },
	})

	healthReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	healthRec := httptest.NewRecorder()
	mux.ServeHTTP(healthRec, healthReq)
	if healthRec.Code != http.StatusOK {
		t.Fatalf("expected /healthz 200, got %d", healthRec.Code)
	}

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	mux.ServeHTTP(metricsRec, metricsReq)
	if metricsRec.Code != http.StatusOK {
		t.Fatalf("expected /metrics 200, got %d", metricsRec.Code)
	}

	body := metricsRec.Body.String()
	expected := []string{
		"harvester_fetched_total",
		"harvester_valid_total",
		"harvester_dlq_total",
		"harvester_http_requests_total",
		"harvester_budget_remaining",
		"harvester_cache_hits_total",
		"harvester_produce_latency_seconds",
		"harvester_last_success_timestamp_seconds",
		"harvester_registry_projects",
	}
	for _, name := range expected {
		if !strings.Contains(body, name) {
			t.Errorf("missing expected metric %q in /metrics output", name)
		}
	}
}

func TestBuildSourcesAndSplitCSV(t *testing.T) {
	cfg := appConfig{
		OpenAlexBaseURL:     "https://api.openalex.org",
		OpenAlexDailyBudget: 1000,
		OpenAlexRateLimit:   5,
		ArxivBaseURL:        "https://export.arxiv.org/oai2",
		ArxivSets:           "cs, physics:hep-ex",
		ArxivPoliteDelay:    3 * time.Second,
	}
	srcs, err := buildSources(
		splitCSV("openalex, arxiv"),
		cfg,
		cache.NewDiskCache(t.TempDir()),
		ratelimit.NewBudgetTracker(state.NewMemoryStore()),
		nil,
		nil,
		true,
	)
	if err != nil {
		t.Fatalf("buildSources failed: %v", err)
	}
	if len(srcs) != 2 || srcs[0].Name() != "openalex" || srcs[1].Name() != "arxiv" {
		t.Fatalf("unexpected sources: %+v", srcs)
	}

	if _, err := buildSources([]string{"unknown"}, cfg, nil, nil, nil, nil, true); err == nil {
		t.Fatal("expected error for unknown source")
	}
}
