package source

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/scipulse/scipulse/services/harvester-go/internal/cache"
	"github.com/scipulse/scipulse/services/harvester-go/internal/metrics"
	"github.com/scipulse/scipulse/services/harvester-go/internal/ratelimit"
)

func TestArxivResumptionTokenAnd503RetryAfter(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		q := r.URL.Query()
		if n == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if n == 2 {
			if q.Get("verb") != "ListRecords" || q.Get("set") != "physics:hep-ex" || q.Get("from") != "2026-11-01" {
				t.Errorf("unexpected initial query: %v", q)
			}
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH>
  <ListRecords>
    <record>
      <header><identifier>oai:arXiv.org:2611.00001</identifier><datestamp>2026-11-10</datestamp></header>
      <metadata><arXiv><id>2611.00001</id><title>First Paper</title></arXiv></metadata>
    </record>
    <resumptionToken>tok-2</resumptionToken>
  </ListRecords>
</OAI-PMH>`))
			return
		}
		if q.Get("resumptionToken") != "tok-2" || q.Get("set") != "" {
			t.Errorf("expected exclusive resumptionToken=tok-2, got %v", q)
		}
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH>
  <ListRecords>
    <record>
      <header><identifier>oai:arXiv.org:2611.00002</identifier><datestamp>2026-11-11</datestamp></header>
      <metadata><arXiv><id>2611.00002</id><title>Second Paper</title></arXiv></metadata>
    </record>
  </ListRecords>
</OAI-PMH>`))
	}))
	defer srv.Close()

	diskCache := cache.NewDiskCache(t.TempDir())
	m := metrics.New(prometheus.NewRegistry())

	src := NewArxivSource(ArxivConfig{
		BaseURL:     srv.URL,
		Sets:        []string{"physics:hep-ex"},
		PoliteDelay: 5 * time.Millisecond,
		Limiter:     ratelimit.NewIntervalLimiter(5*time.Millisecond, 1),
		Cache:       diskCache,
		Metrics:     m,
	})

	ch, err := src.Fetch(context.Background(), Job{Since: "2026-11-01T00:00:00Z"})
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
	if recs[0].ID != "oai:arXiv.org:2611.00001" || recs[1].Cursor != "2026-11-11" {
		t.Fatalf("unexpected records: %+v", recs)
	}
}

func TestArxivOfflineCorruptXMLProducesParseError(t *testing.T) {
	diskCache := cache.NewDiskCache(t.TempDir())
	if err := diskCache.Put("arxiv", "bad-page", []byte("<OAI-PMH><ListRecords><record>broken")); err != nil {
		t.Fatal(err)
	}

	src := NewArxivSource(ArxivConfig{
		Offline: true,
		Cache:   diskCache,
	})
	ch, err := src.Fetch(context.Background(), Job{})
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	var recs []RawRecord
	for r := range ch {
		recs = append(recs, r)
	}
	if len(recs) != 1 || recs[0].ParseError == nil {
		t.Fatalf("expected 1 record with ParseError, got %+v", recs)
	}
}

func TestArxiv503ExhaustionIsTerminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	src := NewArxivSource(ArxivConfig{
		BaseURL:       srv.URL,
		Sets:          []string{"cs"},
		Max503Retries: 1,
		PoliteDelay:   time.Millisecond,
		Metrics:       metrics.New(prometheus.NewRegistry()),
	})
	ch, err := src.Fetch(context.Background(), Job{})
	if err != nil {
		t.Fatal(err)
	}
	recs := collectRecords(t, ch)
	if len(recs) != 1 || recs[0].Err == nil || !strings.Contains(recs[0].Err.Error(), "retries exhausted") {
		t.Fatalf("exhausted 503 retries must end the set with an error, got %+v", recs)
	}
}

func TestArxivPageOfDeletedRecordsStillFollowsToken(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			_, _ = w.Write([]byte(`<OAI-PMH><ListRecords>
  <record><header status="deleted"><identifier>oai:arXiv.org:1</identifier><datestamp>2026-11-10</datestamp></header></record>
  <resumptionToken>tok-2</resumptionToken></ListRecords></OAI-PMH>`))
			return
		}
		_, _ = w.Write([]byte(`<OAI-PMH><ListRecords>
  <record><header><identifier>oai:arXiv.org:2</identifier><datestamp>2026-11-11</datestamp></header><metadata><arXiv><id>2</id></arXiv></metadata></record>
  </ListRecords></OAI-PMH>`))
	}))
	defer srv.Close()

	src := NewArxivSource(ArxivConfig{
		BaseURL:     srv.URL,
		Sets:        []string{"cs"},
		PoliteDelay: time.Millisecond,
		Metrics:     metrics.New(prometheus.NewRegistry()),
	})
	ch, _ := src.Fetch(context.Background(), Job{})
	recs := collectRecords(t, ch)
	if calls.Load() != 2 || len(recs) != 1 || recs[0].Err != nil {
		t.Fatalf("pagination must continue past a page of deleted records: calls=%d recs=%+v", calls.Load(), recs)
	}
}

func TestArxivOAIErrorIsTerminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<OAI-PMH><error code="badResumptionToken">token expired</error></OAI-PMH>`))
	}))
	defer srv.Close()

	src := NewArxivSource(ArxivConfig{BaseURL: srv.URL, Sets: []string{"cs"}, PoliteDelay: time.Millisecond, Metrics: metrics.New(prometheus.NewRegistry())})
	ch, _ := src.Fetch(context.Background(), Job{})
	recs := collectRecords(t, ch)
	if len(recs) != 1 || recs[0].Err == nil || !strings.Contains(recs[0].Err.Error(), "badResumptionToken") {
		t.Fatalf("OAI-PMH error other than noRecordsMatch must be terminal, got %+v", recs)
	}
}

func TestArxivNoRecordsMatchIsEmptySet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<OAI-PMH><error code="noRecordsMatch">none</error></OAI-PMH>`))
	}))
	defer srv.Close()

	src := NewArxivSource(ArxivConfig{BaseURL: srv.URL, Sets: []string{"cs"}, PoliteDelay: time.Millisecond, Metrics: metrics.New(prometheus.NewRegistry())})
	ch, _ := src.Fetch(context.Background(), Job{})
	if recs := collectRecords(t, ch); len(recs) != 0 {
		t.Fatalf("noRecordsMatch is a normal empty result, got %+v", recs)
	}
}

func TestArxivSplitJobsGivesEachSetItsOwnScope(t *testing.T) {
	src := NewArxivSource(ArxivConfig{Sets: []string{"cs", "physics:hep-ex", " "}})
	jobs := src.SplitJobs(Job{Since: "2026-11-01"})
	if len(jobs) != 2 {
		t.Fatalf("expected one job per non-empty set, got %d", len(jobs))
	}
	if jobs[0].Scope() != "cs" || jobs[1].Scope() != "physics:hep-ex" {
		t.Fatalf("unexpected scopes: %q, %q", jobs[0].Scope(), jobs[1].Scope())
	}
	if jobs[1].Since != "2026-11-01" {
		t.Fatalf("Since must be carried into every split job, got %q", jobs[1].Since)
	}
	if src.OrderedCursor() {
		t.Fatal("arxiv must not report an ordered cursor")
	}
}

// R3: offline, the cache holds every set once. SplitJobs must not replay it per configured set.
func TestArxivOfflineIsNotMultipliedBySets(t *testing.T) {
	diskCache := cache.NewDiskCache(t.TempDir())
	for i := 0; i < 7; i++ {
		if err := diskCache.Put("arxiv", "page-"+string(rune('a'+i)), []byte("<OAI-PMH><ListRecords><record>broken")); err != nil {
			t.Fatal(err)
		}
	}
	src := NewArxivSource(ArxivConfig{
		Offline: true,
		Cache:   diskCache,
		Sets:    []string{"cs", "physics:hep-ex", "physics:hep-ph"},
	})
	jobs := src.SplitJobs(Job{})
	if len(jobs) != 1 {
		t.Fatalf("offline SplitJobs must keep one job, got %d", len(jobs))
	}
	ch, err := src.Fetch(context.Background(), jobs[0])
	if err != nil {
		t.Fatal(err)
	}
	if recs := collectRecords(t, ch); len(recs) != 7 {
		t.Fatalf("7 cached pages must yield 7 records, got %d", len(recs))
	}
}
