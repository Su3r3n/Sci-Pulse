package source

import (
	"context"
	"net/http"
	"net/http/httptest"
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
