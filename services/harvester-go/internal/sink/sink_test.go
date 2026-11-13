package sink

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/scipulse/scipulse/services/common-go/avro"
	"github.com/scipulse/scipulse/services/common-go/kafka"
	"github.com/scipulse/scipulse/services/harvester-go/internal/metrics"
	"github.com/scipulse/scipulse/services/harvester-go/internal/pipeline"
)

func newMockSchemaRegistry(t *testing.T) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	nextID := 1
	schemas := make(map[int]string)
	bySubj := make(map[string]int)

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")

		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/subjects/") && strings.HasSuffix(r.URL.Path, "/versions") {
			var req struct {
				Schema string `json:"schema"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			key := r.URL.Path + "\x00" + req.Schema
			id, ok := bySubj[key]
			if !ok {
				id = nextID
				nextID++
				bySubj[key] = id
				schemas[id] = req.Schema
			}
			_ = json.NewEncoder(w).Encode(map[string]int{"id": id})
			return
		}
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/schemas/ids/") {
			var id int
			_, _ = strings.CutPrefix(r.URL.Path, "/schemas/ids/")
			for k := range schemas {
				if strings.HasSuffix(r.URL.Path, "/"+string(rune('0'+k))) {
					id = k
				}
			}
			// parse id cleanly
			rawID := strings.TrimPrefix(r.URL.Path, "/schemas/ids/")
			var parsedID int
			for _, ch := range rawID {
				parsedID = parsedID*10 + int(ch-'0')
			}
			id = parsedID
			s, ok := schemas[id]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"schema": s})
			return
		}
		http.NotFound(w, r)
	}))
}

func TestKafkaSinkProducesValidPaperAndSchemaErrorDLQ(t *testing.T) {
	sr := newMockSchemaRegistry(t)
	defer sr.Close()

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(1, "raw.papers.openalex", "dlq.papers"),
	)
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	defer cluster.Close()

	prod, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers:         cluster.ListenAddrs(),
		DeliveryTimeout: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer prod.Close()

	srClient := avro.NewClient(sr.URL)
	m := metrics.New(prometheus.NewRegistry())
	ks, err := NewKafkaSink(prod, srClient, m, nil)
	if err != nil {
		t.Fatalf("NewKafkaSink: %v", err)
	}

	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	doi := "10.1000/test"
	validPaper := &pipeline.Paper{
		Source:      "openalex",
		SourceID:    "W1",
		DOI:         &doi,
		Title:       "Valid Paper",
		Authors:     []pipeline.Author{{Name: "Ada", Affiliations: []string{}}},
		Topics:      []string{"HEP"},
		PublishedAt: now,
		UpdatedAt:   now,
		Links:       []pipeline.Link{},
		IngestedAt:  now,
	}
	// Invalid enum value triggers schema_error during Avro serialization
	invalidSchemaPaper := &pipeline.Paper{
		Source:      "unknown_source",
		SourceID:    "W2",
		Title:       "Bad Enum",
		Authors:     []pipeline.Author{{Name: "Ada", Affiliations: []string{}}},
		Topics:      []string{},
		PublishedAt: now,
		UpdatedAt:   now,
		Links:       []pipeline.Link{},
		IngestedAt:  now,
	}

	err = ks.ProduceBatch(context.Background(), []BatchItem{
		{
			Result: pipeline.RouteResult{
				Topic: "raw.papers.openalex",
				Key:   validPaper.RecordKey(),
				Paper: validPaper,
			},
			RawBody: []byte(`{"id":"W1"}`),
		},
		{
			Result: pipeline.RouteResult{
				Topic: "raw.papers.openalex",
				Key:   "unknown_source:W2",
				Paper: invalidSchemaPaper,
			},
			RawBody: []byte(`{"id":"W2"}`),
		},
	})
	if err != nil {
		t.Fatalf("ProduceBatch failed: %v", err)
	}

	// Verify both records landed on expected topics and decode back via Avro codec
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics("raw.papers.openalex", "dlq.papers"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var gotPaper bool
	var gotDLQ bool
	codec := avro.NewCodec(srClient)
	for !(gotPaper && gotDLQ) {
		fetches := consumer.PollFetches(ctx)
		if fetches.Err() != nil {
			t.Fatalf("PollFetches: %v", fetches.Err())
		}
		fetches.EachRecord(func(r *kgo.Record) {
			switch r.Topic {
			case "raw.papers.openalex":
				var decoded pipeline.Paper
				if _, err := codec.Decode(ctx, r.Value, &decoded); err != nil {
					t.Fatalf("decode paper: %v", err)
				}
				if string(r.Key) != "10.1000/test" || decoded.SourceID != "W1" {
					t.Fatalf("unexpected paper key=%q decoded=%+v", string(r.Key), decoded)
				}
				gotPaper = true
			case "dlq.papers":
				var decoded pipeline.DLQEnvelope
				if _, err := codec.Decode(ctx, r.Value, &decoded); err != nil {
					t.Fatalf("decode dlq: %v", err)
				}
				if decoded.Reason != pipeline.ReasonSchemaError || decoded.Stage != pipeline.StageSerialize {
					t.Fatalf("unexpected dlq envelope: %+v", decoded)
				}
				gotDLQ = true
			}
		})
	}
}
