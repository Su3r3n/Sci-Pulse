package avro

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockSchemaRegistry struct {
	mu           sync.Mutex
	nextID       int
	byID         map[int]string
	bySubject    map[string][]int
	compatBySubj map[string]string
	registerHits atomic.Int32
	getByIDHits  atomic.Int32
}

func newMockRegistry() *mockSchemaRegistry {
	return &mockSchemaRegistry{
		nextID:       1,
		byID:         make(map[int]string),
		bySubject:    make(map[string][]int),
		compatBySubj: make(map[string]string),
	}
}

func (m *mockSchemaRegistry) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /subjects/{subject}/versions", func(w http.ResponseWriter, r *http.Request) {
		m.registerHits.Add(1)
		subject := r.PathValue("subject")
		var req struct {
			Schema string `json:"schema"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error_code":42201,"message":"invalid json"}`, http.StatusUnprocessableEntity)
			return
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		for _, id := range m.bySubject[subject] {
			if m.byID[id] == req.Schema {
				_ = json.NewEncoder(w).Encode(map[string]int{"id": id})
				return
			}
		}
		id := m.nextID
		m.nextID++
		m.byID[id] = req.Schema
		m.bySubject[subject] = append(m.bySubject[subject], id)
		_ = json.NewEncoder(w).Encode(map[string]int{"id": id})
	})

	mux.HandleFunc("GET /schemas/ids/{id}", func(w http.ResponseWriter, r *http.Request) {
		m.getByIDHits.Add(1)
		var id int
		for _, ch := range r.PathValue("id") {
			if ch < '0' || ch > '9' {
				http.Error(w, `{"error_code":40403,"message":"invalid id"}`, http.StatusNotFound)
				return
			}
			id = id*10 + int(ch-'0')
		}
		m.mu.Lock()
		schema, ok := m.byID[id]
		m.mu.Unlock()
		if !ok {
			http.Error(w, `{"error_code":40403,"message":"schema not found"}`, http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": schema})
	})

	mux.HandleFunc("GET /subjects/{subject}/versions/latest", func(w http.ResponseWriter, r *http.Request) {
		subject := r.PathValue("subject")
		m.mu.Lock()
		ids := m.bySubject[subject]
		if len(ids) == 0 {
			m.mu.Unlock()
			http.Error(w, `{"error_code":40401,"message":"subject not found"}`, http.StatusNotFound)
			return
		}
		id := ids[len(ids)-1]
		schema := m.byID[id]
		ver := len(ids)
		m.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"subject": subject,
			"version": ver,
			"id":      id,
			"schema":  schema,
		})
	})

	mux.HandleFunc("POST /compatibility/subjects/{subject}/versions/latest", func(w http.ResponseWriter, r *http.Request) {
		subject := r.PathValue("subject")
		var req struct {
			Schema string `json:"schema"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error_code":42201,"message":"invalid json"}`, http.StatusUnprocessableEntity)
			return
		}
		m.mu.Lock()
		ids := m.bySubject[subject]
		var latest string
		if len(ids) > 0 {
			latest = m.byID[ids[len(ids)-1]]
		}
		m.mu.Unlock()
		if latest == "" {
			http.Error(w, `{"error_code":40401,"message":"subject not found"}`, http.StatusNotFound)
			return
		}
		compatible := !strings.Contains(req.Schema, `"incompatible_marker"`)
		_ = json.NewEncoder(w).Encode(map[string]bool{"is_compatible": compatible})
	})

	mux.HandleFunc("PUT /config/{subject}", func(w http.ResponseWriter, r *http.Request) {
		subject := r.PathValue("subject")
		var req struct {
			Compatibility string `json:"compatibility"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		m.mu.Lock()
		m.compatBySubj[subject] = req.Compatibility
		m.mu.Unlock()
		_ = json.NewEncoder(w).Encode(req)
	})

	return mux
}

// Test DTOs matching contracts/paper.v1.avsc and contracts/dlq_envelope.v1.avsc.
type paperAuthor struct {
	Name         string   `avro:"name"`
	ORCID        *string  `avro:"orcid"`
	Affiliations []string `avro:"affiliations"`
}

type paperLink struct {
	URL  string `avro:"url"`
	Kind string `avro:"kind"`
}

type paperRecord struct {
	Source       string        `avro:"source"`
	SourceID     string        `avro:"source_id"`
	DOI          *string       `avro:"doi"`
	ArxivID      *string       `avro:"arxiv_id"`
	Title        string        `avro:"title"`
	Abstract     *string       `avro:"abstract"`
	Comments     *string       `avro:"comments"`
	JournalRef   *string       `avro:"journal_ref"`
	Authors      []paperAuthor `avro:"authors"`
	Topics       []string      `avro:"topics"`
	PublishedAt  time.Time     `avro:"published_at"`
	UpdatedAt    time.Time     `avro:"updated_at"`
	CitedByCount *int          `avro:"cited_by_count"`
	Links        []paperLink   `avro:"links"`
	IngestedAt   time.Time     `avro:"ingested_at"`
}

type dlqEnvelopeRecord struct {
	Reason        string    `avro:"reason"`
	Stage         string    `avro:"stage"`
	Source        string    `avro:"source"`
	OriginalTopic string    `avro:"original_topic"`
	OriginalKey   *string   `avro:"original_key"`
	Payload       []byte    `avro:"payload"`
	Error         string    `avro:"error"`
	FailedAt      time.Time `avro:"failed_at"`
	Attempt       int       `avro:"attempt"`
	RuleApplied   *string   `avro:"rule_applied"`
}

func readContract(t *testing.T, name string) string {
	t.Helper()
	p := filepath.Join("..", "..", "..", "contracts", name)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read contract %s: %v", p, err)
	}
	return string(b)
}

func strPtr(s string) *string { return &s }
func intPtr(i int) *int       { return &i }

func TestFrameUnframe(t *testing.T) {
	wire, err := Frame(42, []byte{0xde, 0xad, 0xbe, 0xef})
	if err != nil {
		t.Fatalf("Frame failed: %v", err)
	}
	if len(wire) != 9 || wire[0] != MagicByte {
		t.Fatalf("unexpected wire header: %x", wire)
	}
	if got := binary.BigEndian.Uint32(wire[1:5]); got != 42 {
		t.Fatalf("big-endian id = %d, want 42", got)
	}

	id, payload, err := Unframe(wire)
	if err != nil {
		t.Fatalf("Unframe failed: %v", err)
	}
	if id != 42 || !bytes.Equal(payload, []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Fatalf("Unframe = (%d, %x)", id, payload)
	}

	if _, _, err := Unframe([]byte{0x01, 0, 0, 0, 1}); err == nil {
		t.Fatal("expected error for wrong magic byte")
	}
	if _, _, err := Unframe([]byte{0x00, 0, 0, 1}); err == nil {
		t.Fatal("expected error for truncated header")
	}
}

func TestPaperAndDLQRoundTripWithMockRegistry(t *testing.T) {
	mr := newMockRegistry()
	srv := httptest.NewServer(mr.handler())
	defer srv.Close()

	ctx := context.Background()
	producerClient := NewClient(srv.URL)
	consumerClient := NewClient(srv.URL) // separate cache to exercise GET /schemas/ids/{id}
	enc := NewCodec(producerClient)
	dec := NewCodec(consumerClient)

	paperSchema := readContract(t, "paper.v1.avsc")
	dlqSchema := readContract(t, "dlq_envelope.v1.avsc")

	pub := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	upd := time.Date(2026, 10, 2, 15, 30, 0, 0, time.UTC)
	ing := time.Date(2026, 11, 13, 6, 0, 0, 0, time.UTC)

	wantPaper := paperRecord{
		Source:     "arxiv",
		SourceID:   "2610.01234",
		DOI:        strPtr("10.1000/scipulse.1"),
		ArxivID:    strPtr("2610.01234"),
		Title:      "Scaling High-Energy Physics Pipelines",
		Abstract:   strPtr("A paper about streaming lakehouses."),
		Comments:   strPtr("Code at https://github.com/scipulse/pipeline"),
		JournalRef: strPtr("JHEP 2026"),
		Authors: []paperAuthor{
			{
				Name:         "Ada Lovelace",
				ORCID:        strPtr("0000-0001-2345-6789"),
				Affiliations: []string{"CERN", "ETH Zurich"},
			},
			{
				Name:         "Grace Hopper",
				ORCID:        nil,
				Affiliations: []string{},
			},
		},
		Topics:       []string{"hep-ex", "cs.DB"},
		PublishedAt:  pub,
		UpdatedAt:    upd,
		CitedByCount: intPtr(17),
		Links: []paperLink{
			{URL: "https://github.com/scipulse/pipeline", Kind: "code"},
			{URL: "https://arxiv.org/pdf/2610.01234", Kind: "pdf"},
		},
		IngestedAt: ing,
	}

	wire1, err := enc.Encode(ctx, "raw.papers.arxiv-value", paperSchema, wantPaper)
	if err != nil {
		t.Fatalf("Encode paper 1 failed: %v", err)
	}
	wire2, err := enc.Encode(ctx, "raw.papers.arxiv-value", paperSchema, wantPaper)
	if err != nil {
		t.Fatalf("Encode paper 2 failed: %v", err)
	}
	if !bytes.Equal(wire1, wire2) {
		t.Fatal("expected deterministic wire output")
	}
	// Producer client should hit POST /subjects/.../versions only once due to caching.
	if hits := mr.registerHits.Load(); hits != 1 {
		t.Fatalf("registerHits = %d, want 1 (cached)", hits)
	}

	var gotPaper paperRecord
	gotID, err := dec.Decode(ctx, wire1, &gotPaper)
	if err != nil {
		t.Fatalf("Decode paper failed: %v", err)
	}
	if gotID != 1 {
		t.Fatalf("decoded schema id = %d, want 1", gotID)
	}
	if !reflect.DeepEqual(gotPaper, wantPaper) {
		t.Fatalf("paper mismatch:\ngot:  %+v\nwant: %+v", gotPaper, wantPaper)
	}

	// Second decode must use consumerClient's ID cache.
	var gotPaper2 paperRecord
	if _, err := dec.Decode(ctx, wire2, &gotPaper2); err != nil {
		t.Fatalf("Decode paper 2 failed: %v", err)
	}
	if hits := mr.getByIDHits.Load(); hits != 1 {
		t.Fatalf("getByIDHits = %d, want 1 (cached)", hits)
	}

	// DLQ Envelope round-trip
	wantDLQ := dlqEnvelopeRecord{
		Reason:        "bad_doi",
		Stage:         "validate",
		Source:        "openalex",
		OriginalTopic: "raw.papers.openalex",
		OriginalKey:   strPtr("openalex:W123"),
		Payload:       []byte(`{"id":"W123","doi":"not-a-doi"}`),
		Error:         "doi does not match pattern",
		FailedAt:      ing,
		Attempt:       1,
		RuleApplied:   nil,
	}

	dlqWire, err := enc.Encode(ctx, "dlq.papers-value", dlqSchema, wantDLQ)
	if err != nil {
		t.Fatalf("Encode dlq failed: %v", err)
	}
	var gotDLQ dlqEnvelopeRecord
	dlqID, err := dec.Decode(ctx, dlqWire, &gotDLQ)
	if err != nil {
		t.Fatalf("Decode dlq failed: %v", err)
	}
	if dlqID != 2 {
		t.Fatalf("dlq schema id = %d, want 2", dlqID)
	}
	if !reflect.DeepEqual(gotDLQ, wantDLQ) {
		t.Fatalf("dlq mismatch:\ngot:  %+v\nwant: %+v", gotDLQ, wantDLQ)
	}
}

func TestRegistryClientCompatibilityAndErrors(t *testing.T) {
	mr := newMockRegistry()
	srv := httptest.NewServer(mr.handler())
	defer srv.Close()

	ctx := context.Background()
	c := NewClient(srv.URL)
	paperSchema := readContract(t, "paper.v1.avsc")

	if err := c.SetCompatibility(ctx, "raw.papers.openalex-value", "BACKWARD"); err != nil {
		t.Fatalf("SetCompatibility failed: %v", err)
	}
	id, err := c.RegisterSchema(ctx, "raw.papers.openalex-value", paperSchema)
	if err != nil {
		t.Fatalf("RegisterSchema failed: %v", err)
	}
	latestID, _, _, err := c.GetLatestSchema(ctx, "raw.papers.openalex-value")
	if err != nil || latestID != id {
		t.Fatalf("GetLatestSchema = (%d, %v), want (%d, nil)", latestID, err, id)
	}

	ok, err := c.CheckCompatibility(ctx, "raw.papers.openalex-value", paperSchema)
	if err != nil || !ok {
		t.Fatalf("CheckCompatibility = (%v, %v), want (true, nil)", ok, err)
	}

	// Missing schema ID returns RegistryError
	_, _, err = c.GetSchemaByID(ctx, 9999)
	var regErr *RegistryError
	if !errors.As(err, &regErr) || regErr.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 RegistryError, got %v", err)
	}
}
