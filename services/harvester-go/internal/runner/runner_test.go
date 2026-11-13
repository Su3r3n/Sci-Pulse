package runner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/scipulse/scipulse/services/harvester-go/internal/metrics"
	"github.com/scipulse/scipulse/services/harvester-go/internal/sink"
	"github.com/scipulse/scipulse/services/harvester-go/internal/source"
	"github.com/scipulse/scipulse/services/harvester-go/internal/state"
)

type stubSource struct {
	name    string
	records []source.RawRecord
	delay   time.Duration
	onFetch func(ctx context.Context, out chan<- source.RawRecord)
	ordered bool
	split   []source.Job // when set, SplitJobs returns these jobs

	scopeRecords map[string][]source.RawRecord // per-scope records; overrides records
	scopeErr     map[string]error              // terminal Err emitted after a scope's records
}

// recordsFor returns the records for job, honouring per-scope data when configured.
func (s *stubSource) recordsFor(job source.Job) []source.RawRecord {
	if s.scopeRecords != nil {
		return s.scopeRecords[job.Scope()]
	}
	return s.records
}

func (s *stubSource) Name() string { return s.name }

// OrderedCursor makes the stub an OrderedSource when ordered is set.
func (s *stubSource) OrderedCursor() bool { return s.ordered }

// SplitJobs implements source.JobSplitter; without split configured the job passes through unchanged.
func (s *stubSource) SplitJobs(job source.Job) []source.Job {
	if s.split == nil {
		return []source.Job{job}
	}
	return s.split
}

func (s *stubSource) Fetch(ctx context.Context, job source.Job) (<-chan source.RawRecord, error) {
	out := make(chan source.RawRecord, 16)
	go func() {
		defer close(out)
		if s.onFetch != nil {
			s.onFetch(ctx, out)
			return
		}
		for _, r := range s.recordsFor(job) {
			if s.delay > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(s.delay):
				}
			}
			select {
			case out <- r:
			case <-ctx.Done():
				return
			}
		}
		if err := s.scopeErr[job.Scope()]; err != nil {
			out <- source.RawRecord{Source: s.name, Err: err}
		}
	}()
	return out, nil
}

type mockSink struct {
	mu         sync.Mutex
	batches    [][]sink.BatchItem
	failErr    error
	flushCalls atomic.Int32
	closed     atomic.Bool
}

func (m *mockSink) ProduceBatch(_ context.Context, items []sink.BatchItem) error {
	if m.failErr != nil {
		return m.failErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]sink.BatchItem, len(items))
	copy(cp, items)
	m.batches = append(m.batches, cp)
	return nil
}

func (m *mockSink) Flush(context.Context) error {
	m.flushCalls.Add(1)
	return nil
}

func (m *mockSink) Close() error {
	m.closed.Store(true)
	return nil
}

func (m *mockSink) totalItems() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, b := range m.batches {
		total += len(b)
	}
	return total
}

func TestCursorNotAdvancedWhenProduceFails(t *testing.T) {
	st := state.NewMemoryStore()
	if err := st.SaveCursor(context.Background(), state.Cursor{
		Source:      "openalex",
		Scope:       "default",
		CursorValue: "2026-11-01",
	}); err != nil {
		t.Fatal(err)
	}

	src := &stubSource{
		name: "openalex",
		records: []source.RawRecord{
			{
				Source: "openalex",
				ID:     "W1",
				Body:   []byte(`{"id":"W1","title":"Paper 1","publication_date":"2026-11-10","authorships":[{"author":{"display_name":"Ada"}}]}`),
				Cursor: "2026-11-10",
			},
		},
	}

	failingSink := &mockSink{failErr: errors.New("kafka broker unavailable")}
	m := metrics.New(prometheus.NewRegistry())

	r := New([]source.Source{src}, nil, st, failingSink, m, nil, Config{BatchSize: 10})
	if err := r.RunOnce(context.Background()); err == nil {
		t.Fatal("expected error when sink ProduceBatch fails")
	}

	cur, ok, err := st.GetCursor(context.Background(), "openalex", "default")
	if err != nil || !ok {
		t.Fatalf("GetCursor failed: ok=%v err=%v", ok, err)
	}
	if cur.CursorValue != "2026-11-01" {
		t.Fatalf("cursor MUST NOT advance on produce failure; got %q want 2026-11-01", cur.CursorValue)
	}
	if cur.LastError == nil || *cur.LastError == "" {
		t.Fatal("expected LastError to be recorded")
	}
}

func TestGracefulDrainOnSIGTERMStopsNewFetchesAndCommitsInFlightBatch(t *testing.T) {
	st := state.NewMemoryStore()
	sk := &mockSink{}
	m := metrics.New(prometheus.NewRegistry())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	firstSent := make(chan struct{})
	src := &stubSource{
		name:    "openalex",
		ordered: true,
		onFetch: func(fctx context.Context, out chan<- source.RawRecord) {
			// Emit first record immediately, then cancel ctx to simulate SIGTERM mid-stream
			out <- source.RawRecord{
				Source: "openalex",
				ID:     "W1",
				Body:   []byte(`{"id":"W1","title":"In-flight Paper","publication_date":"2026-11-10","authorships":[{"author":{"display_name":"Ada"}}]}`),
				Cursor: "2026-11-10",
			}
			close(firstSent)
			<-fctx.Done()
			// Must NOT emit second record after cancellation
		},
	}

	r := New([]source.Source{src}, nil, st, sk, m, nil, Config{
		BatchSize:    100,
		DrainTimeout: 5 * time.Second,
		PollInterval: 50 * time.Millisecond,
	})

	done := make(chan error, 1)
	go func() {
		done <- r.RunDaemon(ctx)
	}()

	<-firstSent
	// Allow goroutine a brief moment to read the record from the channel, then signal shutdown
	time.Sleep(30 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("RunDaemon returned error: %v", err)
	}

	if sk.totalItems() != 1 {
		t.Fatalf("expected 1 in-flight record to be produced during drain, got %d", sk.totalItems())
	}
	if sk.flushCalls.Load() == 0 {
		t.Fatal("expected sink.Flush to be called on drain")
	}
	cur, ok, err := st.GetCursor(context.Background(), "openalex", "default")
	if err != nil || !ok || cur.CursorValue != "2026-11-10" {
		t.Fatalf("expected committed cursor 2026-11-10 after drain, got ok=%v cur=%+v err=%v", ok, cur, err)
	}
}

// spySink records the committed cursor of the scope at the moment each batch is produced.
type spySink struct {
	mockSink
	st       state.Store
	source   string
	scope    string
	seenPrev []string
}

func (s *spySink) ProduceBatch(ctx context.Context, items []sink.BatchItem) error {
	cur, ok, _ := s.st.GetCursor(ctx, s.source, s.scope)
	v := ""
	if ok {
		v = cur.CursorValue
	}
	s.seenPrev = append(s.seenPrev, v)
	return s.mockSink.ProduceBatch(ctx, items)
}

func cursorRecords(prefix string, n int) []source.RawRecord {
	out := make([]source.RawRecord, 0, n)
	for i := 1; i <= n; i++ {
		out = append(out, source.RawRecord{
			Source: "openalex",
			ID:     fmt.Sprintf("%s%d", prefix, i),
			Body:   []byte(`{"id":"W1","title":"Paper","publication_date":"2026-11-10","authorships":[{"author":{"display_name":"Ada"}}]}`),
			Cursor: fmt.Sprintf("2026-11-%02d", i),
		})
	}
	return out
}

func mustCursor(t *testing.T, st state.Store, src, scope string) string {
	t.Helper()
	cur, ok, err := st.GetCursor(context.Background(), src, scope)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if !ok {
		return ""
	}
	return cur.CursorValue
}

func TestOrderedSourceCommitsCursorAfterEveryAckedBatch(t *testing.T) {
	st := state.NewMemoryStore()
	sk := &spySink{st: st, source: "openalex", scope: "default"}
	src := &stubSource{name: "openalex", ordered: true, records: cursorRecords("W", 5)}

	r := New([]source.Source{src}, nil, st, sk, metrics.New(prometheus.NewRegistry()), nil, Config{BatchSize: 2})
	if err := r.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	want := []string{"", "2026-11-02", "2026-11-04"}
	if fmt.Sprint(sk.seenPrev) != fmt.Sprint(want) {
		t.Fatalf("cursor before each batch = %q, want %q", sk.seenPrev, want)
	}
	if got := mustCursor(t, st, "openalex", "default"); got != "2026-11-05" {
		t.Fatalf("final cursor = %q, want 2026-11-05", got)
	}
}

func TestUnorderedSourceCommitsCursorOnlyAfterWholeJob(t *testing.T) {
	st := state.NewMemoryStore()
	if err := st.SaveCursor(context.Background(), state.Cursor{Source: "arxiv", Scope: "cs", CursorValue: "2026-11-01"}); err != nil {
		t.Fatal(err)
	}
	sk := &spySink{st: st, source: "arxiv", scope: "cs"}
	// Records arrive out of order: the max must be taken over the whole set, never per batch.
	recs := cursorRecords("A", 5)
	recs[0], recs[4] = recs[4], recs[0]
	src := &stubSource{name: "arxiv", records: recs}

	r := New([]source.Source{src}, nil, st, sk, metrics.New(prometheus.NewRegistry()), nil, Config{BatchSize: 2})
	job := source.Job{Topics: []string{"cs"}}
	if err := r.runJob(context.Background(), src, job); err != nil {
		t.Fatalf("runJob: %v", err)
	}
	for i, v := range sk.seenPrev {
		if v != "2026-11-01" {
			t.Fatalf("batch %d saw cursor %q before job end; unordered cursor must stay at 2026-11-01", i, v)
		}
	}
	if got := mustCursor(t, st, "arxiv", "cs"); got != "2026-11-05" {
		t.Fatalf("final cursor = %q, want 2026-11-05", got)
	}
}

func TestUnorderedCursorUnchangedOnTerminalFetchError(t *testing.T) {
	st := state.NewMemoryStore()
	src := &stubSource{
		name:         "arxiv",
		scopeRecords: map[string][]source.RawRecord{"cs": cursorRecords("A", 3)},
		scopeErr:     map[string]error{"cs": errors.New("arxiv HTTP 500")},
	}
	sk := &mockSink{}
	r := New([]source.Source{src}, nil, st, sk, metrics.New(prometheus.NewRegistry()), nil, Config{BatchSize: 10})
	r.jobs = map[string][]source.Job{"arxiv": {{Topics: []string{"cs"}}}}

	err := r.RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "arxiv HTTP 500") {
		t.Fatalf("RunOnce error = %v, want the terminal fetch error", err)
	}
	if got := mustCursor(t, st, "arxiv", "cs"); got != "" {
		t.Fatalf("cursor advanced to %q after a failed set; must stay unset", got)
	}
	if sk.totalItems() != 3 {
		t.Fatalf("records fetched before the error must still be produced, got %d", sk.totalItems())
	}
}

func TestOrderedCursorKeepsAckedPrefixOnTerminalFetchError(t *testing.T) {
	st := state.NewMemoryStore()
	src := &stubSource{
		name:         "openalex",
		ordered:      true,
		scopeRecords: map[string][]source.RawRecord{"default": cursorRecords("W", 3)},
		scopeErr:     map[string]error{"default": errors.New("openalex HTTP 503")},
	}
	r := New([]source.Source{src}, nil, st, &mockSink{}, metrics.New(prometheus.NewRegistry()), nil, Config{BatchSize: 2})

	if err := r.RunOnce(context.Background()); err == nil {
		t.Fatal("expected terminal fetch error to fail the pass")
	}
	// All three fetched records were produced, so the cursor covers them and nothing beyond.
	if got := mustCursor(t, st, "openalex", "default"); got != "2026-11-03" {
		t.Fatalf("cursor = %q, want 2026-11-03 (last produced record)", got)
	}
}

func TestScopesAreIndependentAndFailureDoesNotStopOthers(t *testing.T) {
	st := state.NewMemoryStore()
	src := &stubSource{
		name: "arxiv",
		split: []source.Job{
			{Topics: []string{"cs"}},
			{Topics: []string{"physics:hep-ex"}},
		},
		scopeRecords: map[string][]source.RawRecord{
			"cs":             cursorRecords("CS", 2),
			"physics:hep-ex": cursorRecords("HEP", 2),
		},
		scopeErr: map[string]error{"physics:hep-ex": errors.New("arxiv: 503 retries exhausted")},
	}
	// Order matters: the failing set comes second and must not hide the first one's cursor.
	r := New([]source.Source{src}, nil, st, &mockSink{}, metrics.New(prometheus.NewRegistry()), nil, Config{BatchSize: 10})

	err := r.RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "503 retries exhausted") {
		t.Fatalf("RunOnce error = %v, want the failing scope's error", err)
	}
	if got := mustCursor(t, st, "arxiv", "cs"); got != "2026-11-02" {
		t.Fatalf("cs cursor = %q, want 2026-11-02", got)
	}
	if got := mustCursor(t, st, "arxiv", "physics:hep-ex"); got != "" {
		t.Fatalf("physics:hep-ex cursor = %q, want unset after its failure", got)
	}
}

func TestCancelledUnorderedJobDoesNotFinaliseCursor(t *testing.T) {
	st := state.NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &stubSource{
		name: "arxiv",
		onFetch: func(fctx context.Context, out chan<- source.RawRecord) {
			out <- cursorRecords("A", 1)[0]
			cancel() // SIGTERM arrives mid-set: the stream stops early
		},
	}
	r := New([]source.Source{src}, nil, st, &mockSink{}, metrics.New(prometheus.NewRegistry()), nil, Config{BatchSize: 10})
	r.jobs = map[string][]source.Job{"arxiv": {{Topics: []string{"cs"}}}}

	if err := r.RunOnce(ctx); err == nil {
		t.Fatal("expected context error from an interrupted pass")
	}
	if got := mustCursor(t, st, "arxiv", "cs"); got != "" {
		t.Fatalf("interrupted set must not finalise its cursor, got %q", got)
	}
}

// R6: offline replays of fixtures never persist a cursor, even when the state store is real.
func TestOfflineRunDoesNotPersistCursors(t *testing.T) {
	st := state.NewMemoryStore()
	src := &stubSource{
		name:    "arxiv",
		records: []source.RawRecord{{Source: "arxiv", ID: "oai:1", Body: []byte(`{}`), Cursor: "2024-06-01"}},
	}
	m := metrics.New(prometheus.NewRegistry())
	r := New([]source.Source{src}, nil, st, &mockSink{}, m, nil, Config{BatchSize: 10, Offline: true})
	if err := r.RunOnce(context.Background()); err != nil {
		t.Fatalf("offline pass failed: %v", err)
	}
	if _, ok, err := st.GetCursor(context.Background(), "arxiv", "default"); err != nil || ok {
		t.Fatalf("offline run must not save a cursor: ok=%v err=%v", ok, err)
	}
}
