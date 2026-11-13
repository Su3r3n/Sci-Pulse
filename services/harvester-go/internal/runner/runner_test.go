package runner

import (
	"context"
	"errors"
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
}

func (s *stubSource) Name() string { return s.name }

func (s *stubSource) Fetch(ctx context.Context, _ source.Job) (<-chan source.RawRecord, error) {
	out := make(chan source.RawRecord, 16)
	go func() {
		defer close(out)
		if s.onFetch != nil {
			s.onFetch(ctx, out)
			return
		}
		for _, r := range s.records {
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
		name: "openalex",
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
