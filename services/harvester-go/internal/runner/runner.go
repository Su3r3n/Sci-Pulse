package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/scipulse/scipulse/services/harvester-go/internal/metrics"
	"github.com/scipulse/scipulse/services/harvester-go/internal/pipeline"
	"github.com/scipulse/scipulse/services/harvester-go/internal/sink"
	"github.com/scipulse/scipulse/services/harvester-go/internal/source"
	"github.com/scipulse/scipulse/services/harvester-go/internal/state"
)

const (
	defaultBatchSize    = 100
	defaultDrainTimeout = 15 * time.Second
	defaultPollInterval = 1 * time.Minute
)

// Config controls Runner execution and graceful drain behavior.
type Config struct {
	BatchSize    int
	DrainTimeout time.Duration
	PollInterval time.Duration
	Now          func() time.Time
}

// Runner orchestrates fetching from Sources, running the pipeline, producing to Sink,
// and advancing cursors in State only after Kafka confirms each batch.
type Runner struct {
	sources []source.Source
	jobs    map[string][]source.Job
	state   state.Store
	sink    sink.Sink
	metrics *metrics.Metrics
	logger  *slog.Logger
	cfg     Config
}

// New creates a Runner for the provided sources and default jobs.
func New(
	sources []source.Source,
	jobs map[string][]source.Job,
	st state.Store,
	sk sink.Sink,
	m *metrics.Metrics,
	logger *slog.Logger,
	cfg Config,
) *Runner {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = defaultDrainTimeout
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if logger == nil {
		logger = slog.Default()
	}
	if jobs == nil {
		jobs = make(map[string][]source.Job)
	}
	return &Runner{
		sources: sources,
		jobs:    jobs,
		state:   st,
		sink:    sk,
		metrics: m,
		logger:  logger,
		cfg:     cfg,
	}
}

// RunOnce executes a single pass across all sources (one worker per source).
func (r *Runner) RunOnce(ctx context.Context) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	for _, src := range r.sources {
		src := src
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.runSourcePass(ctx, src); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("source %s: %w", src.Name(), err))
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return errors.Join(errs...)
}

// RunDaemon runs periodic passes with one worker per source until ctx is cancelled,
// draining in-flight batches and flushing the sink cleanly on shutdown.
func (r *Runner) RunDaemon(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, src := range r.sources {
		src := src
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.sourceWorkerLoop(ctx, src)
		}()
	}

	<-ctx.Done()
	r.logger.Info("daemon stopping new fetches, draining in-flight batches")
	wg.Wait()

	flushCtx, cancel := context.WithTimeout(context.Background(), r.cfg.DrainTimeout)
	defer cancel()
	if err := r.sink.Flush(flushCtx); err != nil {
		return fmt.Errorf("flush sink on shutdown: %w", err)
	}
	return nil
}

func (r *Runner) sourceWorkerLoop(ctx context.Context, src source.Source) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := r.runSourcePass(ctx, src); err != nil && !errors.Is(err, context.Canceled) {
			r.logger.Error("source pass failed", "source", src.Name(), "error", err)
		}
		if ctx.Err() != nil {
			return
		}
		timer := time.NewTimer(r.cfg.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (r *Runner) runSourcePass(ctx context.Context, src source.Source) error {
	jobs := r.jobs[src.Name()]
	if len(jobs) == 0 {
		jobs = []source.Job{{}}
	}
	// A source may split a job into several cursor scopes (arXiv: one scope per OAI set).
	if splitter, ok := src.(source.JobSplitter); ok {
		var expanded []source.Job
		for _, job := range jobs {
			expanded = append(expanded, splitter.SplitJobs(job)...)
		}
		jobs = expanded
	}

	// A failing scope does not stop the others; every failure is reported so --once exits with 1.
	var errs []error
	for _, job := range jobs {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if err := r.runJob(ctx, src, job); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// runJob harvests one scope. Cursor rules (§5.3):
//   - the cursor of a scope is read from state and moves only after Kafka acknowledged the records behind it;
//   - an ordered source (OpenAlex sorted by updated_date) commits after every acknowledged batch, with the
//     highest cursor among acknowledged records;
//   - an unordered source (arXiv) commits once, after the whole job is acknowledged;
//   - a terminal source error or an interrupted context never finalises an unordered cursor.
func (r *Runner) runJob(ctx context.Context, src source.Source, job source.Job) error {
	scope := job.Scope()
	cur, ok, err := r.state.GetCursor(ctx, src.Name(), scope)
	if err != nil {
		return fmt.Errorf("load cursor (%s/%s): %w", src.Name(), scope, err)
	}
	if ok && job.Since == "" {
		job.Since = cur.CursorValue
	}
	ordered := source.IsOrdered(src)

	// The fetch context is cancelled on return, so the source goroutine never blocks on an abandoned channel.
	fetchCtx, cancelFetch := context.WithCancel(ctx)
	ch, err := src.Fetch(fetchCtx, job)
	if err != nil {
		cancelFetch()
		_ = r.state.RecordError(context.Background(), src.Name(), scope, err.Error())
		return fmt.Errorf("fetch (%s/%s): %w", src.Name(), scope, err)
	}
	defer func() {
		cancelFetch()
		go func() {
			for range ch {
			}
		}()
	}()

	var (
		batch    []sink.BatchItem
		batchMax string      // highest cursor among records of the pending batch
		acked    = job.Since // highest cursor such that all records up to it are acknowledged
	)

	// ackBatch produces the pending batch, waits for Kafka acknowledgement and moves acked forward.
	ackBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		// Detach from cancellation so already-fetched records are produced and committed on SIGTERM.
		drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.cfg.DrainTimeout)
		defer cancel()

		if err := r.sink.ProduceBatch(drainCtx, batch); err != nil {
			_ = r.state.RecordError(drainCtx, src.Name(), scope, err.Error())
			return fmt.Errorf("produce batch (%s/%s): %w", src.Name(), scope, err)
		}
		if err := r.sink.Flush(drainCtx); err != nil {
			_ = r.state.RecordError(drainCtx, src.Name(), scope, err.Error())
			return fmt.Errorf("flush batch (%s/%s): %w", src.Name(), scope, err)
		}

		if batchMax > acked {
			acked = batchMax
		}
		batch = batch[:0]
		batchMax = ""
		if ordered {
			return r.commitCursor(drainCtx, src.Name(), scope, acked)
		}
		return nil
	}

	var fetchErr error
	for rec := range ch {
		if rec.Err != nil {
			fetchErr = rec.Err
			break
		}
		r.metrics.IncFetched(src.Name())
		now := r.cfg.Now()
		res := pipeline.Process(rec, now)
		if res.Paper != nil {
			r.metrics.IncValid(src.Name())
		} else if res.DLQ != nil {
			r.metrics.IncDLQ(src.Name(), res.DLQ.Reason)
		}

		if rec.Cursor > batchMax {
			batchMax = rec.Cursor
		}

		batch = append(batch, sink.BatchItem{
			Result:  res,
			RawBody: rec.Body,
		})

		if len(batch) >= r.cfg.BatchSize {
			if err := ackBatch(); err != nil {
				return err
			}
		}
	}

	// Records fetched before a failure or an interruption are still valid; produce them.
	if err := ackBatch(); err != nil {
		return err
	}
	if fetchErr != nil {
		_ = r.state.RecordError(context.Background(), src.Name(), scope, fetchErr.Error())
		return fmt.Errorf("fetch (%s/%s): %w", src.Name(), scope, fetchErr)
	}
	if err := ctx.Err(); err != nil {
		// The stream may have been cut short; the job is not complete, so an unordered cursor stays.
		return err
	}
	if !ordered {
		if err := r.commitCursor(ctx, src.Name(), scope, acked); err != nil {
			return err
		}
	}
	return nil
}

// commitCursor persists value as the cursor of (source, scope). An empty value is a no-op.
func (r *Runner) commitCursor(ctx context.Context, sourceName, scope, value string) error {
	if value == "" {
		return nil
	}
	now := r.cfg.Now()
	if err := r.state.SaveCursor(ctx, state.Cursor{
		Source:        sourceName,
		Scope:         scope,
		CursorValue:   value,
		LastSuccessAt: &now,
	}); err != nil {
		return fmt.Errorf("save cursor (%s/%s): %w", sourceName, scope, err)
	}
	r.metrics.SetLastSuccess(sourceName, scope, now)
	return nil
}
