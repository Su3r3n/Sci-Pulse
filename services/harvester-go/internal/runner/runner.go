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

	for _, job := range jobs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := r.runJob(ctx, src, job); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) runJob(ctx context.Context, src source.Source, job source.Job) error {
	scope := job.Scope()
	if cur, ok, err := r.state.GetCursor(ctx, src.Name(), scope); err == nil && ok && job.Since == "" {
		job.Since = cur.CursorValue
	} else if err != nil {
		return fmt.Errorf("load cursor (%s/%s): %w", src.Name(), scope, err)
	}

	ch, err := src.Fetch(ctx, job)
	if err != nil {
		_ = r.state.RecordError(context.Background(), src.Name(), scope, err.Error())
		return fmt.Errorf("fetch (%s/%s): %w", src.Name(), scope, err)
	}

	batch := make([]sink.BatchItem, 0, r.cfg.BatchSize)
	latestCursor := ""

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		// Detach from fetch cancellation so already-fetched records are flushed & committed on SIGTERM.
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

		now := r.cfg.Now()
		cursorToSave := latestCursor
		if cursorToSave == "" {
			cursorToSave = job.Since
		}
		if cursorToSave != "" {
			if err := r.state.SaveCursor(drainCtx, state.Cursor{
				Source:        src.Name(),
				Scope:         scope,
				CursorValue:   cursorToSave,
				LastSuccessAt: &now,
			}); err != nil {
				return fmt.Errorf("save cursor (%s/%s): %w", src.Name(), scope, err)
			}
		}
		r.metrics.SetLastSuccess(src.Name(), scope, now)
		batch = batch[:0]
		return nil
	}

	for rec := range ch {
		r.metrics.IncFetched(src.Name())
		now := r.cfg.Now()
		res := pipeline.Process(rec, now)
		if res.Paper != nil {
			r.metrics.IncValid(src.Name())
		} else if res.DLQ != nil {
			r.metrics.IncDLQ(src.Name(), res.DLQ.Reason)
		}

		if rec.Cursor != "" && rec.Cursor > latestCursor {
			latestCursor = rec.Cursor
		}

		batch = append(batch, sink.BatchItem{
			Result:  res,
			RawBody: rec.Body,
		})

		if len(batch) >= r.cfg.BatchSize {
			if err := flushBatch(); err != nil {
				return err
			}
		}
	}

	return flushBatch()
}
