package source

import (
	"context"
	"strings"
	"time"
)

// Source fetches raw records for a harvest Job.
type Source interface {
	Name() string
	Fetch(ctx context.Context, job Job) (<-chan RawRecord, error)
}

// Job describes a unit of harvest work (by project or general topics).
type Job struct {
	Project  string   // slug from registry; empty for general topics
	Topics   []string // topics or OAI-PMH sets
	Keywords []string // search keywords
	Repos    []string // for github, gharchive, zenodo
	Since    string   // cursor from state
}

// Scope returns the state cursor scope key for this job.
func (j Job) Scope() string {
	if strings.TrimSpace(j.Project) != "" {
		return strings.TrimSpace(j.Project)
	}
	if len(j.Topics) > 0 {
		return strings.Join(j.Topics, ",")
	}
	return "default"
}

// RawRecord represents a single fetched raw record ready for the pipeline.
type RawRecord struct {
	Source     string
	ID         string
	Body       []byte    // raw record bytes; stored in DLQ payload on failure
	Fetched    time.Time // fetch timestamp
	Cursor     string    // candidate cursor value after this record is confirmed
	ParseError error     // non-nil when a single record failed to parse (goes to DLQ)

	// Err, when non-nil, is terminal: the source stopped early (HTTP error, exhausted
	// retries, broken pagination). The runner fails the job, keeps the cursor where
	// the last acknowledged batch left it and never treats the pass as complete.
	// A source sends at most one Err record, as its last element, then closes the channel.
	Err error
}

// OrderedSource is an optional interface. A source implements it and returns true when
// its records arrive in non-decreasing Cursor order (for example OpenAlex with
// sort=updated_date:asc). Only such sources may move the cursor after every acknowledged
// batch; for all other sources the cursor moves once, after the whole job is acknowledged.
type OrderedSource interface {
	OrderedCursor() bool
}

// IsOrdered reports whether src delivers records in cursor order.
func IsOrdered(src Source) bool {
	o, ok := src.(OrderedSource)
	return ok && o.OrderedCursor()
}

// JobSplitter is an optional interface. A source implements it to turn one Job into
// several Jobs, each with its own cursor scope (for example one arXiv OAI set per scope).
type JobSplitter interface {
	SplitJobs(job Job) []Job
}

// sendRecord delivers rec unless ctx is cancelled first.
func sendRecord(ctx context.Context, out chan<- RawRecord, rec RawRecord) error {
	select {
	case out <- rec:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sendFatal delivers a terminal Err record for the source; it is dropped if ctx is cancelled.
func sendFatal(ctx context.Context, out chan<- RawRecord, source string, err error) {
	_ = sendRecord(ctx, out, RawRecord{Source: source, Err: err})
}
