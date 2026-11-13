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
	ParseError error     // non-nil when page/record framing failed during parse
}
