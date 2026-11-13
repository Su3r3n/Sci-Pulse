package state

import (
	"context"
	"time"

	"github.com/scipulse/scipulse/services/harvester-go/internal/ratelimit"
)

// Cursor represents a harvest progress row in harvest_cursors.
type Cursor struct {
	Source        string
	Scope         string
	CursorValue   string
	LastSuccessAt *time.Time
	LastError     *string
	UpdatedAt     time.Time
}

// ETagEntry represents a cached HTTP ETag row in http_etags.
type ETagEntry struct {
	URL        string
	ETag       string
	LastStatus int16
	FetchedAt  time.Time
}

// Store persists harvester cursors, HTTP ETags, and daily API budgets.
type Store interface {
	ratelimit.BudgetStore
	Migrate(ctx context.Context) error
	Ping(ctx context.Context) error
	GetCursor(ctx context.Context, source, scope string) (Cursor, bool, error)
	SaveCursor(ctx context.Context, cursor Cursor) error
	RecordError(ctx context.Context, source, scope, errMsg string) error
	GetETag(ctx context.Context, url string) (ETagEntry, bool, error)
	SaveETag(ctx context.Context, entry ETagEntry) error
}
