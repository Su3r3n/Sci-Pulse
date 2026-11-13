package state

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS harvest_cursors (
  source          text NOT NULL,
  scope           text NOT NULL,
  cursor_value    text,
  last_success_at timestamptz,
  last_error      text,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source, scope)
);

CREATE TABLE IF NOT EXISTS http_etags (
  url         text PRIMARY KEY,
  etag        text NOT NULL,
  last_status smallint NOT NULL,
  fetched_at  timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS source_budget (
  source  text NOT NULL,
  day     date NOT NULL,
  spent   integer NOT NULL DEFAULT 0,
  budget  integer,
  PRIMARY KEY (source, day)
);
`

// PostgresStore persists harvester state in the harvester Postgres database.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore creates a PostgresStore using the given pgxpool.Pool.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// Migrate creates harvester state tables idempotently.
func (s *PostgresStore) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("migrate harvester schema: %w", err)
	}
	return nil
}

// Ping checks Postgres connectivity.
func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// GetCursor loads the cursor for (source, scope).
func (s *PostgresStore) GetCursor(ctx context.Context, source, scope string) (Cursor, bool, error) {
	var c Cursor
	var cursorVal *string
	err := s.pool.QueryRow(ctx, `
		SELECT source, scope, cursor_value, last_success_at, last_error, updated_at
		FROM harvest_cursors
		WHERE source = $1 AND scope = $2
	`, source, scope).Scan(&c.Source, &c.Scope, &cursorVal, &c.LastSuccessAt, &c.LastError, &c.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Cursor{}, false, nil
		}
		return Cursor{}, false, fmt.Errorf("get cursor: %w", err)
	}
	if cursorVal != nil {
		c.CursorValue = *cursorVal
	}
	return c, true, nil
}

// SaveCursor upserts a successfully committed cursor for (source, scope).
func (s *PostgresStore) SaveCursor(ctx context.Context, cursor Cursor) error {
	successAt := time.Now().UTC()
	if cursor.LastSuccessAt != nil {
		successAt = *cursor.LastSuccessAt
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO harvest_cursors (source, scope, cursor_value, last_success_at, last_error, updated_at)
		VALUES ($1, $2, $3, $4, NULL, now())
		ON CONFLICT (source, scope) DO UPDATE
		SET cursor_value = EXCLUDED.cursor_value,
		    last_success_at = EXCLUDED.last_success_at,
		    last_error = NULL,
		    updated_at = now()
	`, cursor.Source, cursor.Scope, cursor.CursorValue, successAt)
	if err != nil {
		return fmt.Errorf("save cursor: %w", err)
	}
	return nil
}

// RecordError saves last_error without advancing cursor_value.
func (s *PostgresStore) RecordError(ctx context.Context, source, scope, errMsg string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO harvest_cursors (source, scope, last_error, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (source, scope) DO UPDATE
		SET last_error = EXCLUDED.last_error,
		    updated_at = now()
	`, source, scope, errMsg)
	if err != nil {
		return fmt.Errorf("record error: %w", err)
	}
	return nil
}

// GetETag loads a stored HTTP ETag for url.
func (s *PostgresStore) GetETag(ctx context.Context, url string) (ETagEntry, bool, error) {
	var e ETagEntry
	err := s.pool.QueryRow(ctx, `
		SELECT url, etag, last_status, fetched_at
		FROM http_etags
		WHERE url = $1
	`, url).Scan(&e.URL, &e.ETag, &e.LastStatus, &e.FetchedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ETagEntry{}, false, nil
		}
		return ETagEntry{}, false, fmt.Errorf("get etag: %w", err)
	}
	return e, true, nil
}

// SaveETag upserts an HTTP ETag row.
func (s *PostgresStore) SaveETag(ctx context.Context, entry ETagEntry) error {
	fetchedAt := entry.FetchedAt
	if fetchedAt.IsZero() {
		fetchedAt = time.Now().UTC()
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO http_etags (url, etag, last_status, fetched_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (url) DO UPDATE
		SET etag = EXCLUDED.etag,
		    last_status = EXCLUDED.last_status,
		    fetched_at = EXCLUDED.fetched_at
	`, entry.URL, entry.ETag, entry.LastStatus, fetchedAt)
	if err != nil {
		return fmt.Errorf("save etag: %w", err)
	}
	return nil
}

// GetBudget returns spent and budget for (source, day).
func (s *PostgresStore) GetBudget(ctx context.Context, source string, day time.Time) (int, *int, error) {
	var spent int
	var budget *int
	err := s.pool.QueryRow(ctx, `
		SELECT spent, budget
		FROM source_budget
		WHERE source = $1 AND day = $2::date
	`, source, day.UTC().Format("2006-01-02")).Scan(&spent, &budget)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil, nil
		}
		return 0, nil, fmt.Errorf("get budget: %w", err)
	}
	return spent, budget, nil
}

// AddBudgetSpent atomically increments spent for (source, day).
func (s *PostgresStore) AddBudgetSpent(ctx context.Context, source string, day time.Time, delta int, budget *int) (int, *int, error) {
	var spent int
	var storedBudget *int
	err := s.pool.QueryRow(ctx, `
		INSERT INTO source_budget (source, day, spent, budget)
		VALUES ($1, $2::date, $3, $4)
		ON CONFLICT (source, day) DO UPDATE
		SET spent = source_budget.spent + EXCLUDED.spent,
		    budget = COALESCE(source_budget.budget, EXCLUDED.budget)
		RETURNING spent, budget
	`, source, day.UTC().Format("2006-01-02"), delta, budget).Scan(&spent, &storedBudget)
	if err != nil {
		return 0, nil, fmt.Errorf("add budget spent: %w", err)
	}
	if storedBudget == nil {
		return spent, nil, nil
	}
	rem := *storedBudget - spent
	return spent, &rem, nil
}

// SetBudgetSnapshot stores an authoritative spent and budget count from an external rate-limit endpoint.
func (s *PostgresStore) SetBudgetSnapshot(ctx context.Context, source string, day time.Time, spent int, budget int) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO source_budget (source, day, spent, budget)
		VALUES ($1, $2::date, $3, $4)
		ON CONFLICT (source, day) DO UPDATE
		SET spent = EXCLUDED.spent,
		    budget = EXCLUDED.budget
	`, source, day.UTC().Format("2006-01-02"), spent, budget)
	if err != nil {
		return fmt.Errorf("set budget snapshot: %w", err)
	}
	return nil
}
