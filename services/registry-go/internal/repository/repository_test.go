package repository

import (
	"errors"
	"io/fs"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/scipulse/scipulse/services/registry-go/internal/model"
	"github.com/scipulse/scipulse/services/registry-go/migrations"
)

func TestMapPgError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		err          error
		wantNotFound bool
		wantConflict bool
		wantVal      bool
		wantField    string
	}{
		{
			name: "nil error",
			err:  nil,
		},
		{
			name:         "pgx ErrNoRows maps to ErrNotFound",
			err:          pgx.ErrNoRows,
			wantNotFound: true,
		},
		{
			name:         "23505 slug unique violation",
			err:          &pgconn.PgError{Code: "23505", ConstraintName: "tracked_projects_slug_key"},
			wantConflict: true,
			wantField:    "slug",
		},
		{
			name:         "23505 github_full_name unique violation",
			err:          &pgconn.PgError{Code: "23505", ConstraintName: "project_repos_github_full_name_key"},
			wantConflict: true,
			wantField:    "github_full_name",
		},
		{
			name:         "23505 one primary repo violation",
			err:          &pgconn.PgError{Code: "23505", ConstraintName: "project_repos_one_primary"},
			wantConflict: true,
			wantField:    "is_primary",
		},
		{
			name:         "23505 institution ror_id violation",
			err:          &pgconn.PgError{Code: "23505", ConstraintName: "institutions_ror_id_key"},
			wantConflict: true,
			wantField:    "ror_id",
		},
		{
			name:         "23503 foreign key violation maps to ErrNotFound",
			err:          &pgconn.PgError{Code: "23503", ConstraintName: "project_repos_project_id_fkey"},
			wantNotFound: true,
		},
		{
			name:      "23514 check constraint maps to ValidationError",
			err:       &pgconn.PgError{Code: "23514", ConstraintName: "tracked_projects_priority_check", Message: "violates check constraint"},
			wantVal:   true,
			wantField: "priority",
		},
		{
			name:    "22P02 invalid input maps to ValidationError",
			err:     &pgconn.PgError{Code: "22P02", Message: "invalid input syntax"},
			wantVal: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := mapPgError(tc.err)
			if tc.err == nil {
				if got != nil {
					t.Fatalf("expected nil, got %v", got)
				}
				return
			}
			if tc.wantNotFound && !errors.Is(got, model.ErrNotFound) {
				t.Fatalf("expected ErrNotFound, got %v", got)
			}
			if tc.wantConflict {
				if !errors.Is(got, model.ErrConflict) {
					t.Fatalf("expected ErrConflict, got %v", got)
				}
				var ce *model.ConflictError
				if !errors.As(got, &ce) {
					t.Fatalf("expected *model.ConflictError, got %T", got)
				}
				if ce.Field != tc.wantField {
					t.Fatalf("expected conflict field %q, got %q", tc.wantField, ce.Field)
				}
			}
			if tc.wantVal {
				var ve *model.ValidationError
				if !errors.As(got, &ve) {
					t.Fatalf("expected *model.ValidationError, got %T (%v)", got, got)
				}
				if tc.wantField != "" && (len(ve.Errors) == 0 || ve.Errors[0].Field != tc.wantField) {
					t.Fatalf("expected validation field %q, got %+v", tc.wantField, ve.Errors)
				}
			}
		})
	}
}

func TestPgxPoolCollectorDescribe(t *testing.T) {
	t.Parallel()

	col := NewPgxPoolCollector(nil, "registry")
	reg := prometheus.NewRegistry()
	if err := reg.Register(col); err != nil {
		t.Fatalf("failed to register PgxPoolCollector: %v", err)
	}
	ch := make(chan *prometheus.Desc, 16)
	col.Describe(ch)
	close(ch)
	count := 0
	for range ch {
		count++
	}
	if count != 9 {
		t.Fatalf("expected 9 metric descriptors, got %d", count)
	}
}

func TestEmbeddedMigrationsExist(t *testing.T) {
	t.Parallel()

	expected := []string{
		"000001_init.up.sql",
		"000001_init.down.sql",
		"000002_cdc.up.sql",
		"000002_cdc.down.sql",
	}
	for _, name := range expected {
		data, err := fs.ReadFile(migrations.FS, name)
		if err != nil {
			t.Fatalf("missing embedded migration %s: %v", name, err)
		}
		if len(data) == 0 {
			t.Fatalf("empty embedded migration %s", name)
		}
	}
}
