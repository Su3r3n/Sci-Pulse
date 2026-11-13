package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/scipulse/scipulse/services/registry-go/internal/model"
)

// Repository defines persistence operations for the registry domain.
type Repository interface {
	Ping(ctx context.Context) error
	ListProjects(ctx context.Context, params model.ListProjectsParams) (model.ListProjectsResponse, error)
	CreateProject(ctx context.Context, req model.CreateProjectRequest) (model.Project, error)
	GetProject(ctx context.Context, id int64) (model.ProjectDetail, error)
	UpdateProject(ctx context.Context, id int64, req model.UpdateProjectRequest) (model.Project, error)
	DeleteProject(ctx context.Context, id int64) error

	ListProjectRepos(ctx context.Context, projectID int64) ([]model.Repo, error)
	CreateProjectRepo(ctx context.Context, projectID int64, req model.CreateRepoRequest) (model.Repo, error)
	DeleteProjectRepo(ctx context.Context, projectID, repoID int64) error

	ListInstitutions(ctx context.Context) ([]model.Institution, error)
	CreateInstitution(ctx context.Context, req model.CreateInstitutionRequest) (model.Institution, error)

	LinkProjectInstitution(ctx context.Context, projectID, institutionID int64) error
	UnlinkProjectInstitution(ctx context.Context, projectID, institutionID int64) error
}

// PostgresRepository implements Repository using pgxpool.
type PostgresRepository struct {
	pool *pgxpool.Pool
}

// NewPostgresRepository wraps an initialized pgxpool.Pool.
func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

// Ping verifies database connectivity.
func (r *PostgresRepository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

// ListProjects returns cursor-paginated projects ordered by id ascending.
func (r *PostgresRepository) ListProjects(ctx context.Context, params model.ListProjectsParams) (model.ListProjectsResponse, error) {
	fetchLimit := params.Limit + 1
	var (
		rows pgx.Rows
		err  error
	)
	if params.Status != nil {
		rows, err = r.pool.Query(ctx, `
			SELECT id, slug, name, description, keywords, topics, priority, status::text, created_at, updated_at
			FROM tracked_projects
			WHERE id > $1 AND status = $2::project_status
			ORDER BY id ASC
			LIMIT $3
		`, params.Cursor, string(*params.Status), fetchLimit)
	} else {
		rows, err = r.pool.Query(ctx, `
			SELECT id, slug, name, description, keywords, topics, priority, status::text, created_at, updated_at
			FROM tracked_projects
			WHERE id > $1
			ORDER BY id ASC
			LIMIT $2
		`, params.Cursor, fetchLimit)
	}
	if err != nil {
		return model.ListProjectsResponse{}, mapPgError(err)
	}
	defer rows.Close()

	items := make([]model.Project, 0, params.Limit)
	for rows.Next() {
		p, err := scanProject(rows)
		if err != nil {
			return model.ListProjectsResponse{}, err
		}
		items = append(items, p)
	}
	if err := rows.Err(); err != nil {
		return model.ListProjectsResponse{}, mapPgError(err)
	}

	var nextCursor *int64
	if len(items) > params.Limit {
		items = items[:params.Limit]
		lastID := items[len(items)-1].ID
		nextCursor = &lastID
	}

	return model.ListProjectsResponse{
		Items:      items,
		NextCursor: nextCursor,
	}, nil
}

// CreateProject inserts a new tracked_projects row.
func (r *PostgresRepository) CreateProject(ctx context.Context, req model.CreateProjectRequest) (model.Project, error) {
	keywords := req.Keywords
	if keywords == nil {
		keywords = []string{}
	}
	topics := req.Topics
	if topics == nil {
		topics = []string{}
	}
	priority := int16(3)
	if req.Priority != nil {
		priority = *req.Priority
	}
	status := model.ProjectStatusActive
	if req.Status != nil {
		status = *req.Status
	}

	row := r.pool.QueryRow(ctx, `
		INSERT INTO tracked_projects (slug, name, description, keywords, topics, priority, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7::project_status)
		RETURNING id, slug, name, description, keywords, topics, priority, status::text, created_at, updated_at
	`, req.Slug, req.Name, req.Description, keywords, topics, priority, string(status))

	p, err := scanProject(row)
	if err != nil {
		return model.Project{}, mapPgError(err)
	}
	return p, nil
}

// GetProject fetches a project along with its repos and institutions.
func (r *PostgresRepository) GetProject(ctx context.Context, id int64) (model.ProjectDetail, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, slug, name, description, keywords, topics, priority, status::text, created_at, updated_at
		FROM tracked_projects
		WHERE id = $1
	`, id)
	p, err := scanProject(row)
	if err != nil {
		return model.ProjectDetail{}, mapPgError(err)
	}

	repos, err := r.queryProjectRepos(ctx, id)
	if err != nil {
		return model.ProjectDetail{}, err
	}

	insts, err := r.queryProjectInstitutions(ctx, id)
	if err != nil {
		return model.ProjectDetail{}, err
	}

	return model.ProjectDetail{
		Project:      p,
		Repos:        repos,
		Institutions: insts,
	}, nil
}

// UpdateProject applies partial updates to tracked_projects.
func (r *PostgresRepository) UpdateProject(ctx context.Context, id int64, req model.UpdateProjectRequest) (model.Project, error) {
	var (
		sets []string
		args []any
	)
	addSet := func(col string, val any) {
		args = append(args, val)
		sets = append(sets, fmt.Sprintf("%s = $%d", col, len(args)))
	}

	if req.Slug != nil {
		addSet("slug", *req.Slug)
	}
	if req.Name != nil {
		addSet("name", *req.Name)
	}
	if req.Description.Set {
		addSet("description", req.Description.Value)
	}
	if req.Keywords != nil {
		kw := *req.Keywords
		if kw == nil {
			kw = []string{}
		}
		addSet("keywords", kw)
	}
	if req.Topics != nil {
		tp := *req.Topics
		if tp == nil {
			tp = []string{}
		}
		addSet("topics", tp)
	}
	if req.Priority != nil {
		addSet("priority", *req.Priority)
	}
	if req.Status != nil {
		args = append(args, string(*req.Status))
		sets = append(sets, fmt.Sprintf("status = $%d::project_status", len(args)))
	}

	if len(sets) == 0 {
		row := r.pool.QueryRow(ctx, `
			SELECT id, slug, name, description, keywords, topics, priority, status::text, created_at, updated_at
			FROM tracked_projects
			WHERE id = $1
		`, id)
		p, err := scanProject(row)
		if err != nil {
			return model.Project{}, mapPgError(err)
		}
		return p, nil
	}

	args = append(args, id)
	query := fmt.Sprintf(`
		UPDATE tracked_projects
		SET %s
		WHERE id = $%d
		RETURNING id, slug, name, description, keywords, topics, priority, status::text, created_at, updated_at
	`, strings.Join(sets, ", "), len(args))

	row := r.pool.QueryRow(ctx, query, args...)
	p, err := scanProject(row)
	if err != nil {
		return model.Project{}, mapPgError(err)
	}
	return p, nil
}

// DeleteProject removes a project (cascading to repos and project_institutions).
func (r *PostgresRepository) DeleteProject(ctx context.Context, id int64) error {
	tag, err := r.pool.Exec(ctx, `DELETE FROM tracked_projects WHERE id = $1`, id)
	if err != nil {
		return mapPgError(err)
	}
	if tag.RowsAffected() == 0 {
		return model.ErrNotFound
	}
	return nil
}

// ListProjectRepos returns repositories for a project, or ErrNotFound if the project does not exist.
func (r *PostgresRepository) ListProjectRepos(ctx context.Context, projectID int64) ([]model.Repo, error) {
	var exists int
	err := r.pool.QueryRow(ctx, `SELECT 1 FROM tracked_projects WHERE id = $1`, projectID).Scan(&exists)
	if err != nil {
		return nil, mapPgError(err)
	}
	return r.queryProjectRepos(ctx, projectID)
}

// CreateProjectRepo attaches a repository to a project. If IsPrimary is true, any previous
// primary repository for that project is unset inside the same transaction.
func (r *PostgresRepository) CreateProjectRepo(ctx context.Context, projectID int64, req model.CreateRepoRequest) (model.Repo, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return model.Repo{}, err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var lockedID int64
	if err := tx.QueryRow(ctx, `SELECT id FROM tracked_projects WHERE id = $1 FOR UPDATE`, projectID).Scan(&lockedID); err != nil {
		return model.Repo{}, mapPgError(err)
	}

	if req.IsPrimary {
		if _, err := tx.Exec(ctx, `
			UPDATE project_repos
			SET is_primary = false
			WHERE project_id = $1 AND is_primary = true
		`, projectID); err != nil {
			return model.Repo{}, mapPgError(err)
		}
	}

	row := tx.QueryRow(ctx, `
		INSERT INTO project_repos (project_id, github_full_name, is_primary)
		VALUES ($1, $2, $3)
		RETURNING id, project_id, github_full_name, is_primary, created_at, updated_at
	`, projectID, req.GitHubFullName, req.IsPrimary)

	repo, err := scanRepo(row)
	if err != nil {
		return model.Repo{}, mapPgError(err)
	}

	if err := tx.Commit(ctx); err != nil {
		return model.Repo{}, mapPgError(err)
	}
	return repo, nil
}

// DeleteProjectRepo detaches a repository from a project.
func (r *PostgresRepository) DeleteProjectRepo(ctx context.Context, projectID, repoID int64) error {
	tag, err := r.pool.Exec(ctx, `
		DELETE FROM project_repos
		WHERE id = $1 AND project_id = $2
	`, repoID, projectID)
	if err != nil {
		return mapPgError(err)
	}
	if tag.RowsAffected() == 0 {
		return model.ErrNotFound
	}
	return nil
}

// ListInstitutions lists all institutions ordered by id ascending.
func (r *PostgresRepository) ListInstitutions(ctx context.Context) ([]model.Institution, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, ror_id, name, country, created_at, updated_at
		FROM institutions
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, mapPgError(err)
	}
	defer rows.Close()

	items := make([]model.Institution, 0)
	for rows.Next() {
		inst, err := scanInstitution(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, inst)
	}
	if err := rows.Err(); err != nil {
		return nil, mapPgError(err)
	}
	return items, nil
}

// CreateInstitution creates a new institution record.
func (r *PostgresRepository) CreateInstitution(ctx context.Context, req model.CreateInstitutionRequest) (model.Institution, error) {
	row := r.pool.QueryRow(ctx, `
		INSERT INTO institutions (ror_id, name, country)
		VALUES ($1, $2, $3)
		RETURNING id, ror_id, name, country, created_at, updated_at
	`, req.RORID, req.Name, req.Country)

	inst, err := scanInstitution(row)
	if err != nil {
		return model.Institution{}, mapPgError(err)
	}
	return inst, nil
}

// LinkProjectInstitution idempotently links an institution to a project.
func (r *PostgresRepository) LinkProjectInstitution(ctx context.Context, projectID, institutionID int64) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO project_institutions (project_id, institution_id)
		VALUES ($1, $2)
		ON CONFLICT (project_id, institution_id) DO NOTHING
	`, projectID, institutionID)
	if err != nil {
		return mapPgError(err)
	}
	return nil
}

// UnlinkProjectInstitution removes the link between a project and an institution.
func (r *PostgresRepository) UnlinkProjectInstitution(ctx context.Context, projectID, institutionID int64) error {
	tag, err := r.pool.Exec(ctx, `
		DELETE FROM project_institutions
		WHERE project_id = $1 AND institution_id = $2
	`, projectID, institutionID)
	if err != nil {
		return mapPgError(err)
	}
	if tag.RowsAffected() == 0 {
		return model.ErrNotFound
	}
	return nil
}

func (r *PostgresRepository) queryProjectRepos(ctx context.Context, projectID int64) ([]model.Repo, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, project_id, github_full_name, is_primary, created_at, updated_at
		FROM project_repos
		WHERE project_id = $1
		ORDER BY is_primary DESC, id ASC
	`, projectID)
	if err != nil {
		return nil, mapPgError(err)
	}
	defer rows.Close()

	repos := make([]model.Repo, 0)
	for rows.Next() {
		repo, err := scanRepo(rows)
		if err != nil {
			return nil, err
		}
		repos = append(repos, repo)
	}
	if err := rows.Err(); err != nil {
		return nil, mapPgError(err)
	}
	return repos, nil
}

func (r *PostgresRepository) queryProjectInstitutions(ctx context.Context, projectID int64) ([]model.Institution, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT i.id, i.ror_id, i.name, i.country, i.created_at, i.updated_at
		FROM institutions i
		JOIN project_institutions pi ON pi.institution_id = i.id
		WHERE pi.project_id = $1
		ORDER BY i.id ASC
	`, projectID)
	if err != nil {
		return nil, mapPgError(err)
	}
	defer rows.Close()

	insts := make([]model.Institution, 0)
	for rows.Next() {
		inst, err := scanInstitution(rows)
		if err != nil {
			return nil, err
		}
		insts = append(insts, inst)
	}
	if err := rows.Err(); err != nil {
		return nil, mapPgError(err)
	}
	return insts, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanProject(s scanner) (model.Project, error) {
	var (
		p      model.Project
		status string
	)
	if err := s.Scan(
		&p.ID,
		&p.Slug,
		&p.Name,
		&p.Description,
		&p.Keywords,
		&p.Topics,
		&p.Priority,
		&status,
		&p.CreatedAt,
		&p.UpdatedAt,
	); err != nil {
		return model.Project{}, err
	}
	p.Status = model.ProjectStatus(status)
	if p.Keywords == nil {
		p.Keywords = []string{}
	}
	if p.Topics == nil {
		p.Topics = []string{}
	}
	return p, nil
}

func scanRepo(s scanner) (model.Repo, error) {
	var repo model.Repo
	if err := s.Scan(
		&repo.ID,
		&repo.ProjectID,
		&repo.GitHubFullName,
		&repo.IsPrimary,
		&repo.CreatedAt,
		&repo.UpdatedAt,
	); err != nil {
		return model.Repo{}, err
	}
	return repo, nil
}

func scanInstitution(s scanner) (model.Institution, error) {
	var inst model.Institution
	if err := s.Scan(
		&inst.ID,
		&inst.RORID,
		&inst.Name,
		&inst.Country,
		&inst.CreatedAt,
		&inst.UpdatedAt,
	); err != nil {
		return model.Institution{}, err
	}
	if inst.Country != nil {
		trimmed := strings.TrimSpace(*inst.Country)
		inst.Country = &trimmed
	}
	return inst, nil
}

// mapPgError translates pgx/PostgreSQL errors into domain errors.
func mapPgError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505": // unique_violation
			return mapUniqueViolation(pgErr)
		case "23503": // foreign_key_violation
			return model.ErrNotFound
		case "23514", "23502", "22P02", "22001": // check, not_null, invalid_text_representation, string_data_right_truncation
			return &model.ValidationError{
				Detail: "database constraint validation failed",
				Errors: []model.FieldError{{
					Field:   constraintField(pgErr.ConstraintName),
					Message: pgErr.Message,
					Code:    pgErr.Code,
				}},
			}
		}
	}
	return err
}

func mapUniqueViolation(pgErr *pgconn.PgError) error {
	switch pgErr.ConstraintName {
	case "tracked_projects_slug_key":
		return &model.ConflictError{
			Field:  "slug",
			Detail: "project with this slug already exists",
		}
	case "project_repos_github_full_name_key":
		return &model.ConflictError{
			Field:  "github_full_name",
			Detail: "repository is already attached to a project",
		}
	case "project_repos_one_primary":
		return &model.ConflictError{
			Field:  "is_primary",
			Detail: "project already has a primary repository",
		}
	case "institutions_ror_id_key":
		return &model.ConflictError{
			Field:  "ror_id",
			Detail: "institution with this ror_id already exists",
		}
	default:
		return &model.ConflictError{
			Detail: "resource already exists",
		}
	}
}

func constraintField(constraint string) string {
	switch {
	case strings.Contains(constraint, "slug"):
		return "slug"
	case strings.Contains(constraint, "priority"):
		return "priority"
	case strings.Contains(constraint, "github_full_name"):
		return "github_full_name"
	case strings.Contains(constraint, "ror_id"):
		return "ror_id"
	default:
		return ""
	}
}
