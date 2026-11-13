package service

import (
	"context"
	"regexp"
	"strings"

	"github.com/scipulse/scipulse/services/registry-go/internal/model"
	"github.com/scipulse/scipulse/services/registry-go/internal/repository"
)

const (
	DefaultListLimit = 50
	MaxListLimit     = 100
)

var (
	slugRegex           = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,62}$`)
	githubFullNameRegex = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
	countryRegex        = regexp.MustCompile(`^[A-Za-z]{2}$`)
)

// RegistryService enforces domain rules and delegates persistence to Repository.
type RegistryService struct {
	repo repository.Repository
}

// NewRegistryService constructs a RegistryService.
func NewRegistryService(repo repository.Repository) *RegistryService {
	return &RegistryService{repo: repo}
}

// Ping checks repository connectivity.
func (s *RegistryService) Ping(ctx context.Context) error {
	return s.repo.Ping(ctx)
}

// ListProjects validates filter/pagination parameters and returns projects.
func (s *RegistryService) ListProjects(ctx context.Context, params model.ListProjectsParams) (model.ListProjectsResponse, error) {
	var errs []model.FieldError

	if params.Status != nil && !params.Status.IsValid() {
		errs = append(errs, model.FieldError{
			Field:   "status",
			Message: "status must be 'active' or 'paused'",
			Code:    "invalid_enum",
		})
	}
	if params.Limit == 0 {
		params.Limit = DefaultListLimit
	} else if params.Limit < 1 || params.Limit > MaxListLimit {
		errs = append(errs, model.FieldError{
			Field:   "limit",
			Message: "limit must be between 1 and 100",
			Code:    "out_of_range",
		})
	}
	if params.Cursor < 0 {
		errs = append(errs, model.FieldError{
			Field:   "cursor",
			Message: "cursor must be >= 0",
			Code:    "out_of_range",
		})
	}

	if len(errs) > 0 {
		return model.ListProjectsResponse{}, &model.ValidationError{
			Detail: "invalid query parameters",
			Errors: errs,
		}
	}

	return s.repo.ListProjects(ctx, params)
}

// CreateProject validates and creates a tracked project.
func (s *RegistryService) CreateProject(ctx context.Context, req model.CreateProjectRequest) (model.Project, error) {
	var errs []model.FieldError

	req.Slug = strings.TrimSpace(req.Slug)
	req.Name = strings.TrimSpace(req.Name)

	if !slugRegex.MatchString(req.Slug) {
		errs = append(errs, model.FieldError{
			Field:   "slug",
			Message: "slug must match ^[a-z0-9][a-z0-9-]{1,62}$",
			Code:    "invalid_format",
		})
	}
	if req.Name == "" {
		errs = append(errs, model.FieldError{
			Field:   "name",
			Message: "name is required",
			Code:    "required",
		})
	}
	if req.Priority != nil && (*req.Priority < 1 || *req.Priority > 5) {
		errs = append(errs, model.FieldError{
			Field:   "priority",
			Message: "priority must be between 1 and 5",
			Code:    "out_of_range",
		})
	}
	if req.Status != nil && !req.Status.IsValid() {
		errs = append(errs, model.FieldError{
			Field:   "status",
			Message: "status must be 'active' or 'paused'",
			Code:    "invalid_enum",
		})
	}

	if len(errs) > 0 {
		return model.Project{}, &model.ValidationError{
			Detail: "project validation failed",
			Errors: errs,
		}
	}

	req.Keywords = normalizeStringSlice(req.Keywords)
	req.Topics = normalizeStringSlice(req.Topics)

	return s.repo.CreateProject(ctx, req)
}

// GetProject returns a project with its repos and institutions.
func (s *RegistryService) GetProject(ctx context.Context, id int64) (model.ProjectDetail, error) {
	if id <= 0 {
		return model.ProjectDetail{}, invalidIDError("id")
	}
	return s.repo.GetProject(ctx, id)
}

// UpdateProject validates partial updates and applies them to a project.
func (s *RegistryService) UpdateProject(ctx context.Context, id int64, req model.UpdateProjectRequest) (model.Project, error) {
	if id <= 0 {
		return model.Project{}, invalidIDError("id")
	}

	var errs []model.FieldError
	if req.Slug != nil {
		trimmed := strings.TrimSpace(*req.Slug)
		req.Slug = &trimmed
		if !slugRegex.MatchString(trimmed) {
			errs = append(errs, model.FieldError{
				Field:   "slug",
				Message: "slug must match ^[a-z0-9][a-z0-9-]{1,62}$",
				Code:    "invalid_format",
			})
		}
	}
	if req.Name != nil {
		trimmed := strings.TrimSpace(*req.Name)
		req.Name = &trimmed
		if trimmed == "" {
			errs = append(errs, model.FieldError{
				Field:   "name",
				Message: "name cannot be empty",
				Code:    "required",
			})
		}
	}
	if req.Priority != nil && (*req.Priority < 1 || *req.Priority > 5) {
		errs = append(errs, model.FieldError{
			Field:   "priority",
			Message: "priority must be between 1 and 5",
			Code:    "out_of_range",
		})
	}
	if req.Status != nil && !req.Status.IsValid() {
		errs = append(errs, model.FieldError{
			Field:   "status",
			Message: "status must be 'active' or 'paused'",
			Code:    "invalid_enum",
		})
	}

	if len(errs) > 0 {
		return model.Project{}, &model.ValidationError{
			Detail: "project update validation failed",
			Errors: errs,
		}
	}

	if req.Keywords != nil {
		normalized := normalizeStringSlice(*req.Keywords)
		req.Keywords = &normalized
	}
	if req.Topics != nil {
		normalized := normalizeStringSlice(*req.Topics)
		req.Topics = &normalized
	}

	return s.repo.UpdateProject(ctx, id, req)
}

// DeleteProject deletes a project by id.
func (s *RegistryService) DeleteProject(ctx context.Context, id int64) error {
	if id <= 0 {
		return invalidIDError("id")
	}
	return s.repo.DeleteProject(ctx, id)
}

// ListProjectRepos returns repositories attached to a project.
func (s *RegistryService) ListProjectRepos(ctx context.Context, projectID int64) ([]model.Repo, error) {
	if projectID <= 0 {
		return nil, invalidIDError("id")
	}
	return s.repo.ListProjectRepos(ctx, projectID)
}

// CreateProjectRepo validates and attaches a GitHub repository to a project.
// Setting is_primary=true unsets any existing primary repository for the project in one transaction.
func (s *RegistryService) CreateProjectRepo(ctx context.Context, projectID int64, req model.CreateRepoRequest) (model.Repo, error) {
	if projectID <= 0 {
		return model.Repo{}, invalidIDError("id")
	}

	req.GitHubFullName = strings.TrimSpace(req.GitHubFullName)
	if !githubFullNameRegex.MatchString(req.GitHubFullName) {
		return model.Repo{}, &model.ValidationError{
			Detail: "repository validation failed",
			Errors: []model.FieldError{{
				Field:   "github_full_name",
				Message: "github_full_name must match ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$",
				Code:    "invalid_format",
			}},
		}
	}

	return s.repo.CreateProjectRepo(ctx, projectID, req)
}

// DeleteProjectRepo detaches a repository from a project.
func (s *RegistryService) DeleteProjectRepo(ctx context.Context, projectID, repoID int64) error {
	if projectID <= 0 {
		return invalidIDError("id")
	}
	if repoID <= 0 {
		return invalidIDError("repoId")
	}
	return s.repo.DeleteProjectRepo(ctx, projectID, repoID)
}

// ListInstitutions lists all institutions.
func (s *RegistryService) ListInstitutions(ctx context.Context) ([]model.Institution, error) {
	return s.repo.ListInstitutions(ctx)
}

// CreateInstitution validates and creates an institution.
func (s *RegistryService) CreateInstitution(ctx context.Context, req model.CreateInstitutionRequest) (model.Institution, error) {
	var errs []model.FieldError

	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		errs = append(errs, model.FieldError{
			Field:   "name",
			Message: "name is required",
			Code:    "required",
		})
	}
	if req.RORID != nil {
		trimmed := strings.TrimSpace(*req.RORID)
		if trimmed == "" {
			errs = append(errs, model.FieldError{
				Field:   "ror_id",
				Message: "ror_id cannot be empty when provided",
				Code:    "invalid_format",
			})
		} else {
			req.RORID = &trimmed
		}
	}
	if req.Country != nil {
		trimmed := strings.TrimSpace(*req.Country)
		if !countryRegex.MatchString(trimmed) {
			errs = append(errs, model.FieldError{
				Field:   "country",
				Message: "country must be a 2-letter ISO code",
				Code:    "invalid_format",
			})
		} else {
			upper := strings.ToUpper(trimmed)
			req.Country = &upper
		}
	}

	if len(errs) > 0 {
		return model.Institution{}, &model.ValidationError{
			Detail: "institution validation failed",
			Errors: errs,
		}
	}

	return s.repo.CreateInstitution(ctx, req)
}

// LinkProjectInstitution links an institution to a project.
func (s *RegistryService) LinkProjectInstitution(ctx context.Context, projectID, institutionID int64) error {
	if projectID <= 0 {
		return invalidIDError("id")
	}
	if institutionID <= 0 {
		return invalidIDError("institutionId")
	}
	return s.repo.LinkProjectInstitution(ctx, projectID, institutionID)
}

// UnlinkProjectInstitution removes an institution link from a project.
func (s *RegistryService) UnlinkProjectInstitution(ctx context.Context, projectID, institutionID int64) error {
	if projectID <= 0 {
		return invalidIDError("id")
	}
	if institutionID <= 0 {
		return invalidIDError("institutionId")
	}
	return s.repo.UnlinkProjectInstitution(ctx, projectID, institutionID)
}

func invalidIDError(field string) error {
	return &model.ValidationError{
		Detail: "invalid path parameter",
		Errors: []model.FieldError{{
			Field:   field,
			Message: field + " must be a positive integer",
			Code:    "invalid_id",
		}},
	}
}

func normalizeStringSlice(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(in))
	for _, v := range in {
		trimmed := strings.TrimSpace(v)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
