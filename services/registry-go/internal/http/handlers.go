package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/scipulse/scipulse/services/registry-go/internal/model"
	"github.com/scipulse/scipulse/services/registry-go/internal/service"
)

const maxRequestBodyBytes = 1 << 20 // 1 MiB

// Handler implements HTTP endpoints for the registry API.
type Handler struct {
	svc         *service.RegistryService
	logger      *slog.Logger
	openapiYAML []byte
}

// NewHandler creates a Handler.
func NewHandler(svc *service.RegistryService, logger *slog.Logger, openapiYAML []byte) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		svc:         svc,
		logger:      logger,
		openapiYAML: openapiYAML,
	}
}

// ServeDocs renders Swagger UI pointing at /openapi.yaml.
func (h *Handler) ServeDocs(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(swaggerUIHTML))
}

// ServeOpenAPI serves the embedded openapi.yaml specification.
func (h *Handler) ServeOpenAPI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(h.openapiYAML)
}

// ListProjects handles GET /api/v1/projects.
func (h *Handler) ListProjects(w http.ResponseWriter, r *http.Request) {
	var params model.ListProjectsParams
	var errs []model.FieldError

	q := r.URL.Query()
	if rawStatus := strings.TrimSpace(q.Get("status")); rawStatus != "" {
		st := model.ProjectStatus(rawStatus)
		if !st.IsValid() {
			errs = append(errs, model.FieldError{
				Field:   "status",
				Message: "status must be 'active' or 'paused'",
				Code:    "invalid_enum",
			})
		} else {
			params.Status = &st
		}
	}

	if rawLimit := strings.TrimSpace(q.Get("limit")); rawLimit != "" {
		n, err := strconv.Atoi(rawLimit)
		if err != nil || n < 1 || n > service.MaxListLimit {
			errs = append(errs, model.FieldError{
				Field:   "limit",
				Message: "limit must be an integer between 1 and 100",
				Code:    "out_of_range",
			})
		} else {
			params.Limit = n
		}
	} else {
		params.Limit = service.DefaultListLimit
	}

	if rawCursor := strings.TrimSpace(q.Get("cursor")); rawCursor != "" {
		c, err := strconv.ParseInt(rawCursor, 10, 64)
		if err != nil || c < 0 {
			errs = append(errs, model.FieldError{
				Field:   "cursor",
				Message: "cursor must be a non-negative integer",
				Code:    "invalid_format",
			})
		} else {
			params.Cursor = c
		}
	}

	if len(errs) > 0 {
		writeProblem(w, r, http.StatusBadRequest, "https://scipulse.dev/problems/validation-error", "Bad Request", "invalid query parameters", errs)
		return
	}

	resp, err := h.svc.ListProjects(r.Context(), params)
	if err != nil {
		h.handleServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// CreateProject handles POST /api/v1/projects.
func (h *Handler) CreateProject(w http.ResponseWriter, r *http.Request) {
	var req model.CreateProjectRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}

	proj, err := h.svc.CreateProject(r.Context(), req)
	if err != nil {
		h.handleServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, proj)
}

// GetProject handles GET /api/v1/projects/{id}.
func (h *Handler) GetProject(w http.ResponseWriter, r *http.Request) {
	id, ok := parseIDParam(w, r, "id")
	if !ok {
		return
	}

	detail, err := h.svc.GetProject(r.Context(), id)
	if err != nil {
		h.handleServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

// UpdateProject handles PATCH /api/v1/projects/{id}.
func (h *Handler) UpdateProject(w http.ResponseWriter, r *http.Request) {
	id, ok := parseIDParam(w, r, "id")
	if !ok {
		return
	}

	var req model.UpdateProjectRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}

	proj, err := h.svc.UpdateProject(r.Context(), id, req)
	if err != nil {
		h.handleServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, proj)
}

// DeleteProject handles DELETE /api/v1/projects/{id}.
func (h *Handler) DeleteProject(w http.ResponseWriter, r *http.Request) {
	id, ok := parseIDParam(w, r, "id")
	if !ok {
		return
	}

	if err := h.svc.DeleteProject(r.Context(), id); err != nil {
		h.handleServiceError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ListProjectRepos handles GET /api/v1/projects/{id}/repos.
func (h *Handler) ListProjectRepos(w http.ResponseWriter, r *http.Request) {
	id, ok := parseIDParam(w, r, "id")
	if !ok {
		return
	}

	repos, err := h.svc.ListProjectRepos(r.Context(), id)
	if err != nil {
		h.handleServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, repos)
}

// CreateProjectRepo handles POST /api/v1/projects/{id}/repos.
func (h *Handler) CreateProjectRepo(w http.ResponseWriter, r *http.Request) {
	id, ok := parseIDParam(w, r, "id")
	if !ok {
		return
	}

	var req model.CreateRepoRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}

	repo, err := h.svc.CreateProjectRepo(r.Context(), id, req)
	if err != nil {
		h.handleServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, repo)
}

// DeleteProjectRepo handles DELETE /api/v1/projects/{id}/repos/{repoId}.
func (h *Handler) DeleteProjectRepo(w http.ResponseWriter, r *http.Request) {
	projectID, ok := parseIDParam(w, r, "id")
	if !ok {
		return
	}
	repoID, ok := parseIDParam(w, r, "repoId")
	if !ok {
		return
	}

	if err := h.svc.DeleteProjectRepo(r.Context(), projectID, repoID); err != nil {
		h.handleServiceError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ListInstitutions handles GET /api/v1/institutions.
func (h *Handler) ListInstitutions(w http.ResponseWriter, r *http.Request) {
	insts, err := h.svc.ListInstitutions(r.Context())
	if err != nil {
		h.handleServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, insts)
}

// CreateInstitution handles POST /api/v1/institutions.
func (h *Handler) CreateInstitution(w http.ResponseWriter, r *http.Request) {
	var req model.CreateInstitutionRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}

	inst, err := h.svc.CreateInstitution(r.Context(), req)
	if err != nil {
		h.handleServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, inst)
}

// LinkProjectInstitution handles PUT /api/v1/projects/{id}/institutions/{institutionId}.
func (h *Handler) LinkProjectInstitution(w http.ResponseWriter, r *http.Request) {
	projectID, ok := parseIDParam(w, r, "id")
	if !ok {
		return
	}
	institutionID, ok := parseIDParam(w, r, "institutionId")
	if !ok {
		return
	}

	if err := h.svc.LinkProjectInstitution(r.Context(), projectID, institutionID); err != nil {
		h.handleServiceError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// UnlinkProjectInstitution handles DELETE /api/v1/projects/{id}/institutions/{institutionId}.
func (h *Handler) UnlinkProjectInstitution(w http.ResponseWriter, r *http.Request) {
	projectID, ok := parseIDParam(w, r, "id")
	if !ok {
		return
	}
	institutionID, ok := parseIDParam(w, r, "institutionId")
	if !ok {
		return
	}

	if err := h.svc.UnlinkProjectInstitution(r.Context(), projectID, institutionID); err != nil {
		h.handleServiceError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleServiceError(w http.ResponseWriter, r *http.Request, err error) {
	var valErr *model.ValidationError
	if errors.As(err, &valErr) {
		writeProblem(w, r, http.StatusBadRequest, "https://scipulse.dev/problems/validation-error", "Bad Request", valErr.Detail, valErr.Errors)
		return
	}

	var conflictErr *model.ConflictError
	if errors.As(err, &conflictErr) {
		var fieldErrs []model.FieldError
		if conflictErr.Field != "" || conflictErr.Detail != "" {
			fieldErrs = []model.FieldError{{
				Field:   conflictErr.Field,
				Message: conflictErr.Error(),
				Code:    "conflict",
			}}
		}
		writeProblem(w, r, http.StatusConflict, "https://scipulse.dev/problems/conflict", "Conflict", conflictErr.Error(), fieldErrs)
		return
	}
	if errors.Is(err, model.ErrConflict) {
		writeProblem(w, r, http.StatusConflict, "https://scipulse.dev/problems/conflict", "Conflict", err.Error(), nil)
		return
	}

	if errors.Is(err, model.ErrNotFound) {
		writeProblem(w, r, http.StatusNotFound, "https://scipulse.dev/problems/not-found", "Not Found", "requested resource was not found", nil)
		return
	}

	if errors.Is(err, context.DeadlineExceeded) {
		writeProblem(w, r, http.StatusGatewayTimeout, "https://scipulse.dev/problems/timeout", "Gateway Timeout", "request timed out", nil)
		return
	}

	h.logger.Error("internal handler error", "method", r.Method, "path", r.URL.Path, "error", err)
	writeProblem(w, r, http.StatusInternalServerError, "about:blank", "Internal Server Error", "an unexpected error occurred", nil)
}

func parseIDParam(w http.ResponseWriter, r *http.Request, name string) (int64, bool) {
	raw := chi.URLParam(r, name)
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		writeProblem(w, r, http.StatusBadRequest, "https://scipulse.dev/problems/validation-error", "Bad Request", "invalid path parameter", []model.FieldError{{
			Field:   name,
			Message: name + " must be a positive integer",
			Code:    "invalid_id",
		}})
		return 0, false
	}
	return id, true
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	if r.Body == nil {
		writeProblem(w, r, http.StatusBadRequest, "https://scipulse.dev/problems/invalid-json", "Bad Request", "request body is required", []model.FieldError{{
			Message: "request body is required",
			Code:    "empty_body",
		}})
		return false
	}

	dec := json.NewDecoder(io.LimitReader(r.Body, maxRequestBodyBytes))
	if err := dec.Decode(dst); err != nil {
		writeProblem(w, r, http.StatusBadRequest, "https://scipulse.dev/problems/invalid-json", "Bad Request", "invalid JSON payload", []model.FieldError{{
			Message: err.Error(),
			Code:    "invalid_json",
		}})
		return false
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeProblem(w, r, http.StatusBadRequest, "https://scipulse.dev/problems/invalid-json", "Bad Request", "request body must contain a single JSON object", []model.FieldError{{
			Message: "unexpected trailing data after JSON payload",
			Code:    "trailing_data",
		}})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeProblem(w http.ResponseWriter, r *http.Request, status int, problemType, title, detail string, errs []model.FieldError) {
	instance := ""
	if r != nil && r.URL != nil {
		instance = r.URL.Path
	}
	problem := model.NewProblemDetail(status, problemType, title, detail, instance, errs)
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problem)
}

const swaggerUIHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>SciPulse Registry API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = function () {
      window.ui = SwaggerUIBundle({
        url: "/openapi.yaml",
        dom_id: "#swagger-ui"
      });
    };
  </script>
</body>
</html>
`
