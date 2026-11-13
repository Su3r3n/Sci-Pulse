package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/scipulse/scipulse/services/common-go/obs"
	reghttp "github.com/scipulse/scipulse/services/registry-go/internal/http"
	"github.com/scipulse/scipulse/services/registry-go/internal/model"
	"github.com/scipulse/scipulse/services/registry-go/internal/service"
)

const testAPIKey = "super-secret-test-key"

type mockRepo struct {
	mu           sync.Mutex
	nextProjID   int64
	nextRepoID   int64
	nextInstID   int64
	projects     map[int64]model.Project
	repos        map[int64]model.Repo
	institutions map[int64]model.Institution
	links        map[int64]map[int64]struct{}
	pingErr      error
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		nextProjID:   1,
		nextRepoID:   1,
		nextInstID:   1,
		projects:     make(map[int64]model.Project),
		repos:        make(map[int64]model.Repo),
		institutions: make(map[int64]model.Institution),
		links:        make(map[int64]map[int64]struct{}),
	}
}

func (m *mockRepo) Ping(context.Context) error { return m.pingErr }

func (m *mockRepo) ListProjects(_ context.Context, params model.ListProjectsParams) (model.ListProjectsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := []model.Project{}
	for id := int64(1); id < m.nextProjID; id++ {
		p, ok := m.projects[id]
		if !ok || p.ID <= params.Cursor {
			continue
		}
		if params.Status != nil && p.Status != *params.Status {
			continue
		}
		items = append(items, p)
	}
	var next *int64
	if len(items) > params.Limit {
		items = items[:params.Limit]
		c := items[len(items)-1].ID
		next = &c
	}
	return model.ListProjectsResponse{Items: items, NextCursor: next}, nil
}

func (m *mockRepo) CreateProject(_ context.Context, req model.CreateProjectRequest) (model.Project, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.projects {
		if p.Slug == req.Slug {
			return model.Project{}, &model.ConflictError{Field: "slug", Detail: "project with this slug already exists"}
		}
	}
	now := time.Now().UTC()
	prio := int16(3)
	if req.Priority != nil {
		prio = *req.Priority
	}
	st := model.ProjectStatusActive
	if req.Status != nil {
		st = *req.Status
	}
	p := model.Project{
		ID:          m.nextProjID,
		Slug:        req.Slug,
		Name:        req.Name,
		Description: req.Description,
		Keywords:    req.Keywords,
		Topics:      req.Topics,
		Priority:    prio,
		Status:      st,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	m.projects[p.ID] = p
	m.nextProjID++
	return p, nil
}

func (m *mockRepo) GetProject(_ context.Context, id int64) (model.ProjectDetail, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.projects[id]
	if !ok {
		return model.ProjectDetail{}, model.ErrNotFound
	}
	repos := []model.Repo{}
	for _, r := range m.repos {
		if r.ProjectID == id {
			repos = append(repos, r)
		}
	}
	insts := []model.Institution{}
	for instID := range m.links[id] {
		if inst, ok := m.institutions[instID]; ok {
			insts = append(insts, inst)
		}
	}
	return model.ProjectDetail{Project: p, Repos: repos, Institutions: insts}, nil
}

func (m *mockRepo) UpdateProject(_ context.Context, id int64, req model.UpdateProjectRequest) (model.Project, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.projects[id]
	if !ok {
		return model.Project{}, model.ErrNotFound
	}
	if req.Slug != nil {
		for otherID, other := range m.projects {
			if otherID != id && other.Slug == *req.Slug {
				return model.Project{}, &model.ConflictError{Field: "slug", Detail: "project with this slug already exists"}
			}
		}
		p.Slug = *req.Slug
	}
	if req.Name != nil {
		p.Name = *req.Name
	}
	if req.Description.Set {
		p.Description = req.Description.Value
	}
	if req.Keywords != nil {
		p.Keywords = *req.Keywords
	}
	if req.Topics != nil {
		p.Topics = *req.Topics
	}
	if req.Priority != nil {
		p.Priority = *req.Priority
	}
	if req.Status != nil {
		p.Status = *req.Status
	}
	p.UpdatedAt = time.Now().UTC()
	m.projects[id] = p
	return p, nil
}

func (m *mockRepo) DeleteProject(_ context.Context, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.projects[id]; !ok {
		return model.ErrNotFound
	}
	delete(m.projects, id)
	for rid, r := range m.repos {
		if r.ProjectID == id {
			delete(m.repos, rid)
		}
	}
	delete(m.links, id)
	return nil
}

func (m *mockRepo) ListProjectRepos(_ context.Context, projectID int64) ([]model.Repo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.projects[projectID]; !ok {
		return nil, model.ErrNotFound
	}
	out := []model.Repo{}
	for _, r := range m.repos {
		if r.ProjectID == projectID {
			out = append(out, r)
		}
	}
	return out, nil
}

func (m *mockRepo) CreateProjectRepo(_ context.Context, projectID int64, req model.CreateRepoRequest) (model.Repo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.projects[projectID]; !ok {
		return model.Repo{}, model.ErrNotFound
	}
	for _, r := range m.repos {
		if r.GitHubFullName == req.GitHubFullName {
			return model.Repo{}, &model.ConflictError{Field: "github_full_name", Detail: "repository is already attached to a project"}
		}
	}
	if req.IsPrimary {
		for rid, r := range m.repos {
			if r.ProjectID == projectID && r.IsPrimary {
				r.IsPrimary = false
				m.repos[rid] = r
			}
		}
	}
	now := time.Now().UTC()
	repo := model.Repo{
		ID:             m.nextRepoID,
		ProjectID:      projectID,
		GitHubFullName: req.GitHubFullName,
		IsPrimary:      req.IsPrimary,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	m.repos[repo.ID] = repo
	m.nextRepoID++
	return repo, nil
}

func (m *mockRepo) DeleteProjectRepo(_ context.Context, projectID, repoID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.repos[repoID]
	if !ok || r.ProjectID != projectID {
		return model.ErrNotFound
	}
	delete(m.repos, repoID)
	return nil
}

func (m *mockRepo) ListInstitutions(_ context.Context) ([]model.Institution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]model.Institution, 0, len(m.institutions))
	for id := int64(1); id < m.nextInstID; id++ {
		if inst, ok := m.institutions[id]; ok {
			out = append(out, inst)
		}
	}
	return out, nil
}

func (m *mockRepo) CreateInstitution(_ context.Context, req model.CreateInstitutionRequest) (model.Institution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.RORID != nil {
		for _, inst := range m.institutions {
			if inst.RORID != nil && *inst.RORID == *req.RORID {
				return model.Institution{}, &model.ConflictError{Field: "ror_id", Detail: "institution with this ror_id already exists"}
			}
		}
	}
	now := time.Now().UTC()
	inst := model.Institution{
		ID:        m.nextInstID,
		RORID:     req.RORID,
		Name:      req.Name,
		Country:   req.Country,
		CreatedAt: now,
		UpdatedAt: now,
	}
	m.institutions[inst.ID] = inst
	m.nextInstID++
	return inst, nil
}

func (m *mockRepo) LinkProjectInstitution(_ context.Context, projectID, institutionID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.projects[projectID]; !ok {
		return model.ErrNotFound
	}
	if _, ok := m.institutions[institutionID]; !ok {
		return model.ErrNotFound
	}
	if m.links[projectID] == nil {
		m.links[projectID] = make(map[int64]struct{})
	}
	m.links[projectID][institutionID] = struct{}{}
	return nil
}

func (m *mockRepo) UnlinkProjectInstitution(_ context.Context, projectID, institutionID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.links[projectID] == nil {
		return model.ErrNotFound
	}
	if _, ok := m.links[projectID][institutionID]; !ok {
		return model.ErrNotFound
	}
	delete(m.links[projectID], institutionID)
	return nil
}

func newTestRouter(repo *mockRepo) http.Handler {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	promReg := obs.NewRegistry()
	svc := service.NewRegistryService(repo)
	return reghttp.NewRouter(reghttp.RouterOptions{
		Service:      svc,
		APIKey:       testAPIKey,
		Logger:       logger,
		PromRegistry: promReg,
		OpenAPIYAML:  []byte("openapi: 3.0.3\ninfo:\n  title: Test\n  version: 1.0.0\n"),
	})
}

func doReq(t *testing.T, h http.Handler, method, path string, body any, apiKey string) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		switch v := body.(type) {
		case string:
			buf.WriteString(v)
		default:
			if err := json.NewEncoder(&buf).Encode(v); err != nil {
				t.Fatalf("encode request body: %v", err)
			}
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if apiKey != "" {
		req.Header.Set("X-API-Key", apiKey)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func assertProblemJSON(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int) model.ProblemDetail {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("expected status %d, got %d (body: %s)", wantStatus, rec.Code, rec.Body.String())
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "application/problem+json") {
		t.Fatalf("expected Content-Type application/problem+json, got %q", ct)
	}
	var p model.ProblemDetail
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode problem+json: %v (body: %s)", err, rec.Body.String())
	}
	if p.Status != wantStatus {
		t.Fatalf("problem.status=%d, want %d", p.Status, wantStatus)
	}
	if p.Errors == nil {
		t.Fatalf("expected non-nil errors[] array in problem+json")
	}
	return p
}

func TestObservabilityAndDocsEndpoints(t *testing.T) {
	t.Parallel()
	repo := newMockRepo()
	h := newTestRouter(repo)

	rec := doReq(t, h, http.MethodGet, "/healthz", nil, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /healthz, got %d", rec.Code)
	}

	repo.pingErr = errors.New("db down")
	recUnhealthy := doReq(t, h, http.MethodGet, "/healthz", nil, "")
	if recUnhealthy.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on unhealthy /healthz, got %d", recUnhealthy.Code)
	}
	repo.pingErr = nil

	recDocs := doReq(t, h, http.MethodGet, "/docs", nil, "")
	if recDocs.Code != http.StatusOK || !strings.Contains(recDocs.Body.String(), "/openapi.yaml") {
		t.Fatalf("expected Swagger UI HTML on /docs, got status=%d", recDocs.Code)
	}

	recSpec := doReq(t, h, http.MethodGet, "/openapi.yaml", nil, "")
	if recSpec.Code != http.StatusOK || !strings.Contains(recSpec.Body.String(), "openapi: 3.0.3") {
		t.Fatalf("expected openapi.yaml on /openapi.yaml, got status=%d", recSpec.Code)
	}

	// Trigger an API request so Prometheus HTTP metrics are emitted.
	_ = doReq(t, h, http.MethodGet, "/api/v1/projects", nil, "")
	recMetrics := doReq(t, h, http.MethodGet, "/metrics", nil, "")
	if recMetrics.Code != http.StatusOK {
		t.Fatalf("expected 200 on /metrics, got %d", recMetrics.Code)
	}
	body := recMetrics.Body.String()
	if !strings.Contains(body, "http_requests_total") || !strings.Contains(body, "http_request_duration_seconds") {
		t.Fatalf("expected http_requests_total and http_request_duration_seconds in /metrics, got:\n%s", body)
	}
}

func TestAuthRequiredOnWriteRoutes(t *testing.T) {
	t.Parallel()
	h := newTestRouter(newMockRepo())

	writeEndpoints := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/api/v1/projects", `{"slug":"root","name":"ROOT"}`},
		{http.MethodPatch, "/api/v1/projects/1", `{"priority":1}`},
		{http.MethodDelete, "/api/v1/projects/1", ""},
		{http.MethodPost, "/api/v1/projects/1/repos", `{"github_full_name":"root-project/root"}`},
		{http.MethodDelete, "/api/v1/projects/1/repos/1", ""},
		{http.MethodPost, "/api/v1/institutions", `{"name":"CERN"}`},
		{http.MethodPut, "/api/v1/projects/1/institutions/1", ""},
		{http.MethodDelete, "/api/v1/projects/1/institutions/1", ""},
	}

	for _, ep := range writeEndpoints {
		recMissing := doReq(t, h, ep.method, ep.path, ep.body, "")
		assertProblemJSON(t, recMissing, http.StatusUnauthorized)

		recWrong := doReq(t, h, ep.method, ep.path, ep.body, "wrong-key")
		assertProblemJSON(t, recWrong, http.StatusUnauthorized)
	}
}

func TestProjectsReposInstitutionsLifecycle(t *testing.T) {
	t.Parallel()
	h := newTestRouter(newMockRepo())

	// 1. Create project -> 201
	rec := doReq(t, h, http.MethodPost, "/api/v1/projects", map[string]any{
		"slug":     "root-project",
		"name":     "ROOT",
		"keywords": []string{"hep", "data-analysis"},
		"priority": 1,
	}, testAPIKey)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", rec.Code, rec.Body.String())
	}
	var proj model.Project
	if err := json.Unmarshal(rec.Body.Bytes(), &proj); err != nil {
		t.Fatalf("unmarshal project: %v", err)
	}
	if proj.ID != 1 || proj.Slug != "root-project" || proj.Priority != 1 {
		t.Fatalf("unexpected created project: %+v", proj)
	}

	// 2. Duplicate slug -> 409 problem+json
	recDup := doReq(t, h, http.MethodPost, "/api/v1/projects", map[string]any{
		"slug": "root-project",
		"name": "ROOT Duplicate",
	}, testAPIKey)
	p409 := assertProblemJSON(t, recDup, http.StatusConflict)
	if len(p409.Errors) == 0 || p409.Errors[0].Field != "slug" {
		t.Fatalf("expected slug error in 409 response, got %+v", p409)
	}

	// 3. Invalid payload -> 400 problem+json
	recBad := doReq(t, h, http.MethodPost, "/api/v1/projects", map[string]any{
		"slug":     "BAD_SLUG",
		"name":     "",
		"priority": 9,
	}, testAPIKey)
	p400 := assertProblemJSON(t, recBad, http.StatusBadRequest)
	if len(p400.Errors) < 3 {
		t.Fatalf("expected 3 validation errors, got %+v", p400.Errors)
	}

	// 4. Malformed JSON -> 400 problem+json
	recMalformed := doReq(t, h, http.MethodPost, "/api/v1/projects", `{"slug":`, testAPIKey)
	assertProblemJSON(t, recMalformed, http.StatusBadRequest)

	// 5. List projects with filters & invalid query -> 200 / 400
	recList := doReq(t, h, http.MethodGet, "/api/v1/projects?status=active&limit=10&cursor=0", nil, "")
	if recList.Code != http.StatusOK {
		t.Fatalf("expected 200 on list, got %d", recList.Code)
	}
	recBadQuery := doReq(t, h, http.MethodGet, "/api/v1/projects?limit=200&status=unknown", nil, "")
	assertProblemJSON(t, recBadQuery, http.StatusBadRequest)

	// 6. Attach repos (including primary switch) -> 201 / 409 / 200
	recR1 := doReq(t, h, http.MethodPost, "/api/v1/projects/1/repos", map[string]any{
		"github_full_name": "root-project/root",
		"is_primary":       true,
	}, testAPIKey)
	if recR1.Code != http.StatusCreated {
		t.Fatalf("expected 201 creating repo, got %d (%s)", recR1.Code, recR1.Body.String())
	}
	recR2 := doReq(t, h, http.MethodPost, "/api/v1/projects/1/repos", map[string]any{
		"github_full_name": "root-project/cling",
		"is_primary":       true,
	}, testAPIKey)
	if recR2.Code != http.StatusCreated {
		t.Fatalf("expected 201 creating second primary repo, got %d", recR2.Code)
	}
	recRDup := doReq(t, h, http.MethodPost, "/api/v1/projects/1/repos", map[string]any{
		"github_full_name": "root-project/root",
	}, testAPIKey)
	assertProblemJSON(t, recRDup, http.StatusConflict)

	// 7. Create institution & link to project -> 201 / 204 / 200 detail
	recInst := doReq(t, h, http.MethodPost, "/api/v1/institutions", map[string]any{
		"ror_id":  "https://ror.org/01ggx4157",
		"name":    "CERN",
		"country": "ch",
	}, testAPIKey)
	if recInst.Code != http.StatusCreated {
		t.Fatalf("expected 201 creating institution, got %d (%s)", recInst.Code, recInst.Body.String())
	}
	recInstDup := doReq(t, h, http.MethodPost, "/api/v1/institutions", map[string]any{
		"ror_id": "https://ror.org/01ggx4157",
		"name":   "CERN Dup",
	}, testAPIKey)
	assertProblemJSON(t, recInstDup, http.StatusConflict)

	recLink := doReq(t, h, http.MethodPut, "/api/v1/projects/1/institutions/1", nil, testAPIKey)
	if recLink.Code != http.StatusNoContent {
		t.Fatalf("expected 204 linking institution, got %d", recLink.Code)
	}

	// 8. Get project detail -> 200 with repos & institutions
	recDetail := doReq(t, h, http.MethodGet, "/api/v1/projects/1", nil, "")
	if recDetail.Code != http.StatusOK {
		t.Fatalf("expected 200 on get project, got %d", recDetail.Code)
	}
	var detail model.ProjectDetail
	if err := json.Unmarshal(recDetail.Body.Bytes(), &detail); err != nil {
		t.Fatalf("unmarshal detail: %v", err)
	}
	if len(detail.Repos) != 2 || len(detail.Institutions) != 1 {
		t.Fatalf("expected 2 repos and 1 institution, got repos=%d insts=%d", len(detail.Repos), len(detail.Institutions))
	}

	// 9. Patch project -> 200
	recPatch := doReq(t, h, http.MethodPatch, "/api/v1/projects/1", map[string]any{
		"status":   "paused",
		"priority": 5,
	}, testAPIKey)
	if recPatch.Code != http.StatusOK {
		t.Fatalf("expected 200 on patch, got %d (%s)", recPatch.Code, recPatch.Body.String())
	}

	// 10. Unlink institution & delete repo -> 204, then 404 on repeat
	recUnlink := doReq(t, h, http.MethodDelete, "/api/v1/projects/1/institutions/1", nil, testAPIKey)
	if recUnlink.Code != http.StatusNoContent {
		t.Fatalf("expected 204 on unlink, got %d", recUnlink.Code)
	}
	recUnlinkAgain := doReq(t, h, http.MethodDelete, "/api/v1/projects/1/institutions/1", nil, testAPIKey)
	assertProblemJSON(t, recUnlinkAgain, http.StatusNotFound)

	recDelRepo := doReq(t, h, http.MethodDelete, "/api/v1/projects/1/repos/1", nil, testAPIKey)
	if recDelRepo.Code != http.StatusNoContent {
		t.Fatalf("expected 204 on delete repo, got %d", recDelRepo.Code)
	}
	recDelRepoAgain := doReq(t, h, http.MethodDelete, "/api/v1/projects/1/repos/1", nil, testAPIKey)
	assertProblemJSON(t, recDelRepoAgain, http.StatusNotFound)

	// 11. Delete project -> 204, then GET -> 404
	recDelProj := doReq(t, h, http.MethodDelete, "/api/v1/projects/1", nil, testAPIKey)
	if recDelProj.Code != http.StatusNoContent {
		t.Fatalf("expected 204 on delete project, got %d", recDelProj.Code)
	}
	recGetMissing := doReq(t, h, http.MethodGet, "/api/v1/projects/1", nil, "")
	assertProblemJSON(t, recGetMissing, http.StatusNotFound)
}
