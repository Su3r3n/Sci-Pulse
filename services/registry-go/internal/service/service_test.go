package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/scipulse/scipulse/services/registry-go/internal/model"
)

type memRepo struct {
	mu           sync.Mutex
	nextProjID   int64
	nextRepoID   int64
	nextInstID   int64
	projects     map[int64]model.Project
	repos        map[int64]model.Repo
	institutions map[int64]model.Institution
	links        map[int64]map[int64]struct{}
}

func newMemRepo() *memRepo {
	return &memRepo{
		nextProjID:   1,
		nextRepoID:   1,
		nextInstID:   1,
		projects:     make(map[int64]model.Project),
		repos:        make(map[int64]model.Repo),
		institutions: make(map[int64]model.Institution),
		links:        make(map[int64]map[int64]struct{}),
	}
}

func (m *memRepo) Ping(context.Context) error { return nil }

func (m *memRepo) ListProjects(_ context.Context, params model.ListProjectsParams) (model.ListProjectsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var items []model.Project
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
	if items == nil {
		items = []model.Project{}
	}
	var next *int64
	if len(items) > params.Limit {
		items = items[:params.Limit]
		c := items[len(items)-1].ID
		next = &c
	}
	return model.ListProjectsResponse{Items: items, NextCursor: next}, nil
}

func (m *memRepo) CreateProject(_ context.Context, req model.CreateProjectRequest) (model.Project, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.projects {
		if p.Slug == req.Slug {
			return model.Project{}, &model.ConflictError{Field: "slug", Detail: "project with this slug already exists"}
		}
	}
	now := time.Now().UTC()
	priority := int16(3)
	if req.Priority != nil {
		priority = *req.Priority
	}
	status := model.ProjectStatusActive
	if req.Status != nil {
		status = *req.Status
	}
	p := model.Project{
		ID:          m.nextProjID,
		Slug:        req.Slug,
		Name:        req.Name,
		Description: req.Description,
		Keywords:    req.Keywords,
		Topics:      req.Topics,
		Priority:    priority,
		Status:      status,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	m.projects[p.ID] = p
	m.nextProjID++
	return p, nil
}

func (m *memRepo) GetProject(_ context.Context, id int64) (model.ProjectDetail, error) {
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

func (m *memRepo) UpdateProject(_ context.Context, id int64, req model.UpdateProjectRequest) (model.Project, error) {
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

func (m *memRepo) DeleteProject(_ context.Context, id int64) error {
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

func (m *memRepo) ListProjectRepos(_ context.Context, projectID int64) ([]model.Repo, error) {
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

func (m *memRepo) CreateProjectRepo(_ context.Context, projectID int64, req model.CreateRepoRequest) (model.Repo, error) {
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

func (m *memRepo) DeleteProjectRepo(_ context.Context, projectID, repoID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.repos[repoID]
	if !ok || r.ProjectID != projectID {
		return model.ErrNotFound
	}
	delete(m.repos, repoID)
	return nil
}

func (m *memRepo) ListInstitutions(_ context.Context) ([]model.Institution, error) {
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

func (m *memRepo) CreateInstitution(_ context.Context, req model.CreateInstitutionRequest) (model.Institution, error) {
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

func (m *memRepo) LinkProjectInstitution(_ context.Context, projectID, institutionID int64) error {
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

func (m *memRepo) UnlinkProjectInstitution(_ context.Context, projectID, institutionID int64) error {
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

func TestCreateProjectValidation(t *testing.T) {
	t.Parallel()
	svc := NewRegistryService(newMemRepo())
	ctx := context.Background()

	badPriority := int16(6)
	badStatus := model.ProjectStatus("archived")
	tests := []struct {
		name string
		req  model.CreateProjectRequest
	}{
		{
			name: "uppercase slug",
			req:  model.CreateProjectRequest{Slug: "Root-Project", Name: "ROOT"},
		},
		{
			name: "single char slug",
			req:  model.CreateProjectRequest{Slug: "a", Name: "A"},
		},
		{
			name: "slug starts with hyphen",
			req:  model.CreateProjectRequest{Slug: "-root", Name: "ROOT"},
		},
		{
			name: "empty name",
			req:  model.CreateProjectRequest{Slug: "root", Name: "   "},
		},
		{
			name: "priority out of range",
			req:  model.CreateProjectRequest{Slug: "root", Name: "ROOT", Priority: &badPriority},
		},
		{
			name: "invalid status",
			req:  model.CreateProjectRequest{Slug: "root", Name: "ROOT", Status: &badStatus},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := svc.CreateProject(ctx, tc.req)
			var ve *model.ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("expected ValidationError, got %v", err)
			}
		})
	}
}

func TestCreateProjectRepoPrimarySwitch(t *testing.T) {
	t.Parallel()
	svc := NewRegistryService(newMemRepo())
	ctx := context.Background()

	proj, err := svc.CreateProject(ctx, model.CreateProjectRequest{
		Slug: "root-project",
		Name: "ROOT",
	})
	if err != nil {
		t.Fatalf("CreateProject failed: %v", err)
	}

	r1, err := svc.CreateProjectRepo(ctx, proj.ID, model.CreateRepoRequest{
		GitHubFullName: "root-project/root",
		IsPrimary:      true,
	})
	if err != nil {
		t.Fatalf("CreateProjectRepo r1 failed: %v", err)
	}
	if !r1.IsPrimary {
		t.Fatalf("expected r1 to be primary")
	}

	r2, err := svc.CreateProjectRepo(ctx, proj.ID, model.CreateRepoRequest{
		GitHubFullName: "root-project/cling",
		IsPrimary:      true,
	})
	if err != nil {
		t.Fatalf("CreateProjectRepo r2 failed: %v", err)
	}
	if !r2.IsPrimary {
		t.Fatalf("expected r2 to be primary")
	}

	repos, err := svc.ListProjectRepos(ctx, proj.ID)
	if err != nil {
		t.Fatalf("ListProjectRepos failed: %v", err)
	}
	primaryCount := 0
	for _, r := range repos {
		if r.ID == r1.ID && r.IsPrimary {
			t.Fatalf("expected old primary r1 to be unset")
		}
		if r.IsPrimary {
			primaryCount++
		}
	}
	if primaryCount != 1 {
		t.Fatalf("expected exactly 1 primary repo, got %d", primaryCount)
	}
}

func TestCreateProjectRepoValidation(t *testing.T) {
	t.Parallel()
	svc := NewRegistryService(newMemRepo())
	ctx := context.Background()

	proj, err := svc.CreateProject(ctx, model.CreateProjectRequest{Slug: "rucio", Name: "Rucio"})
	if err != nil {
		t.Fatalf("CreateProject failed: %v", err)
	}

	for _, bad := range []string{"rucio", "https://github.com/rucio/rucio", "rucio/rucio/extra", "rucio/"} {
		_, err := svc.CreateProjectRepo(ctx, proj.ID, model.CreateRepoRequest{GitHubFullName: bad})
		var ve *model.ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("expected ValidationError for %q, got %v", bad, err)
		}
	}
}

func TestCreateInstitutionCountryNormalization(t *testing.T) {
	t.Parallel()
	svc := NewRegistryService(newMemRepo())
	ctx := context.Background()

	ch := "ch"
	inst, err := svc.CreateInstitution(ctx, model.CreateInstitutionRequest{
		Name:    "CERN",
		Country: &ch,
	})
	if err != nil {
		t.Fatalf("CreateInstitution failed: %v", err)
	}
	if inst.Country == nil || *inst.Country != "CH" {
		t.Fatalf("expected normalized uppercase country CH, got %v", inst.Country)
	}

	badCountry := "CHE"
	_, err = svc.CreateInstitution(ctx, model.CreateInstitutionRequest{
		Name:    "Invalid",
		Country: &badCountry,
	})
	var ve *model.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected ValidationError for country %q, got %v", badCountry, err)
	}
}
