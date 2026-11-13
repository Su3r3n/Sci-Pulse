package model

import (
	"encoding/json"
	"time"
)

// ProjectStatus represents the tracking state of a project.
type ProjectStatus string

const (
	ProjectStatusActive ProjectStatus = "active"
	ProjectStatusPaused ProjectStatus = "paused"
)

// IsValid reports whether s is a known project status.
func (s ProjectStatus) IsValid() bool {
	return s == ProjectStatusActive || s == ProjectStatusPaused
}

// OptionalString distinguishes between an omitted JSON field and an explicit null/string value.
type OptionalString struct {
	Set   bool
	Value *string
}

// UnmarshalJSON records that the field was explicitly present in JSON.
func (o *OptionalString) UnmarshalJSON(data []byte) error {
	o.Set = true
	if string(data) == "null" {
		o.Value = nil
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	o.Value = &s
	return nil
}

// Project corresponds to a row in tracked_projects.
type Project struct {
	ID          int64         `json:"id"`
	Slug        string        `json:"slug"`
	Name        string        `json:"name"`
	Description *string       `json:"description,omitempty"`
	Keywords    []string      `json:"keywords"`
	Topics      []string      `json:"topics"`
	Priority    int16         `json:"priority"`
	Status      ProjectStatus `json:"status"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
}

// ProjectDetail includes a project and its linked repositories and institutions.
type ProjectDetail struct {
	Project
	Repos        []Repo        `json:"repos"`
	Institutions []Institution `json:"institutions"`
}

// Repo corresponds to a row in project_repos.
type Repo struct {
	ID             int64     `json:"id"`
	ProjectID      int64     `json:"project_id"`
	GitHubFullName string    `json:"github_full_name"`
	IsPrimary      bool      `json:"is_primary"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// Institution corresponds to a row in institutions.
type Institution struct {
	ID        int64     `json:"id"`
	RORID     *string   `json:"ror_id,omitempty"`
	Name      string    `json:"name"`
	Country   *string   `json:"country,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// CreateProjectRequest is the payload for POST /api/v1/projects.
type CreateProjectRequest struct {
	Slug        string         `json:"slug"`
	Name        string         `json:"name"`
	Description *string        `json:"description,omitempty"`
	Keywords    []string       `json:"keywords,omitempty"`
	Topics      []string       `json:"topics,omitempty"`
	Priority    *int16         `json:"priority,omitempty"`
	Status      *ProjectStatus `json:"status,omitempty"`
}

// UpdateProjectRequest is the payload for PATCH /api/v1/projects/{id}.
type UpdateProjectRequest struct {
	Slug        *string        `json:"slug,omitempty"`
	Name        *string        `json:"name,omitempty"`
	Description OptionalString `json:"description,omitempty"`
	Keywords    *[]string      `json:"keywords,omitempty"`
	Topics      *[]string      `json:"topics,omitempty"`
	Priority    *int16         `json:"priority,omitempty"`
	Status      *ProjectStatus `json:"status,omitempty"`
}

// ListProjectsParams filters and paginates GET /api/v1/projects.
type ListProjectsParams struct {
	Status *ProjectStatus
	Limit  int
	Cursor int64
}

// ListProjectsResponse is returned by GET /api/v1/projects.
type ListProjectsResponse struct {
	Items      []Project `json:"items"`
	NextCursor *int64    `json:"next_cursor,omitempty"`
}

// CreateRepoRequest is the payload for POST /api/v1/projects/{id}/repos.
type CreateRepoRequest struct {
	GitHubFullName string `json:"github_full_name"`
	IsPrimary      bool   `json:"is_primary"`
}

// CreateInstitutionRequest is the payload for POST /api/v1/institutions.
type CreateInstitutionRequest struct {
	RORID   *string `json:"ror_id,omitempty"`
	Name    string  `json:"name"`
	Country *string `json:"country,omitempty"`
}
