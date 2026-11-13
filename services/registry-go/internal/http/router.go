package http

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/scipulse/scipulse/services/common-go/obs"
	"github.com/scipulse/scipulse/services/registry-go/internal/service"
)

// RouterOptions configures the HTTP router.
type RouterOptions struct {
	Service        *service.RegistryService
	APIKey         string
	Logger         *slog.Logger
	PromRegistry   *prometheus.Registry
	OpenAPIYAML    []byte
	RequestTimeout time.Duration
	HealthChecks   map[string]obs.HealthCheck
}

// NewRouter constructs the chi router with observability, auth, and API endpoints.
func NewRouter(opts RouterOptions) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(RequestIDHeader)
	r.Use(ProblemRecoverer(opts.Logger))
	r.Use(SlogLogger(opts.Logger))

	metrics := NewMetrics(opts.PromRegistry)
	r.Use(MetricsMiddleware(metrics))

	checks := opts.HealthChecks
	if checks == nil && opts.Service != nil {
		checks = map[string]obs.HealthCheck{
			"postgres": func(ctx context.Context) error {
				return opts.Service.Ping(ctx)
			},
		}
	}

	h := NewHandler(opts.Service, opts.Logger, opts.OpenAPIYAML)

	r.Get("/healthz", obs.HealthHandler(checks).ServeHTTP)
	r.Get("/metrics", obs.MetricsHandler(opts.PromRegistry).ServeHTTP)
	r.Get("/docs", h.ServeDocs)
	r.Get("/openapi.yaml", h.ServeOpenAPI)

	timeout := opts.RequestTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	r.Route("/api/v1", func(api chi.Router) {
		api.Use(middleware.Timeout(timeout))

		// Public read routes
		api.Get("/projects", h.ListProjects)
		api.Get("/projects/{id}", h.GetProject)
		api.Get("/projects/{id}/repos", h.ListProjectRepos)
		api.Get("/institutions", h.ListInstitutions)

		// Authenticated write routes
		api.Group(func(protected chi.Router) {
			protected.Use(APIKeyAuth(opts.APIKey))

			protected.Post("/projects", h.CreateProject)
			protected.Patch("/projects/{id}", h.UpdateProject)
			protected.Delete("/projects/{id}", h.DeleteProject)

			protected.Post("/projects/{id}/repos", h.CreateProjectRepo)
			protected.Delete("/projects/{id}/repos/{repoId}", h.DeleteProjectRepo)

			protected.Post("/institutions", h.CreateInstitution)
			protected.Put("/projects/{id}/institutions/{institutionId}", h.LinkProjectInstitution)
			protected.Delete("/projects/{id}/institutions/{institutionId}", h.UnlinkProjectInstitution)
		})
	})

	r.NotFound(func(w http.ResponseWriter, req *http.Request) {
		writeProblem(w, req, http.StatusNotFound, "https://scipulse.dev/problems/not-found", "Not Found", "endpoint not found", nil)
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, req *http.Request) {
		writeProblem(w, req, http.StatusMethodNotAllowed, "https://scipulse.dev/problems/method-not-allowed", "Method Not Allowed", "HTTP method is not allowed on this endpoint", nil)
	})

	return r
}
