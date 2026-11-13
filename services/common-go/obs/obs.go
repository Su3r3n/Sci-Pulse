package obs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ParseLogLevel converts a string level ("debug", "info", "warn", "error") into slog.Level.
func ParseLogLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// NewLogger creates a structured JSON slog.Logger writing to stdout with a "service" field.
func NewLogger(service string, level slog.Level) *slog.Logger {
	return NewLoggerWithWriter(os.Stdout, service, level)
}

// NewLoggerWithWriter creates a structured JSON slog.Logger writing to w.
func NewLoggerWithWriter(w io.Writer, service string, level slog.Level) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
	})
	logger := slog.New(handler)
	if service != "" {
		logger = logger.With("service", service)
	}
	return logger
}

// NewRegistry creates an isolated Prometheus registry with standard Go and process collectors.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

// HealthCheck validates the readiness of a single dependency.
type HealthCheck func(ctx context.Context) error

type healthResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

// HealthHandler returns an HTTP handler for /healthz.
// When checks is empty, it returns 200 {"status":"ok"}.
// When all checks pass, it returns 200; if any check fails, it returns 503.
func HealthHandler(checks map[string]HealthCheck) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := healthResponse{
			Status: "ok",
		}
		code := http.StatusOK

		if len(checks) > 0 {
			resp.Checks = make(map[string]string, len(checks))
			names := make([]string, 0, len(checks))
			for name := range checks {
				names = append(names, name)
			}
			sort.Strings(names)

			for _, name := range names {
				check := checks[name]
				if check == nil {
					resp.Checks[name] = "ok"
					continue
				}
				cctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
				err := check(cctx)
				cancel()
				if err != nil {
					resp.Status = "unhealthy"
					resp.Checks[name] = err.Error()
					code = http.StatusServiceUnavailable
				} else {
					resp.Checks[name] = "ok"
				}
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(resp)
	})
}

// MetricsHandler returns the Prometheus HTTP handler for reg.
func MetricsHandler(reg *prometheus.Registry) http.Handler {
	if reg == nil {
		return promhttp.Handler()
	}
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// NewMux builds a ServeMux exposing /healthz, /readyz, and /metrics.
func NewMux(reg *prometheus.Registry, checks map[string]HealthCheck) *http.ServeMux {
	mux := http.NewServeMux()
	hh := HealthHandler(checks)
	mux.Handle("GET /healthz", hh)
	mux.Handle("GET /readyz", hh)
	mux.Handle("GET /metrics", MetricsHandler(reg))
	return mux
}

// Server wraps an HTTP server exposing /healthz and /metrics.
type Server struct {
	httpServer *http.Server
	logger     *slog.Logger
}

// NewServer constructs an observability HTTP server on addr (e.g., ":8082").
func NewServer(addr string, reg *prometheus.Registry, logger *slog.Logger, checks map[string]HealthCheck) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	mux := NewMux(reg, checks)
	return &Server{
		httpServer: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		logger: logger,
	}
}

// Handler returns the underlying HTTP handler.
func (s *Server) Handler() http.Handler {
	return s.httpServer.Handler
}

// ListenAndServe starts the server and blocks until closed.
// Returns nil on graceful http.ErrServerClosed.
func (s *Server) ListenAndServe() error {
	s.logger.Info("obs server listening", "addr", s.httpServer.Addr)
	err := s.httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Serve accepts connections on an existing net.Listener.
func (s *Server) Serve(ln net.Listener) error {
	err := s.httpServer.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown gracefully stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// WaitForShutdown blocks until ctx is canceled or SIGINT/SIGTERM is received,
// then runs each shutdown hook in order with a context bounded by timeout.
func WaitForShutdown(ctx context.Context, timeout time.Duration, logger *slog.Logger, hooks ...func(context.Context) error) error {
	if logger == nil {
		logger = slog.Default()
	}
	sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	<-sigCtx.Done()
	logger.Info("shutdown signal received")

	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	shutCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var errs []error
	for i, hook := range hooks {
		if hook == nil {
			continue
		}
		if err := hook(shutCtx); err != nil {
			logger.Error("shutdown hook failed", "hook_index", i, "error", err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
