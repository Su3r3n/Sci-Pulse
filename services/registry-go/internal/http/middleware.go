package http

import (
	"crypto/subtle"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/scipulse/scipulse/services/registry-go/internal/model"
)

// Metrics holds Prometheus HTTP metrics for the registry service.
type Metrics struct {
	requestsTotal   *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
}

// NewMetrics creates and registers HTTP metrics with reg (if non-nil).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		requestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "http_requests_total",
				Help: "Total number of HTTP requests processed.",
			},
			[]string{"route", "code"},
		),
		requestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "http_request_duration_seconds",
				Help:    "HTTP request latency in seconds.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"route"},
		),
	}
	if reg != nil {
		reg.MustRegister(m.requestsTotal, m.requestDuration)
	}
	return m
}

// RequestIDHeader sets X-Request-ID on the response using chi's request ID.
func RequestIDHeader(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if reqID := middleware.GetReqID(r.Context()); reqID != "" {
			w.Header().Set("X-Request-ID", reqID)
		}
		next.ServeHTTP(w, r)
	})
}

// SlogLogger logs each HTTP request as structured JSON using slog.
func SlogLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)

			status := ww.Status()
			if status == 0 {
				status = http.StatusOK
			}

			route := routePattern(r)
			logger.Info("http request",
				"request_id", middleware.GetReqID(r.Context()),
				"method", r.Method,
				"path", r.URL.Path,
				"route", route,
				"status", status,
				"bytes", ww.BytesWritten(),
				"duration_ms", float64(time.Since(start).Microseconds())/1000.0,
			)
		})
	}
}

// ProblemRecoverer catches panics and responds with an RFC 7807/9457 problem+json 500 error.
func ProblemRecoverer(logger *slog.Logger) func(http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					if rec == http.ErrAbortHandler {
						panic(rec)
					}
					logger.Error("panic recovered",
						"request_id", middleware.GetReqID(r.Context()),
						"panic", fmt.Sprintf("%v", rec),
						"stack", string(debug.Stack()),
					)
					writeProblem(w, r, http.StatusInternalServerError, "about:blank", "Internal Server Error", "an unexpected error occurred", nil)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// MetricsMiddleware records http_requests_total{route,code} and http_request_duration_seconds{route}.
func MetricsMiddleware(m *Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if m == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)

			status := ww.Status()
			if status == 0 {
				status = http.StatusOK
			}
			route := routePattern(r)

			m.requestsTotal.WithLabelValues(route, strconv.Itoa(status)).Inc()
			m.requestDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
		})
	}
}

// APIKeyAuth enforces X-API-Key authentication using constant-time comparison.
func APIKeyAuth(expectedKey string) func(http.Handler) http.Handler {
	expectedBytes := []byte(expectedKey)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			provided := r.Header.Get("X-API-Key")
			if len(expectedBytes) == 0 || subtle.ConstantTimeCompare([]byte(provided), expectedBytes) != 1 {
				writeProblem(
					w,
					r,
					http.StatusUnauthorized,
					"https://scipulse.dev/problems/unauthorized",
					"Unauthorized",
					"missing or invalid X-API-Key header",
					[]model.FieldError{{
						Field:   "X-API-Key",
						Message: "missing or invalid API key",
						Code:    "unauthorized",
					}},
				)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func routePattern(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if pattern := rctx.RoutePattern(); pattern != "" {
			return pattern
		}
	}
	return "unmatched"
}
