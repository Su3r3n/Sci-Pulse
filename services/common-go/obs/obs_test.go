package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewLoggerWithWriter(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLoggerWithWriter(&buf, "test-svc", ParseLogLevel("debug"))
	logger.Info("hello", "k", "v")

	var parsed map[string]any
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("expected valid JSON log line, got %q: %v", buf.String(), err)
	}
	if parsed["service"] != "test-svc" {
		t.Fatalf("service = %v, want test-svc", parsed["service"])
	}
	if parsed["msg"] != "hello" {
		t.Fatalf("msg = %v, want hello", parsed["msg"])
	}
	if parsed["k"] != "v" {
		t.Fatalf("k = %v, want v", parsed["k"])
	}
}

func TestHealthAndMetricsHandlers(t *testing.T) {
	reg := NewRegistry()
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "scipulse_test_events_total",
		Help: "Test counter",
	})
	reg.MustRegister(counter)
	counter.Add(3)

	// Healthy case (no checks)
	mux := NewMux(reg, nil)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz status = %d, want 200", rec.Code)
	}

	// Healthy case (passing check)
	mux = NewMux(reg, map[string]HealthCheck{
		"postgres": func(context.Context) error { return nil },
	})
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz with passing check status = %d, want 200", rec.Code)
	}

	// Unhealthy case (failing check)
	mux = NewMux(reg, map[string]HealthCheck{
		"postgres": func(context.Context) error { return nil },
		"kafka":    func(context.Context) error { return errors.New("broker down") },
	})
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/healthz with failing check status = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "broker down") {
		t.Fatalf("expected error message in /healthz body, got %s", rec.Body.String())
	}

	// Metrics endpoint
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "scipulse_test_events_total 3") {
		t.Fatalf("expected custom metric in /metrics output, got:\n%s", rec.Body.String())
	}
}

func TestWaitForShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var called []string
	var buf bytes.Buffer
	logger := NewLoggerWithWriter(&buf, "shutdown-test", slog.LevelInfo)

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	err := WaitForShutdown(ctx, time.Second, logger,
		func(context.Context) error {
			called = append(called, "first")
			return nil
		},
		func(context.Context) error {
			called = append(called, "second")
			return errors.New("hook2 failed")
		},
	)
	if err == nil || !strings.Contains(err.Error(), "hook2 failed") {
		t.Fatalf("expected hook2 error, got %v", err)
	}
	if len(called) != 2 || called[0] != "first" || called[1] != "second" {
		t.Fatalf("hooks called = %v, want [first second]", called)
	}
}
