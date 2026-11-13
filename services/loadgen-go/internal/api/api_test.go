package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newMux() *http.ServeMux {
	mux := http.NewServeMux()
	Register(mux, slog.New(slog.NewTextHandler(&strings.Builder{}, nil)))
	return mux
}

func TestParseStressRequest(t *testing.T) {
	params := func(m map[string]string) func(string) string {
		return func(k string) string { return m[k] }
	}
	req, err := ParseStressRequest(params(map[string]string{"rate": "25000", "duration": "60s"}))
	if err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	if req.Rate != 25000 || req.Duration != 60*time.Second || req.Profile != ProfileSteady {
		t.Fatalf("unexpected parse result: %+v", req)
	}

	bad := []map[string]string{
		{"duration": "60s"},
		{"rate": "0", "duration": "60s"},
		{"rate": "x", "duration": "60s"},
		{"rate": "10", "duration": "-1s"},
		{"rate": "10", "duration": "60s", "profile": "spiky"},
	}
	for _, b := range bad {
		if _, err := ParseStressRequest(params(b)); err == nil {
			t.Errorf("expected rejection for %v", b)
		}
	}
}

func TestStressTestValidatesThenReportsNotImplemented(t *testing.T) {
	mux := newMux()

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/stress-test?rate=0&duration=60s", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid rate: status %d, want 400", rec.Code)
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/stress-test?rate=30000&duration=60s&profile=burst", nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("valid request: status %d, want 501 until S5", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("content type %q, want problem+json", ct)
	}
}

func TestStatusAndStopAreIdempotent(t *testing.T) {
	mux := newMux()

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/status", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status: %d", rec.Code)
	}
	var st Status
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil || st.State != "idle" {
		t.Fatalf("status body %q: %v", rec.Body.String(), err)
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/stress-test", nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete: %d, want 204", rec.Code)
	}
}
