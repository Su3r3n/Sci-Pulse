// Package api implements the loadgen control API of §5.6:
//
//	POST   /api/v1/stress-test?rate=...&duration=...&profile=steady|burst
//	GET    /api/v1/status
//	DELETE /api/v1/stress-test
//
// The generator itself (pacing, synthetic_event.v1 production) is planned for S5. Until then the
// control plane is a stub: it validates requests, reports an idle state and refuses to start a run.
package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// Profiles accepted by POST /api/v1/stress-test.
const (
	ProfileSteady = "steady"
	ProfileBurst  = "burst"
)

// Status is the body of GET /api/v1/status.
type Status struct {
	State        string  `json:"state"`
	OfferedRate  float64 `json:"offered_rate"`
	AchievedRate float64 `json:"achieved_rate"`
}

// StressRequest is a validated POST /api/v1/stress-test request.
type StressRequest struct {
	Rate     int
	Duration time.Duration
	Profile  string
}

// ParseStressRequest validates the query parameters of POST /api/v1/stress-test.
func ParseStressRequest(q func(string) string) (StressRequest, error) {
	rate, err := strconv.Atoi(q("rate"))
	if err != nil || rate <= 0 {
		return StressRequest{}, fmt.Errorf("rate must be a positive integer (events per second), got %q", q("rate"))
	}
	duration, err := time.ParseDuration(q("duration"))
	if err != nil || duration <= 0 {
		return StressRequest{}, fmt.Errorf("duration must be a positive Go duration such as 60s, got %q", q("duration"))
	}
	profile := q("profile")
	if profile == "" {
		profile = ProfileSteady
	}
	if profile != ProfileSteady && profile != ProfileBurst {
		return StressRequest{}, fmt.Errorf("profile must be %q or %q, got %q", ProfileSteady, ProfileBurst, profile)
	}
	return StressRequest{Rate: rate, Duration: duration, Profile: profile}, nil
}

// Register adds the control routes to mux.
func Register(mux *http.ServeMux, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	mux.HandleFunc("POST /api/v1/stress-test", func(w http.ResponseWriter, r *http.Request) {
		req, err := ParseStressRequest(func(k string) string { return r.URL.Query().Get(k) })
		if err != nil {
			writeProblem(w, http.StatusBadRequest, "Invalid stress-test parameters", err.Error())
			return
		}
		logger.Info("stress test requested but generator is not implemented yet",
			"rate", req.Rate, "duration", req.Duration, "profile", req.Profile)
		writeProblem(w, http.StatusNotImplemented, "Not implemented",
			"load generation is scheduled for sprint S5 (§10.9); the control plane only validates requests")
	})
	mux.HandleFunc("GET /api/v1/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, Status{State: "idle"})
	})
	mux.HandleFunc("DELETE /api/v1/stress-test", func(w http.ResponseWriter, r *http.Request) {
		// Nothing runs yet, so stopping is an idempotent no-op.
		w.WriteHeader(http.StatusNoContent)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// writeProblem writes an RFC 7807 problem document, the error format used across SciPulse services.
func writeProblem(w http.ResponseWriter, code int, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":   "about:blank",
		"title":  title,
		"status": code,
		"detail": detail,
	})
}
