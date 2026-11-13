// Command loadgen is the synthetic load generator (§5.6). This revision provides the process
// skeleton only: HTTP server with /healthz, /metrics and the control API stub. Event generation
// into load.synthetic arrives with sprint S5.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/scipulse/scipulse/services/common-go/config"
	"github.com/scipulse/scipulse/services/common-go/obs"
	"github.com/scipulse/scipulse/services/loadgen-go/internal/api"
	"github.com/scipulse/scipulse/services/loadgen-go/internal/metrics"
)

// Config holds runtime settings from the environment.
type Config struct {
	HTTPAddr          string        `env:"HTTP_ADDR" default:":8084"`
	Port              string        `env:"PORT"` // used when HTTP_ADDR is not set explicitly (m3)
	LogLevel          string        `env:"LOG_LEVEL" default:"info"`
	KafkaBrokers      string        `env:"KAFKA_BROKERS" default:"kafka:9092"`
	SchemaRegistryURL string        `env:"SCHEMA_REGISTRY_URL" default:"http://schema-registry:8081"`
	ShutdownTimeout   time.Duration `env:"SHUTDOWN_TIMEOUT" default:"10s"`
}

// listenAddr picks the listen address: an explicit HTTP_ADDR wins, then ":"+PORT when PORT is set,
// otherwise the loaded default (HTTP_ADDR default ":8084").
func listenAddr(explicitHTTPAddr, port, loaded string) string {
	if a := strings.TrimSpace(explicitHTTPAddr); a != "" {
		return a
	}
	if p := strings.TrimSpace(port); p != "" {
		return ":" + p
	}
	return loaded
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "loadgen: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg Config
	if err := config.Load(&cfg); err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger := obs.NewLogger("loadgen-go", obs.ParseLogLevel(cfg.LogLevel))

	reg := obs.NewRegistry()
	metrics.New(reg)

	mux := obs.NewMux(reg, map[string]obs.HealthCheck{
		"process": func(context.Context) error { return nil },
	})
	api.Register(mux, logger)

	addr := listenAddr(os.Getenv("HTTP_ADDR"), cfg.Port, cfg.HTTPAddr)
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("loadgen HTTP server listening", "addr", addr, "kafka_brokers", cfg.KafkaBrokers)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	logger.Info("shutdown signal received")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	slog.Default().Info("loadgen stopped")
	return nil
}
