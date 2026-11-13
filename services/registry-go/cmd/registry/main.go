package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/golang-migrate/migrate/v4"
	pgx5migrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/scipulse/scipulse/services/common-go/config"
	"github.com/scipulse/scipulse/services/common-go/obs"
	registry "github.com/scipulse/scipulse/services/registry-go"
	reghttp "github.com/scipulse/scipulse/services/registry-go/internal/http"
	"github.com/scipulse/scipulse/services/registry-go/internal/repository"
	"github.com/scipulse/scipulse/services/registry-go/internal/service"
	"github.com/scipulse/scipulse/services/registry-go/migrations"
)

// Config holds runtime configuration loaded from environment variables.
type Config struct {
	DatabaseURL     string        `env:"DATABASE_URL" required:"true"`
	APIKey          string        `env:"REGISTRY_API_KEY" required:"true"`
	Port            string        `env:"PORT" default:"8080"`
	LogLevel        string        `env:"LOG_LEVEL" default:"info"`
	RequestTimeout  time.Duration `env:"REQUEST_TIMEOUT" default:"5s"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" default:"10s"`
	DBMaxConns      int32         `env:"DB_MAX_CONNS" default:"10"`
	DBMinConns      int32         `env:"DB_MIN_CONNS" default:"2"`
}

func main() {
	var cfg Config
	if err := config.Load(&cfg); err != nil {
		slog.New(slog.NewJSONHandler(os.Stdout, nil)).Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	logger := obs.NewLogger("registry", obs.ParseLogLevel(cfg.LogLevel))

	if err := runMigrations(cfg.DatabaseURL, logger); err != nil {
		logger.Error("database migration failed", "error", err)
		os.Exit(1)
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		logger.Error("invalid DATABASE_URL", "error", err)
		os.Exit(1)
	}
	if cfg.DBMaxConns > 0 {
		poolCfg.MaxConns = cfg.DBMaxConns
	}
	if cfg.DBMinConns >= 0 {
		poolCfg.MinConns = cfg.DBMinConns
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	cancel()
	if err != nil {
		logger.Error("failed to create postgres pool", "error", err)
		os.Exit(1)
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := pool.Ping(pingCtx); err != nil {
		pingCancel()
		pool.Close()
		logger.Error("failed to ping postgres", "error", err)
		os.Exit(1)
	}
	pingCancel()

	promReg := obs.NewRegistry()
	promReg.MustRegister(repository.NewPgxPoolCollector(pool, "registry"))

	repo := repository.NewPostgresRepository(pool)
	svc := service.NewRegistryService(repo)
	router := reghttp.NewRouter(reghttp.RouterOptions{
		Service:        svc,
		APIKey:         cfg.APIKey,
		Logger:         logger,
		PromRegistry:   promReg,
		OpenAPIYAML:    registry.OpenAPIYAML,
		RequestTimeout: cfg.RequestTimeout,
	})

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("registry HTTP server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	waitCtx, stopWait := context.WithCancel(context.Background())
	defer stopWait()

	go func() {
		if err, ok := <-errCh; ok && err != nil {
			logger.Error("http server error", "error", err)
			stopWait()
		}
	}()

	if err := obs.WaitForShutdown(waitCtx, cfg.ShutdownTimeout, logger,
		func(sCtx context.Context) error {
			return srv.Shutdown(sCtx)
		},
		func(_ context.Context) error {
			pool.Close()
			return nil
		},
	); err != nil {
		logger.Error("shutdown completed with errors", "error", err)
		os.Exit(1)
	}

	logger.Info("registry server stopped cleanly")
}

func runMigrations(databaseURL string, logger *slog.Logger) error {
	poolCfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}

	sqlDB := stdlib.OpenDB(*poolCfg.ConnConfig)
	defer func() {
		_ = sqlDB.Close()
	}()

	dbDriver, err := pgx5migrate.WithInstance(sqlDB, &pgx5migrate.Config{})
	if err != nil {
		return fmt.Errorf("create pgx migrate driver: %w", err)
	}

	srcDriver, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("create iofs migration source: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", srcDriver, "pgx5", dbDriver)
	if err != nil {
		return fmt.Errorf("init migrate instance: %w", err)
	}
	defer func() {
		_, _ = m.Close()
	}()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply up migrations: %w", err)
	}

	version, dirty, err := m.Version()
	if err != nil && !errors.Is(err, migrate.ErrNilVersion) {
		return fmt.Errorf("read migration version: %w", err)
	}
	logger.Info("database migrations applied", "version", version, "dirty", dirty)
	return nil
}
