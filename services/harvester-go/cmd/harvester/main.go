package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/scipulse/scipulse/services/common-go/avro"
	"github.com/scipulse/scipulse/services/common-go/config"
	"github.com/scipulse/scipulse/services/common-go/kafka"
	"github.com/scipulse/scipulse/services/common-go/obs"
	"github.com/scipulse/scipulse/services/harvester-go/internal/cache"
	"github.com/scipulse/scipulse/services/harvester-go/internal/metrics"
	"github.com/scipulse/scipulse/services/harvester-go/internal/ratelimit"
	"github.com/scipulse/scipulse/services/harvester-go/internal/runner"
	"github.com/scipulse/scipulse/services/harvester-go/internal/sink"
	"github.com/scipulse/scipulse/services/harvester-go/internal/source"
	"github.com/scipulse/scipulse/services/harvester-go/internal/state"
)

type appConfig struct {
	KafkaBrokers         string        `env:"KAFKA_BROKERS" default:"kafka:9092"`
	KafkaDeliveryTimeout time.Duration `env:"KAFKA_DELIVERY_TIMEOUT" default:"150s"`
	SchemaRegistryURL    string        `env:"SCHEMA_REGISTRY_URL" default:"http://schema-registry:8081"`
	DatabaseDSN          string        `env:"HARVESTER_DB_DSN"`
	MetricsAddr          string        `env:"HARVESTER_METRICS_ADDR" default:":8082"`
	CacheDir             string        `env:"HARVESTER_CACHE_DIR" default:"data/cache"`
	PollInterval         time.Duration `env:"HARVESTER_POLL_INTERVAL" default:"5m"`
	BatchSize            int           `env:"HARVESTER_BATCH_SIZE" default:"100"`
	DrainTimeout         time.Duration `env:"HARVESTER_DRAIN_TIMEOUT" default:"15s"`
	LogLevel             string        `env:"LOG_LEVEL" default:"info"`

	OpenAlexBaseURL     string  `env:"OPENALEX_BASE_URL" default:"https://api.openalex.org"`
	OpenAlexAPIKey      string  `env:"OPENALEX_API_KEY"`
	OpenAlexEmail       string  `env:"OPENALEX_EMAIL"`
	OpenAlexDailyBudget int     `env:"OPENALEX_DAILY_BUDGET" default:"100000"`
	OpenAlexRateLimit   float64 `env:"OPENALEX_RATE_LIMIT" default:"5"`

	ArxivBaseURL     string        `env:"ARXIV_BASE_URL" default:"https://export.arxiv.org/oai2"`
	ArxivSets        string        `env:"ARXIV_SETS" default:"cs,physics:hep-ex,physics:hep-ph"`
	ArxivPoliteDelay time.Duration `env:"ARXIV_POLITE_DELAY" default:"3s"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "harvester: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	mode := "daemon"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		switch args[0] {
		case "run":
			mode = "daemon"
			args = args[1:]
		default:
			return fmt.Errorf("unknown subcommand %q (expected 'run' or flags)", args[0])
		}
	}

	fs := flag.NewFlagSet("harvester", flag.ContinueOnError)
	once := fs.Bool("once", false, "run a single harvest pass and exit")
	offline := fs.Bool("offline", false, "read cached payloads from data/cache without network calls")
	sourcesFlag := fs.String("sources", "openalex,arxiv", "comma-separated list of enabled sources")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *once {
		mode = "once"
	}

	var cfg appConfig
	if err := config.Load(&cfg); err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if cfg.ArxivPoliteDelay < 3*time.Second {
		cfg.ArxivPoliteDelay = 3 * time.Second
	}

	logger := obs.NewLogger("harvester-go", obs.ParseLogLevel(cfg.LogLevel))
	reg := obs.NewRegistry()
	m := metrics.New(reg)

	rootCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	var (
		store  state.Store
		pgPool *pgxpool.Pool
	)
	if cfg.DatabaseDSN != "" {
		pool, err := pgxpool.New(rootCtx, cfg.DatabaseDSN)
		if err != nil {
			return fmt.Errorf("connect harvester db: %w", err)
		}
		pgPool = pool
		defer pgPool.Close()

		pgStore := state.NewPostgresStore(pgPool)
		if err := pgStore.Migrate(rootCtx); err != nil {
			return fmt.Errorf("migrate harvester db: %w", err)
		}
		store = pgStore
	} else {
		if !*offline {
			logger.Warn("HARVESTER_DB_DSN not set, using ephemeral in-memory state store")
		}
		store = state.NewMemoryStore()
	}

	healthChecks := map[string]obs.HealthCheck{
		"state": store.Ping,
	}
	obsSrv := obs.NewServer(cfg.MetricsAddr, reg, logger, healthChecks)
	obsErrCh := make(chan error, 1)
	go func() {
		if err := obsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			obsErrCh <- err
		}
		close(obsErrCh)
	}()
	defer func() {
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = obsSrv.Shutdown(shCtx)
	}()

	brokers := splitCSV(cfg.KafkaBrokers)
	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers:         brokers,
		ClientID:        "scipulse-harvester",
		DeliveryTimeout: cfg.KafkaDeliveryTimeout,
		Logger:          logger,
	})
	if err != nil {
		return fmt.Errorf("init kafka producer: %w", err)
	}
	srClient := avro.NewClient(cfg.SchemaRegistryURL)
	kafkaSink, err := sink.NewKafkaSink(producer, srClient, m, logger)
	if err != nil {
		producer.Close()
		return fmt.Errorf("init kafka sink: %w", err)
	}
	defer func() {
		if err := kafkaSink.Close(); err != nil {
			logger.Error("kafka sink close error", "error", err)
		}
	}()

	diskCache := cache.NewDiskCache(cfg.CacheDir)
	budgetTracker := ratelimit.NewBudgetTracker(store)

	enabledNames := splitCSV(*sourcesFlag)
	sources, err := buildSources(enabledNames, cfg, diskCache, budgetTracker, m, logger, *offline)
	if err != nil {
		return err
	}
	if err := kafkaSink.EnsureSchemas(rootCtx, srClient, enabledNames); err != nil {
		logger.Warn("schema pre-registration warning (will retry per record)", "error", err)
	}

	r := runner.New(sources, nil, store, kafkaSink, m, logger, runner.Config{
		BatchSize:    cfg.BatchSize,
		DrainTimeout: cfg.DrainTimeout,
		PollInterval: cfg.PollInterval,
	})

	logger.Info("harvester starting",
		"mode", mode,
		"offline", *offline,
		"sources", enabledNames,
		"metrics_addr", cfg.MetricsAddr)

	if mode == "once" {
		if err := r.RunOnce(rootCtx); err != nil {
			return fmt.Errorf("once pass failed: %w", err)
		}
		logger.Info("harvester once pass completed")
		return nil
	}

	if err := r.RunDaemon(rootCtx); err != nil {
		return fmt.Errorf("daemon shutdown error: %w", err)
	}
	select {
	case err := <-obsErrCh:
		if err != nil {
			return fmt.Errorf("metrics server error: %w", err)
		}
	default:
	}
	logger.Info("harvester exited cleanly")
	return nil
}

func buildSources(
	names []string,
	cfg appConfig,
	diskCache cache.Store,
	budget *ratelimit.BudgetTracker,
	m *metrics.Metrics,
	logger *slog.Logger,
	offline bool,
) ([]source.Source, error) {
	var out []source.Source
	for _, name := range names {
		switch name {
		case "openalex":
			var dailyBudget *int
			if cfg.OpenAlexDailyBudget > 0 {
				b := cfg.OpenAlexDailyBudget
				dailyBudget = &b
			}
			out = append(out, source.NewOpenAlexSource(source.OpenAlexConfig{
				BaseURL:     cfg.OpenAlexBaseURL,
				APIKey:      cfg.OpenAlexAPIKey,
				Email:       cfg.OpenAlexEmail,
				DailyBudget: dailyBudget,
				Offline:     offline,
				Cache:       diskCache,
				Limiter:     ratelimit.NewTokenBucket(cfg.OpenAlexRateLimit, 5),
				Budget:      budget,
				Metrics:     m,
				Logger:      logger,
			}))
		case "arxiv":
			out = append(out, source.NewArxivSource(source.ArxivConfig{
				BaseURL:     cfg.ArxivBaseURL,
				Sets:        splitCSV(cfg.ArxivSets),
				PoliteDelay: cfg.ArxivPoliteDelay,
				Offline:     offline,
				Cache:       diskCache,
				Metrics:     m,
				Logger:      logger,
			}))
		default:
			return nil, fmt.Errorf("unsupported source %q", name)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no sources configured")
	}
	return out, nil
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
