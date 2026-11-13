package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestHelpers(t *testing.T) {
	t.Setenv("TEST_STR", "hello")
	t.Setenv("TEST_EMPTY", "")
	t.Setenv("TEST_INT", "42")
	t.Setenv("TEST_INT_BAD", "abc")
	t.Setenv("TEST_DUR", "2m30s")
	t.Setenv("TEST_DUR_BAD", "nope")
	t.Setenv("TEST_BOOL", "true")
	t.Setenv("TEST_BOOL_BAD", "maybe")

	if got := GetEnv("TEST_STR", "def"); got != "hello" {
		t.Fatalf("GetEnv = %q, want hello", got)
	}
	if got := GetEnv("TEST_MISSING", "def"); got != "def" {
		t.Fatalf("GetEnv missing = %q, want def", got)
	}
	if got := GetEnv("TEST_EMPTY", "def"); got != "def" {
		t.Fatalf("GetEnv empty = %q, want def", got)
	}

	if got := MustGetEnv("TEST_STR"); got != "hello" {
		t.Fatalf("MustGetEnv = %q, want hello", got)
	}

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected MustGetEnv to panic on missing var")
			}
		}()
		MustGetEnv("TEST_MISSING_PANIC")
	}()

	if n, err := GetEnvInt("TEST_INT", 10); err != nil || n != 42 {
		t.Fatalf("GetEnvInt = (%d, %v), want (42, nil)", n, err)
	}
	if n, err := GetEnvInt("TEST_MISSING", 10); err != nil || n != 10 {
		t.Fatalf("GetEnvInt missing = (%d, %v), want (10, nil)", n, err)
	}
	if _, err := GetEnvInt("TEST_INT_BAD", 10); err == nil {
		t.Fatal("expected error for invalid int")
	}

	if d, err := GetEnvDuration("TEST_DUR", time.Second); err != nil || d != 150*time.Second {
		t.Fatalf("GetEnvDuration = (%v, %v), want (2m30s, nil)", d, err)
	}
	if d, err := GetEnvDuration("TEST_MISSING", 5*time.Second); err != nil || d != 5*time.Second {
		t.Fatalf("GetEnvDuration missing = (%v, %v), want (5s, nil)", d, err)
	}
	if _, err := GetEnvDuration("TEST_DUR_BAD", time.Second); err == nil {
		t.Fatal("expected error for invalid duration")
	}

	if b, err := GetEnvBool("TEST_BOOL", false); err != nil || !b {
		t.Fatalf("GetEnvBool = (%v, %v), want (true, nil)", b, err)
	}
	if b, err := GetEnvBool("TEST_MISSING", true); err != nil || !b {
		t.Fatalf("GetEnvBool missing = (%v, %v), want (true, nil)", b, err)
	}
	if _, err := GetEnvBool("TEST_BOOL_BAD", false); err == nil {
		t.Fatal("expected error for invalid bool")
	}
}

type sampleNested struct {
	Brokers []string `env:"KAFKA_BROKERS" default:"localhost:9094"`
	Timeout time.Duration `env:"KAFKA_TIMEOUT" default:"2m"`
}

type sampleConfig struct {
	Port     int     `env:"PORT" default:"8080"`
	MaxConns int32   `env:"MAX_CONNS" default:"10"`
	APIKey   string  `env:"API_KEY" required:"true"`
	Debug    bool    `env:"DEBUG" default:"false"`
	Rate     float64 `env:"RATE" default:"1.5"`
	Kafka    sampleNested
	validated bool
}

func (c *sampleConfig) Validate() error {
	c.validated = true
	if c.Port <= 0 || c.Port > 65535 {
		return errors.New("port out of range")
	}
	return nil
}

func TestLoadFrom_SuccessAndDefaults(t *testing.T) {
	env := map[string]string{
		"API_KEY":       "secret-123",
		"KAFKA_BROKERS": "kafka1:9092, kafka2:9092",
		"DEBUG":         "true",
	}
	var cfg sampleConfig
	err := LoadFrom(&cfg, func(k string) (string, bool) {
		v, ok := env[k]
		return v, ok
	})
	if err != nil {
		t.Fatalf("LoadFrom unexpected error: %v", err)
	}
	if cfg.Port != 8080 {
		t.Fatalf("Port = %d, want 8080", cfg.Port)
	}
	if cfg.MaxConns != 10 {
		t.Fatalf("MaxConns = %d, want 10", cfg.MaxConns)
	}
	if cfg.APIKey != "secret-123" {
		t.Fatalf("APIKey = %q, want secret-123", cfg.APIKey)
	}
	if !cfg.Debug {
		t.Fatal("Debug = false, want true")
	}
	if cfg.Rate != 1.5 {
		t.Fatalf("Rate = %v, want 1.5", cfg.Rate)
	}
	if len(cfg.Kafka.Brokers) != 2 || cfg.Kafka.Brokers[0] != "kafka1:9092" || cfg.Kafka.Brokers[1] != "kafka2:9092" {
		t.Fatalf("Brokers = %v, want [kafka1:9092 kafka2:9092]", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.Timeout != 2*time.Minute {
		t.Fatalf("Timeout = %v, want 2m", cfg.Kafka.Timeout)
	}
	if !cfg.validated {
		t.Fatal("expected Validate() to be called")
	}
}

func TestLoadFrom_Errors(t *testing.T) {
	var cfg sampleConfig
	err := LoadFrom(&cfg, func(string) (string, bool) { return "", false })
	if err == nil || !strings.Contains(err.Error(), "API_KEY") {
		t.Fatalf("expected missing API_KEY error, got: %v", err)
	}

	env := map[string]string{
		"API_KEY": "ok",
		"PORT":    "99999",
	}
	err = LoadFrom(&cfg, func(k string) (string, bool) {
		v, ok := env[k]
		return v, ok
	})
	if err == nil || !strings.Contains(err.Error(), "port out of range") {
		t.Fatalf("expected validation error, got: %v", err)
	}
}
