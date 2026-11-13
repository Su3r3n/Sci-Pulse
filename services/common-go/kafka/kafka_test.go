package kafka

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestProducerConfigDefaults(t *testing.T) {
	if _, err := (ProducerConfig{}).BuildOptions(); err == nil {
		t.Fatal("expected error when brokers are empty")
	}

	p, err := NewProducer(ProducerConfig{Brokers: []string{"localhost:9094"}})
	if err != nil {
		t.Fatalf("NewProducer failed: %v", err)
	}
	defer p.Close()

	cfg := p.Config()
	if cfg.DeliveryTimeout < MinChaosDeliveryTimeout {
		t.Fatalf("DeliveryTimeout = %v, want >= %v", cfg.DeliveryTimeout, MinChaosDeliveryTimeout)
	}
	if cfg.Linger != DefaultLinger {
		t.Fatalf("Linger = %v, want %v", cfg.Linger, DefaultLinger)
	}
	if cfg.BatchMaxBytes != DefaultBatchMaxBytes {
		t.Fatalf("BatchMaxBytes = %d, want %d", cfg.BatchMaxBytes, DefaultBatchMaxBytes)
	}
}

func TestConsumerConfigValidation(t *testing.T) {
	if _, err := (ConsumerConfig{}).BuildOptions(); err == nil {
		t.Fatal("expected error when brokers empty")
	}
	if _, err := (ConsumerConfig{Brokers: []string{"localhost:9094"}}).BuildOptions(); err == nil {
		t.Fatal("expected error when group empty")
	}
	if _, err := (ConsumerConfig{Brokers: []string{"localhost:9094"}, Group: "g1"}).BuildOptions(); err == nil {
		t.Fatal("expected error when topics empty")
	}
}

func TestProducerAndConsumerManualCommit_Kfake(t *testing.T) {
	const topic = "raw.papers.openalex"
	const group = "test-group"

	cluster := kfake.MustCluster(kfake.SeedTopics(1, topic))
	defer cluster.Close()

	prod, err := NewProducer(ProducerConfig{
		Brokers:         cluster.ListenAddrs(),
		Linger:          2 * time.Millisecond,
		DeliveryTimeout: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, msg := range []struct{ k, v string }{
		{"k1", "v1"},
		{"k2", "v2"},
	} {
		if err := prod.ProduceSync(ctx, topic, []byte(msg.k), []byte(msg.v)); err != nil {
			t.Fatalf("ProduceSync(%s): %v", msg.k, err)
		}
	}
	if err := prod.Shutdown(ctx); err != nil {
		t.Fatalf("Producer.Shutdown: %v", err)
	}

	// First consumer fails on k2 -> k1 must be committed, k2 must NOT be committed.
	cons1, err := NewConsumer(ConsumerConfig{
		Brokers:       cluster.ListenAddrs(),
		Group:         group,
		Topics:        []string{topic},
		FromBeginning: true,
	})
	if err != nil {
		t.Fatalf("NewConsumer 1: %v", err)
	}

	var seen1 []string
	errFail := errors.New("simulated handler failure")
	err = cons1.Consume(ctx, func(_ context.Context, rec *kgo.Record) error {
		seen1 = append(seen1, string(rec.Key)+":"+string(rec.Value))
		if string(rec.Key) == "k2" {
			return errFail
		}
		return nil
	})
	cons1.Close()
	if !errors.Is(err, errFail) {
		t.Fatalf("Consume 1 err = %v, want %v", err, errFail)
	}
	if len(seen1) != 2 || seen1[0] != "k1:v1" || seen1[1] != "k2:v2" {
		t.Fatalf("seen1 = %v, want [k1:v1 k2:v2]", seen1)
	}

	// Second consumer in same group must resume at k2 (k1 already committed, k2 not committed).
	cons2, err := NewConsumer(ConsumerConfig{
		Brokers:       cluster.ListenAddrs(),
		Group:         group,
		Topics:        []string{topic},
		FromBeginning: true,
	})
	if err != nil {
		t.Fatalf("NewConsumer 2: %v", err)
	}
	defer cons2.Close()

	c2Ctx, c2Cancel := context.WithCancel(ctx)
	var seen2 []string
	var count atomic.Int32
	err = cons2.Consume(c2Ctx, func(_ context.Context, rec *kgo.Record) error {
		seen2 = append(seen2, string(rec.Key)+":"+string(rec.Value))
		if count.Add(1) == 1 {
			c2Cancel()
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Consume 2 unexpected error: %v", err)
	}
	if len(seen2) != 1 || seen2[0] != "k2:v2" {
		t.Fatalf("seen2 = %v, want [k2:v2] (k1 was committed, k2 redelivered)", seen2)
	}
}
