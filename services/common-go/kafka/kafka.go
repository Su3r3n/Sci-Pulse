package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// DefaultLinger is the default batching linger duration for producers.
	DefaultLinger = 10 * time.Millisecond
	// DefaultBatchMaxBytes is the default max batch size (1 MiB).
	DefaultBatchMaxBytes int32 = 1 << 20
	// DefaultDeliveryTimeout is >= 2m to survive transient broker outages (Chaos C1).
	DefaultDeliveryTimeout = 150 * time.Second
	// MinChaosDeliveryTimeout is the recommended minimum delivery timeout for Chaos C1.
	MinChaosDeliveryTimeout = 2 * time.Minute
)

// ProducerConfig configures an idempotent, acks=all Kafka producer.
type ProducerConfig struct {
	Brokers            []string
	ClientID           string
	Linger             time.Duration
	BatchMaxBytes      int32
	MaxBufferedRecords int
	DeliveryTimeout    time.Duration
	Compression        []kgo.CompressionCodec
	Logger             *slog.Logger
	ExtraOpts          []kgo.Opt
}

func (c ProducerConfig) withDefaults() ProducerConfig {
	if c.Linger <= 0 {
		c.Linger = DefaultLinger
	}
	if c.BatchMaxBytes <= 0 {
		c.BatchMaxBytes = DefaultBatchMaxBytes
	}
	if c.DeliveryTimeout <= 0 {
		c.DeliveryTimeout = DefaultDeliveryTimeout
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

// BuildOptions constructs the kgo options for ProducerConfig (exported for inspection/testing).
func (c ProducerConfig) BuildOptions() ([]kgo.Opt, error) {
	if len(c.Brokers) == 0 {
		return nil, errors.New("kafka: producer requires at least one broker")
	}
	cfg := c.withDefaults()

	// Note: franz-go enables idempotency by default when RequiredAcks(AllISRAcks()) is used
	// and DisableIdempotentWrite() is NOT passed.
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(cfg.Linger),
		kgo.ProducerBatchMaxBytes(cfg.BatchMaxBytes),
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	if cfg.MaxBufferedRecords > 0 {
		opts = append(opts, kgo.MaxBufferedRecords(cfg.MaxBufferedRecords))
	}
	if len(cfg.Compression) > 0 {
		opts = append(opts, kgo.ProducerBatchCompression(cfg.Compression...))
	}
	if len(cfg.ExtraOpts) > 0 {
		opts = append(opts, cfg.ExtraOpts...)
	}
	return opts, nil
}

// Producer wraps kgo.Client with idempotent acks=all production and graceful flush/shutdown.
type Producer struct {
	client *kgo.Client
	cfg    ProducerConfig
	logger *slog.Logger
}

// NewProducer creates a new idempotent Producer with RequiredAcks(AllISRAcks()).
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	resolved := cfg.withDefaults()
	opts, err := resolved.BuildOptions()
	if err != nil {
		return nil, err
	}
	if resolved.DeliveryTimeout < MinChaosDeliveryTimeout {
		resolved.Logger.Warn("kafka producer DeliveryTimeout is below recommended 2m chaos C1 threshold",
			"delivery_timeout", resolved.DeliveryTimeout)
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: create producer client: %w", err)
	}
	return &Producer{
		client: client,
		cfg:    resolved,
		logger: resolved.Logger,
	}, nil
}

// Config returns the resolved ProducerConfig.
func (p *Producer) Config() ProducerConfig {
	return p.cfg
}

// Client returns the underlying kgo.Client.
func (p *Producer) Client() *kgo.Client {
	return p.client
}

// ProduceSync produces a single record synchronously and waits for broker acknowledgment.
func (p *Producer) ProduceSync(ctx context.Context, topic string, key, value []byte, headers ...kgo.RecordHeader) error {
	rec := &kgo.Record{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: headers,
	}
	return p.ProduceRecordsSync(ctx, rec)
}

// ProduceRecordsSync produces one or more records synchronously and returns the first error if any failed.
func (p *Producer) ProduceRecordsSync(ctx context.Context, recs ...*kgo.Record) error {
	if len(recs) == 0 {
		return nil
	}
	results := p.client.ProduceSync(ctx, recs...)
	return results.FirstErr()
}

// ProduceAsync enqueues a record asynchronously and invokes cb on completion.
func (p *Producer) ProduceAsync(ctx context.Context, rec *kgo.Record, cb func(*kgo.Record, error)) {
	p.client.Produce(ctx, rec, cb)
}

// Flush waits for all buffered records to be acknowledged or ctx to expire.
func (p *Producer) Flush(ctx context.Context) error {
	return p.client.Flush(ctx)
}

// Close closes the underlying kgo.Client immediately.
func (p *Producer) Close() {
	p.client.Close()
}

// Shutdown flushes buffered records within ctx and then closes the client.
func (p *Producer) Shutdown(ctx context.Context) error {
	err := p.client.Flush(ctx)
	p.client.Close()
	if err != nil {
		return fmt.Errorf("kafka: flush on shutdown: %w", err)
	}
	return nil
}

// ConsumerConfig configures a Kafka consumer group with manual offset commits.
type ConsumerConfig struct {
	Brokers        []string
	Group          string
	Topics         []string
	ClientID       string
	FromBeginning  bool
	FetchMaxWait   time.Duration
	FetchMaxBytes  int32
	SessionTimeout time.Duration
	Logger         *slog.Logger
	ExtraOpts      []kgo.Opt
}

func (c ConsumerConfig) withDefaults() ConsumerConfig {
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

// BuildOptions constructs kgo options for ConsumerConfig, always enforcing DisableAutoCommit().
func (c ConsumerConfig) BuildOptions() ([]kgo.Opt, error) {
	if len(c.Brokers) == 0 {
		return nil, errors.New("kafka: consumer requires at least one broker")
	}
	if c.Group == "" {
		return nil, errors.New("kafka: consumer group must not be empty")
	}
	if len(c.Topics) == 0 {
		return nil, errors.New("kafka: consumer requires at least one topic")
	}

	resetOffset := kgo.NewOffset().AtEnd()
	if c.FromBeginning {
		resetOffset = kgo.NewOffset().AtStart()
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ConsumerGroup(c.Group),
		kgo.ConsumeTopics(c.Topics...),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(resetOffset),
	}
	if c.ClientID != "" {
		opts = append(opts, kgo.ClientID(c.ClientID))
	}
	if c.FetchMaxWait > 0 {
		opts = append(opts, kgo.FetchMaxWait(c.FetchMaxWait))
	}
	if c.FetchMaxBytes > 0 {
		opts = append(opts, kgo.FetchMaxBytes(c.FetchMaxBytes))
	}
	if c.SessionTimeout > 0 {
		opts = append(opts, kgo.SessionTimeout(c.SessionTimeout))
	}
	if len(c.ExtraOpts) > 0 {
		opts = append(opts, c.ExtraOpts...)
	}
	return opts, nil
}

// Consumer wraps kgo.Client with DisableAutoCommit and manual CommitRecords after successful processing.
type Consumer struct {
	client *kgo.Client
	cfg    ConsumerConfig
	logger *slog.Logger
}

// NewConsumer creates a new Consumer with auto-commit disabled.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	resolved := cfg.withDefaults()
	opts, err := resolved.BuildOptions()
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: create consumer client: %w", err)
	}
	return &Consumer{
		client: client,
		cfg:    resolved,
		logger: resolved.Logger,
	}, nil
}

// Client returns the underlying kgo.Client.
func (c *Consumer) Client() *kgo.Client {
	return c.client
}

// CommitRecords manually commits offsets for the given records.
func (c *Consumer) CommitRecords(ctx context.Context, recs ...*kgo.Record) error {
	if len(recs) == 0 {
		return nil
	}
	return c.client.CommitRecords(ctx, recs...)
}

// Close closes the underlying kgo.Client.
func (c *Consumer) Close() {
	c.client.Close()
}

// RecordHandler processes a single Kafka record. Returning a non-nil error prevents
// that record (and subsequent records in the same partition batch) from being committed.
type RecordHandler func(ctx context.Context, rec *kgo.Record) error

// BatchHandler processes a batch of records. Returning a non-nil error prevents committing the batch.
type BatchHandler func(ctx context.Context, recs []*kgo.Record) error

// Consume polls records and invokes handler per record (per partition in order).
// Successfully processed records are committed via CommitRecords. If handler returns an error,
// any earlier succeeded records in the partition are committed first, and Consume returns the error
// without committing the failed record.
func (c *Consumer) Consume(ctx context.Context, handler RecordHandler) error {
	if handler == nil {
		return errors.New("kafka: record handler must not be nil")
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
					return fe.Err
				}
				c.logger.Error("kafka fetch error", "topic", fe.Topic, "partition", fe.Partition, "error", fe.Err)
			}
		}

		var toCommit []*kgo.Record
		var handlerErr error

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if handlerErr != nil {
				return
			}
			for _, rec := range p.Records {
				if err := handler(ctx, rec); err != nil {
					handlerErr = err
					return
				}
				toCommit = append(toCommit, rec)
			}
		})

		if len(toCommit) > 0 {
			commitCtx := ctx
			if handlerErr != nil {
				// Use a short background context if caller context is still valid or canceling,
				// so already-processed records are not lost on error.
				var cancel context.CancelFunc
				commitCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
				err := c.client.CommitRecords(commitCtx, toCommit...)
				cancel()
				if err != nil {
					return errors.Join(handlerErr, fmt.Errorf("kafka: commit succeeded prefix: %w", err))
				}
			} else if err := c.client.CommitRecords(commitCtx, toCommit...); err != nil {
				return fmt.Errorf("kafka: commit records: %w", err)
			}
		}

		if handlerErr != nil {
			return handlerErr
		}
	}
}

// ConsumeBatch polls fetches and passes all records in each poll to handler,
// committing offsets only if handler returns nil.
func (c *Consumer) ConsumeBatch(ctx context.Context, handler BatchHandler) error {
	if handler == nil {
		return errors.New("kafka: batch handler must not be nil")
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
					return fe.Err
				}
				c.logger.Error("kafka fetch error", "topic", fe.Topic, "partition", fe.Partition, "error", fe.Err)
			}
		}

		recs := fetches.Records()
		if len(recs) == 0 {
			continue
		}

		if err := handler(ctx, recs); err != nil {
			return err
		}
		if err := c.client.CommitRecords(ctx, recs...); err != nil {
			return fmt.Errorf("kafka: commit batch records: %w", err)
		}
	}
}
