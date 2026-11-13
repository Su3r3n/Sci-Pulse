package sink

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/scipulse/scipulse/services/common-go/avro"
	"github.com/scipulse/scipulse/services/common-go/kafka"
	"github.com/scipulse/scipulse/services/harvester-go/internal/metrics"
	"github.com/scipulse/scipulse/services/harvester-go/internal/pipeline"
)

//go:embed schemas/paper.v1.avsc
var PaperSchemaV1 string

//go:embed schemas/dlq_envelope.v1.avsc
var DLQEnvelopeSchemaV1 string

// BatchItem pairs a routed pipeline output with the original raw bytes for DLQ fallback.
type BatchItem struct {
	Result  pipeline.RouteResult
	RawBody []byte
}

// Sink produces validated papers and DLQ envelopes to Kafka.
type Sink interface {
	ProduceBatch(ctx context.Context, items []BatchItem) error
	Flush(ctx context.Context) error
	Close() error
}

// KafkaSink encodes records with Confluent Avro framing and produces them via common-go/kafka.
type KafkaSink struct {
	producer *kafka.Producer
	codec    *avro.Codec
	metrics  *metrics.Metrics
	logger   *slog.Logger
}

// NewKafkaSink constructs a KafkaSink backed by a common-go kafka.Producer and avro.Codec.
func NewKafkaSink(producer *kafka.Producer, srClient *avro.Client, m *metrics.Metrics, logger *slog.Logger) (*KafkaSink, error) {
	if producer == nil {
		return nil, fmt.Errorf("sink: producer must not be nil")
	}
	if srClient == nil {
		return nil, fmt.Errorf("sink: schema registry client must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &KafkaSink{
		producer: producer,
		codec:    avro.NewCodec(srClient),
		metrics:  m,
		logger:   logger,
	}, nil
}

// EnsureSchemas pre-registers paper.v1 and dlq_envelope.v1 for the given paper sources.
func (s *KafkaSink) EnsureSchemas(ctx context.Context, srClient *avro.Client, sources []string) error {
	if _, err := srClient.RegisterSchema(ctx, pipeline.DLQTopicPapers+"-value", DLQEnvelopeSchemaV1); err != nil {
		return fmt.Errorf("register dlq schema: %w", err)
	}
	for _, src := range sources {
		subj := pipeline.RawTopicForSource(src) + "-value"
		if _, err := srClient.RegisterSchema(ctx, subj, PaperSchemaV1); err != nil {
			return fmt.Errorf("register paper schema for %s: %w", subj, err)
		}
	}
	return nil
}

// ProduceBatch Avro-encodes and synchronously produces all items in a batch.
// If any Paper fails Avro serialization, it is rerouted to dlq.papers with reason="schema_error".
func (s *KafkaSink) ProduceBatch(ctx context.Context, items []BatchItem) error {
	if len(items) == 0 {
		return nil
	}

	records := make([]*kgo.Record, 0, len(items))
	topicsSeen := make(map[string]struct{})

	for _, item := range items {
		res := item.Result
		if res.Paper != nil {
			subject := res.Topic + "-value"
			wire, err := s.codec.Encode(ctx, subject, PaperSchemaV1, res.Paper)
			if err != nil {
				s.logger.Warn("paper avro serialization failed, routing to dlq",
					"source", res.Paper.Source,
					"source_id", res.Paper.SourceID,
					"error", err)
				s.metrics.IncDLQ(res.Paper.Source, pipeline.ReasonSchemaError)

				var origKey *string
				if res.Key != "" {
					k := res.Key
					origKey = &k
				}
				dlq := &pipeline.DLQEnvelope{
					Reason:        pipeline.ReasonSchemaError,
					Stage:         pipeline.StageSerialize,
					Source:        res.Paper.Source,
					OriginalTopic: res.Topic,
					OriginalKey:   origKey,
					Payload:       item.RawBody,
					Error:         err.Error(),
					FailedAt:      time.Now().UTC(),
					Attempt:       0,
					RuleApplied:   nil,
				}
				dlqWire, dlqErr := s.codec.Encode(ctx, pipeline.DLQTopicPapers+"-value", DLQEnvelopeSchemaV1, dlq)
				if dlqErr != nil {
					return fmt.Errorf("encode schema_error dlq envelope: %w", dlqErr)
				}
				records = append(records, &kgo.Record{
					Topic: pipeline.DLQTopicPapers,
					Key:   []byte(res.Key),
					Value: dlqWire,
				})
				topicsSeen[pipeline.DLQTopicPapers] = struct{}{}
				continue
			}

			records = append(records, &kgo.Record{
				Topic: res.Topic,
				Key:   []byte(res.Key),
				Value: wire,
			})
			topicsSeen[res.Topic] = struct{}{}
			continue
		}

		if res.DLQ != nil {
			wire, err := s.codec.Encode(ctx, pipeline.DLQTopicPapers+"-value", DLQEnvelopeSchemaV1, res.DLQ)
			if err != nil {
				return fmt.Errorf("encode dlq envelope: %w", err)
			}
			var keyBytes []byte
			if res.Key != "" {
				keyBytes = []byte(res.Key)
			}
			records = append(records, &kgo.Record{
				Topic: pipeline.DLQTopicPapers,
				Key:   keyBytes,
				Value: wire,
			})
			topicsSeen[pipeline.DLQTopicPapers] = struct{}{}
		}
	}

	if len(records) == 0 {
		return nil
	}

	start := time.Now()
	if err := s.producer.ProduceRecordsSync(ctx, records...); err != nil {
		return fmt.Errorf("produce batch to kafka: %w", err)
	}
	dur := time.Since(start)
	for topic := range topicsSeen {
		s.metrics.ObserveProduceLatency(topic, dur)
	}
	return nil
}

// Flush flushes any buffered records on the underlying Kafka producer.
func (s *KafkaSink) Flush(ctx context.Context) error {
	return s.producer.Flush(ctx)
}

// Close flushes and shuts down the Kafka producer.
func (s *KafkaSink) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return s.producer.Shutdown(ctx)
}
