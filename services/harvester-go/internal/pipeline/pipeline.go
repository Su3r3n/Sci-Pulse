package pipeline

import (
	"time"

	"github.com/scipulse/scipulse/services/harvester-go/internal/source"
)

// RouteResult holds the output of processing a single RawRecord through map -> validate -> route.
type RouteResult struct {
	Topic string
	Key   string
	Paper *Paper
	DLQ   *DLQEnvelope
}

// Process runs map -> validate -> route on a RawRecord.
func Process(raw source.RawRecord, now time.Time) RouteResult {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	originalTopic := RawTopicForSource(raw.Source)

	paper, stage, reason, err := MapRecord(raw, now)
	if err != nil {
		var origKey *string
		if raw.ID != "" {
			k := raw.Source + ":" + raw.ID
			origKey = &k
		}
		return RouteResult{
			Topic: DLQTopicPapers,
			Key:   derefString(origKey),
			DLQ: &DLQEnvelope{
				Reason:        reason,
				Stage:         stage,
				Source:        raw.Source,
				OriginalTopic: originalTopic,
				OriginalKey:   origKey,
				Payload:       raw.Body,
				Error:         err.Error(),
				FailedAt:      now.UTC(),
				Attempt:       0,
				RuleApplied:   nil,
			},
		}
	}

	if reason, verr := ValidatePaper(paper, now); verr != nil {
		key := paper.RecordKey()
		var origKey *string
		if key != "" && key != raw.Source+":" {
			origKey = &key
		} else if raw.ID != "" {
			k := raw.Source + ":" + raw.ID
			origKey = &k
		}
		return RouteResult{
			Topic: DLQTopicPapers,
			Key:   derefString(origKey),
			DLQ: &DLQEnvelope{
				Reason:        reason,
				Stage:         StageValidate,
				Source:        raw.Source,
				OriginalTopic: originalTopic,
				OriginalKey:   origKey,
				Payload:       raw.Body,
				Error:         verr.Error(),
				FailedAt:      now.UTC(),
				Attempt:       0,
				RuleApplied:   nil,
			},
		}
	}

	return RouteResult{
		Topic: originalTopic,
		Key:   paper.RecordKey(),
		Paper: &paper,
	}
}

func derefString(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
