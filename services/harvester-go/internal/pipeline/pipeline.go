package pipeline

import (
	"strings"
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
			k := rawRecordKey(raw)
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
			k := rawRecordKey(raw)
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

// rawRecordKey is the "<source>:<source_id>" key of a raw record whose paper could not be mapped.
// The identifier is reduced to the form the mapper uses for source_id (the OpenAlex URL and the OAI
// prefix are removed), so a replayed record lands in the same partition as its mapped form (§3.4).
func rawRecordKey(raw source.RawRecord) string {
	id := strings.TrimSpace(raw.ID)
	switch raw.Source {
	case "openalex":
		id = strings.TrimPrefix(id, "https://openalex.org/")
		id = strings.TrimPrefix(id, "http://openalex.org/")
	case "arxiv":
		id = strings.TrimPrefix(id, "oai:arXiv.org:")
	}
	return raw.Source + ":" + id
}

func derefString(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
