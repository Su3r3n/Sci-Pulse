package pipeline

import "time"

// DLQ reason codes from §3.3.
const (
	ReasonParseError         = "parse_error"
	ReasonMissingSourceID    = "missing_source_id"
	ReasonMissingTitle       = "missing_title"
	ReasonMissingPublishedAt = "missing_published_at"
	ReasonBadDOI             = "bad_doi"
	ReasonFuturePublishedAt  = "future_published_at"
	ReasonNoAuthors          = "no_authors"
	ReasonSchemaError        = "schema_error"
)

// DLQ stages matching contracts/dlq_envelope.v1.avsc.
const (
	StageFetch     = "fetch"
	StageParse     = "parse"
	StageMap       = "map"
	StageValidate  = "validate"
	StageSerialize = "serialize"
	StageProduce   = "produce"
)

// DLQTopicPapers is the Kafka topic for paper dead-letter events.
const DLQTopicPapers = "dlq.papers"

// RawTopicForSource returns raw.papers.<source>.
func RawTopicForSource(source string) string {
	return "raw.papers." + source
}

// Paper matches contracts/paper.v1.avsc (org.scipulse.v1.Paper).
type Paper struct {
	Source       string    `avro:"source"`
	SourceID     string    `avro:"source_id"`
	DOI          *string   `avro:"doi"`
	ArxivID      *string   `avro:"arxiv_id"`
	Title        string    `avro:"title"`
	Abstract     *string   `avro:"abstract"`
	Comments     *string   `avro:"comments"`
	JournalRef   *string   `avro:"journal_ref"`
	Authors      []Author  `avro:"authors"`
	Topics       []string  `avro:"topics"`
	PublishedAt  time.Time `avro:"published_at"`
	UpdatedAt    time.Time `avro:"updated_at"`
	CitedByCount *int      `avro:"cited_by_count"`
	Links        []Link    `avro:"links"`
	IngestedAt   time.Time `avro:"ingested_at"`
}

// Author matches org.scipulse.v1.Author.
type Author struct {
	Name         string   `avro:"name"`
	ORCID        *string  `avro:"orcid"`
	Affiliations []string `avro:"affiliations"`
}

// Link matches org.scipulse.v1.Link.
type Link struct {
	URL  string `avro:"url"`
	Kind string `avro:"kind"`
}

// DLQEnvelope matches contracts/dlq_envelope.v1.avsc (org.scipulse.v1.DlqEnvelope).
type DLQEnvelope struct {
	Reason        string    `avro:"reason"`
	Stage         string    `avro:"stage"`
	Source        string    `avro:"source"`
	OriginalTopic string    `avro:"original_topic"`
	OriginalKey   *string   `avro:"original_key"`
	Payload       []byte    `avro:"payload"`
	Error         string    `avro:"error"`
	FailedAt      time.Time `avro:"failed_at"`
	Attempt       int       `avro:"attempt"`
	RuleApplied   *string   `avro:"rule_applied"`
}

// RecordKey returns the Kafka UTF-8 message key for Paper:
// normalized DOI if present, otherwise "<source>:<source_id>".
func (p Paper) RecordKey() string {
	if p.DOI != nil && *p.DOI != "" {
		return *p.DOI
	}
	return p.Source + ":" + p.SourceID
}
