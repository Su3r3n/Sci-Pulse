package pipeline

import (
	"errors"
	"testing"
	"time"

	"github.com/scipulse/scipulse/services/harvester-go/internal/source"
)

var fixedNow = time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)

func TestOpenAlexMappingAndNormalization(t *testing.T) {
	rawJSON := []byte(`{
		"id": "https://openalex.org/W2741809807",
		"doi": "HTTPS://DOI.ORG/10.1016/J.NIMA.2026.01.001",
		"title": "Fast Analysis with Rucio",
		"abstract_inverted_index": {
			"Code": [0],
			"at": [1],
			"https://github.com/rucio/rucio.": [2]
		},
		"publication_date": "2026-10-15",
		"updated_date": "2026-11-10T08:30:00Z",
		"cited_by_count": 42,
		"authorships": [{
			"author": {"display_name": "Alice Physicist", "orcid": "https://orcid.org/0000-0001-2345-6789"},
			"institutions": [{"display_name": "CERN"}]
		}],
		"topics": [{"display_name": "High Energy Physics"}],
		"primary_location": {"landing_page_url": "https://example.org/paper", "pdf_url": "https://example.org/paper.pdf"}
	}`)

	res := Process(source.RawRecord{
		Source:  "openalex",
		ID:      "https://openalex.org/W2741809807",
		Body:    rawJSON,
		Fetched: fixedNow,
	}, fixedNow)

	if res.DLQ != nil || res.Paper == nil {
		t.Fatalf("expected valid paper, got DLQ=%+v", res.DLQ)
	}
	p := res.Paper
	if res.Topic != "raw.papers.openalex" {
		t.Fatalf("unexpected topic: %s", res.Topic)
	}
	if res.Key != "10.1016/j.nima.2026.01.001" {
		t.Fatalf("unexpected key: %s", res.Key)
	}
	if p.SourceID != "W2741809807" {
		t.Fatalf("unexpected source_id: %s", p.SourceID)
	}
	if p.DOI == nil || *p.DOI != "10.1016/j.nima.2026.01.001" {
		t.Fatalf("expected normalized DOI, got %v", p.DOI)
	}
	if p.Abstract == nil || *p.Abstract != "Code at https://github.com/rucio/rucio." {
		t.Fatalf("unexpected abstract: %v", p.Abstract)
	}
	if len(p.Authors) != 1 || p.Authors[0].Name != "Alice Physicist" || *p.Authors[0].ORCID != "0000-0001-2345-6789" {
		t.Fatalf("unexpected authors: %+v", p.Authors)
	}
	hasCodeLink := false
	for _, l := range p.Links {
		if l.Kind == "code" && l.URL == "https://github.com/rucio/rucio" {
			hasCodeLink = true
		}
	}
	if !hasCodeLink {
		t.Fatalf("expected extracted GitHub code link, got %+v", p.Links)
	}
}

func TestArxivMappingPreservesCommentsJournalRefAndCodeLinks(t *testing.T) {
	rawXML := []byte(`<record>
		<header>
			<identifier>oai:arXiv.org:2611.00042</identifier>
			<datestamp>2026-11-11</datestamp>
		</header>
		<metadata>
			<arXiv xmlns="http://arxiv.org/OAI/arXiv/">
				<id>2611.00042</id>
				<created>2026-11-01</created>
				<updated>2026-11-10</updated>
				<authors>
					<author>
						<keyname>Doe</keyname>
						<forenames>Jane</forenames>
						<affiliation>DESY</affiliation>
					</author>
				</authors>
				<title>ROOT
					DataFrames at Scale</title>
				<categories>hep-ex cs.DC</categories>
				<comments>12 pages, code available at https://github.com/root-project/root</comments>
				<journal-ref>J. Comput. Phys. 2026</journal-ref>
				<abstract>We benchmark ROOT.</abstract>
			</arXiv>
		</metadata>
	</record>`)

	res := Process(source.RawRecord{
		Source:  "arxiv",
		ID:      "oai:arXiv.org:2611.00042",
		Body:    rawXML,
		Fetched: fixedNow,
	}, fixedNow)

	if res.DLQ != nil || res.Paper == nil {
		t.Fatalf("expected valid paper, got DLQ=%+v", res.DLQ)
	}
	p := res.Paper
	if res.Topic != "raw.papers.arxiv" || res.Key != "arxiv:2611.00042" {
		t.Fatalf("unexpected topic=%s key=%s", res.Topic, res.Key)
	}
	if p.Comments == nil || *p.Comments != "12 pages, code available at https://github.com/root-project/root" {
		t.Fatalf("comments not preserved: %v", p.Comments)
	}
	if p.JournalRef == nil || *p.JournalRef != "J. Comput. Phys. 2026" {
		t.Fatalf("journal_ref not preserved: %v", p.JournalRef)
	}
	if p.Title != "ROOT DataFrames at Scale" {
		t.Fatalf("unexpected title: %q", p.Title)
	}
	foundCode := false
	for _, l := range p.Links {
		if l.Kind == "code" && l.URL == "https://github.com/root-project/root" {
			foundCode = true
		}
	}
	if !foundCode {
		t.Fatalf("expected GitHub code link from comments, got %+v", p.Links)
	}
}

func TestDLQReasonCodes(t *testing.T) {
	cases := []struct {
		name       string
		rec        source.RawRecord
		wantReason string
		wantStage  string
	}{
		{
			name: "source_parse_error",
			rec: source.RawRecord{
				Source:     "arxiv",
				ID:         "bad-page",
				Body:       []byte("<OAI-PMH><broken"),
				ParseError: errors.New("unexpected EOF"),
			},
			wantReason: ReasonParseError,
			wantStage:  StageParse,
		},
		{
			name: "corrupt_json_parse_error",
			rec: source.RawRecord{
				Source: "openalex",
				ID:     "bad-json",
				Body:   []byte(`{"id": "W1"`),
			},
			wantReason: ReasonParseError,
			wantStage:  StageParse,
		},
		{
			name: "missing_source_id",
			rec: source.RawRecord{
				Source: "openalex",
				Body:   []byte(`{"title":"No ID","publication_date":"2026-10-01","authorships":[{"author":{"display_name":"A"}}]}`),
			},
			wantReason: ReasonMissingSourceID,
			wantStage:  StageValidate,
		},
		{
			name: "missing_title",
			rec: source.RawRecord{
				Source: "openalex",
				ID:     "W1",
				Body:   []byte(`{"id":"W1","title":"  ","publication_date":"2026-10-01","authorships":[{"author":{"display_name":"A"}}]}`),
			},
			wantReason: ReasonMissingTitle,
			wantStage:  StageValidate,
		},
		{
			name: "missing_published_at",
			rec: source.RawRecord{
				Source: "openalex",
				ID:     "W2",
				Body:   []byte(`{"id":"W2","title":"Valid Title","authorships":[{"author":{"display_name":"A"}}]}`),
			},
			wantReason: ReasonMissingPublishedAt,
			wantStage:  StageValidate,
		},
		{
			name: "future_published_at",
			rec: source.RawRecord{
				Source: "openalex",
				ID:     "W3",
				Body:   []byte(`{"id":"W3","title":"Future","publication_date":"2099-01-01","authorships":[{"author":{"display_name":"A"}}]}`),
			},
			wantReason: ReasonFuturePublishedAt,
			wantStage:  StageValidate,
		},
		{
			name: "bad_doi",
			rec: source.RawRecord{
				Source: "openalex",
				ID:     "W4",
				Body:   []byte(`{"id":"W4","doi":"not-a-doi","title":"Bad DOI","publication_date":"2026-10-01","authorships":[{"author":{"display_name":"A"}}]}`),
			},
			wantReason: ReasonBadDOI,
			wantStage:  StageValidate,
		},
		{
			name: "no_authors",
			rec: source.RawRecord{
				Source: "openalex",
				ID:     "W5",
				Body:   []byte(`{"id":"W5","title":"No Authors","publication_date":"2026-10-01","authorships":[]}`),
			},
			wantReason: ReasonNoAuthors,
			wantStage:  StageValidate,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := Process(tc.rec, fixedNow)
			if res.Paper != nil || res.DLQ == nil {
				t.Fatalf("expected DLQ result, got paper=%+v", res.Paper)
			}
			if res.Topic != DLQTopicPapers {
				t.Fatalf("expected topic %s, got %s", DLQTopicPapers, res.Topic)
			}
			if res.DLQ.Reason != tc.wantReason {
				t.Fatalf("reason mismatch: got %q want %q", res.DLQ.Reason, tc.wantReason)
			}
			if res.DLQ.Stage != tc.wantStage {
				t.Fatalf("stage mismatch: got %q want %q", res.DLQ.Stage, tc.wantStage)
			}
			if string(res.DLQ.Payload) != string(tc.rec.Body) {
				t.Fatalf("payload mismatch: got %q want %q", res.DLQ.Payload, tc.rec.Body)
			}
		})
	}
}

func TestParseErrorDLQKeyMatchesMappedSourceKey(t *testing.T) {
	cases := []struct {
		name string
		raw  source.RawRecord
		want string
	}{
		{
			name: "openalex URL id",
			raw:  source.RawRecord{Source: "openalex", ID: "https://openalex.org/W2741809807", Body: []byte(`{not json`)},
			want: "openalex:W2741809807",
		},
		{
			name: "arxiv OAI id",
			raw:  source.RawRecord{Source: "arxiv", ID: "oai:arXiv.org:2611.00001", Body: []byte(`<record>broken`)},
			want: "arxiv:2611.00001",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := Process(tc.raw, fixedNow)
			if res.DLQ == nil {
				t.Fatalf("expected DLQ for broken payload")
			}
			if res.Key != tc.want {
				t.Fatalf("DLQ key = %q, want %q", res.Key, tc.want)
			}
			if res.DLQ.OriginalKey == nil || *res.DLQ.OriginalKey != tc.want {
				t.Fatalf("original_key = %v, want %q", res.DLQ.OriginalKey, tc.want)
			}
		})
	}
}
