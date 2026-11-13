package tests

import (
	"encoding/json"
	"encoding/xml"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const (
	coder1Root = `C:\Users\beish\orca\workspaces\Sci-Pulse\main`
	coder2Root = `C:\Users\beish\orca\workspaces\Sci-Pulse\main-2`
)

type avroSchema struct {
	Type      string      `json:"type"`
	Name      string      `json:"name"`
	Namespace string      `json:"namespace"`
	Fields    []avroField `json:"fields"`
}

type avroField struct {
	Name    string          `json:"name"`
	Type    json.RawMessage `json:"type"`
	Default json.RawMessage `json:"default,omitempty"`
}

func TestAvroContractsExistAndMatchSpec(t *testing.T) {
	expectedSchemas := map[string][]string{
		"paper.v1.avsc": {
			"source", "source_id", "doi", "arxiv_id",
			"title", "abstract", "comments", "journal_ref",
			"authors", "topics", "published_at", "updated_at",
			"cited_by_count", "links", "ingested_at",
		},
		"dlq_envelope.v1.avsc": {
			"reason", "stage", "source", "original_topic",
			"original_key", "payload", "error", "failed_at",
			"attempt", "rule_applied",
		},
		"repo_snapshot.v1.avsc": {
			"repo_full_name", "stars", "forks", "open_issues",
			"archived", "pushed_at", "snapshot_at",
		},
		"release.v1.avsc": {
			"repo_full_name", "tag", "name", "published_at",
			"prerelease", "fetched_at",
		},
		"code_event.v1.avsc": {
			"event_id", "type", "repo_full_name", "actor_login",
			"created_at", "replayed_at", "source_hour",
		},
		"software_record.v1.avsc": {
			"record_doi", "concept_doi", "version", "title",
			"published_at", "related_identifiers", "repo_full_name",
		},
		"synthetic_event.v1.avsc": {
			"event_type", "country", "latency_ms", "bytes",
			"produced_at", "synthetic",
		},
	}

	contractsDir := filepath.Join(coder1Root, "contracts")
	for file, reqFields := range expectedSchemas {
		raw, err := os.ReadFile(filepath.Join(contractsDir, file))
		if err != nil {
			t.Fatalf("failed reading %s: %v", file, err)
		}
		var s avroSchema
		if err := json.Unmarshal(raw, &s); err != nil {
			t.Fatalf("invalid JSON in %s: %v", file, err)
		}
		if s.Type != "record" || s.Namespace != "org.scipulse.v1" {
			t.Errorf("%s: expected type=record namespace=org.scipulse.v1, got type=%s namespace=%s", file, s.Type, s.Namespace)
		}
		fieldSet := make(map[string]avroField, len(s.Fields))
		for _, f := range s.Fields {
			fieldSet[f.Name] = f
		}
		for _, req := range reqFields {
			if _, ok := fieldSet[req]; !ok {
				t.Errorf("%s: missing required field %q", file, req)
			}
		}
	}
}

func checkBackwardCompatible(oldSchema, newSchema avroSchema) []string {
	var errs []string
	oldFields := make(map[string]avroField, len(oldSchema.Fields))
	for _, f := range oldSchema.Fields {
		oldFields[f.Name] = f
	}
	for _, nf := range newSchema.Fields {
		of, exists := oldFields[nf.Name]
		if !exists {
			if len(nf.Default) == 0 {
				errs = append(errs, "added field without default: "+nf.Name)
			}
			continue
		}
		if string(of.Type) != string(nf.Type) {
			errs = append(errs, "changed type of field: "+nf.Name)
		}
	}
	return errs
}

func TestAvroBackwardCompatibilityRules(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(coder1Root, "contracts", "paper.v1.avsc"))
	if err != nil {
		t.Fatal(err)
	}
	var base avroSchema
	if err := json.Unmarshal(raw, &base); err != nil {
		t.Fatal(err)
	}

	// 1. Adding optional field with default null -> compatible (§3.5)
	validNext := base
	validNext.Fields = append(append([]avroField{}, base.Fields...), avroField{
		Name:    "openaire_id",
		Type:    json.RawMessage(`["null","string"]`),
		Default: json.RawMessage(`null`),
	})
	if errs := checkBackwardCompatible(base, validNext); len(errs) != 0 {
		t.Fatalf("expected valid optional field addition to be BACKWARD-compatible, got %v", errs)
	}

	// 2. Adding required field without default -> incompatible (§3.5)
	invalidReq := base
	invalidReq.Fields = append(append([]avroField{}, base.Fields...), avroField{
		Name: "mandatory_new_field",
		Type: json.RawMessage(`"string"`),
	})
	if errs := checkBackwardCompatible(base, invalidReq); len(errs) == 0 {
		t.Fatal("expected adding field without default to fail BACKWARD compatibility check")
	}

	// 3. Changing field type -> incompatible (§3.5)
	invalidType := base
	invalidType.Fields = append([]avroField{}, base.Fields...)
	invalidType.Fields[0].Type = json.RawMessage(`"int"`)
	if errs := checkBackwardCompatible(base, invalidType); len(errs) == 0 {
		t.Fatal("expected changing field type to fail BACKWARD compatibility check")
	}
}

func TestOfflineFixturesValidAndDLQCases(t *testing.T) {
	oaDir := filepath.Join("..", "data", "cache", "openalex")
	arxivDir := filepath.Join("..", "data", "cache", "arxiv")

	validOA, err := os.ReadFile(filepath.Join(oaDir, "01_valid_works.json"))
	if err != nil {
		t.Fatal(err)
	}
	var oaPayload struct {
		Results []map[string]any `json:"results"`
	}
	if err := json.Unmarshal(validOA, &oaPayload); err != nil {
		t.Fatalf("01_valid_works.json should be valid JSON: %v", err)
	}
	if len(oaPayload.Results) != 3 {
		t.Fatalf("expected 3 valid OpenAlex works, got %d", len(oaPayload.Results))
	}

	corruptOA, err := os.ReadFile(filepath.Join(oaDir, "03_corrupt.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(corruptOA, &oaPayload); err == nil {
		t.Fatal("expected 03_corrupt.json to fail JSON unmarshaling for DLQ parse_error")
	}

	validArxiv, err := os.ReadFile(filepath.Join(arxivDir, "01_valid_listrecords.xml"))
	if err != nil {
		t.Fatal(err)
	}
	var xmlNode any
	if err := xml.Unmarshal(validArxiv, &xmlNode); err != nil {
		t.Fatalf("01_valid_listrecords.xml should be valid XML: %v", err)
	}

	corruptArxiv, err := os.ReadFile(filepath.Join(arxivDir, "03_corrupt.xml"))
	if err != nil {
		t.Fatal(err)
	}
	var decoderTarget struct {
		XMLName xml.Name `xml:"OAI-PMH"`
		Records []any    `xml:"ListRecords>record"`
	}
	if err := xml.Unmarshal(corruptArxiv, &decoderTarget); err == nil {
		t.Fatal("expected 03_corrupt.xml to fail XML unmarshaling for DLQ parse_error")
	}
}

func TestKafkaTopicsMatchSection2_2(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(coder2Root, "platform", "kafka", "init-topics.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	expected := map[string]string{
		"raw.papers.openalex":                       "3",
		"raw.papers.arxiv":                          "3",
		"raw.papers.inspire":                        "3",
		"raw.code.github":                           "3",
		"raw.code.events":                           "3",
		"raw.code.zenodo":                           "1",
		"cdc.registry.public.tracked_projects":      "1",
		"cdc.registry.public.project_repos":         "1",
		"cdc.registry.public.institutions":          "1",
		"cdc.registry.public.project_institutions":  "1",
		"dlq.papers":                                "1",
		"dlq.code":                                  "1",
		"load.synthetic":                            "6",
	}
	for topic, partitions := range expected {
		re := regexp.MustCompile(`create_(?:delete|compact)_topic\s+"` + regexp.QuoteMeta(topic) + `"\s+` + partitions + `\b`)
		if !re.MatchString(script) {
			t.Errorf("topic %s with %s partitions not found in init-topics.sh", topic, partitions)
		}
	}
	if !strings.Contains(script, "cleanup.policy=compact") {
		t.Error("expected cleanup.policy=compact for CDC topics in init-topics.sh")
	}
}

func TestRegistryMigrationsMatchSection4_3(t *testing.T) {
	migDir := filepath.Join(coder1Root, "services", "registry-go", "migrations")
	initUp, err := os.ReadFile(filepath.Join(migDir, "000001_init.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	cdcUp, err := os.ReadFile(filepath.Join(migDir, "000002_cdc.up.sql"))
	if err != nil {
		t.Fatal(err)
	}

	for _, table := range []string{"institutions", "tracked_projects", "project_repos", "project_institutions"} {
		if !strings.Contains(string(initUp), "CREATE TABLE "+table) {
			t.Errorf("000001_init.up.sql missing CREATE TABLE %s", table)
		}
		if !strings.Contains(string(cdcUp), "ALTER TABLE "+table) {
			t.Errorf("000002_cdc.up.sql missing ALTER TABLE %s REPLICA IDENTITY FULL", table)
		}
	}
	if !strings.Contains(string(cdcUp), "CREATE PUBLICATION dbz_registry") {
		t.Error("000002_cdc.up.sql missing CREATE PUBLICATION dbz_registry")
	}
}
