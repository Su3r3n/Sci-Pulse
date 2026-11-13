package cache

import (
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"
)

func TestDiskCacheRoundTrip(t *testing.T) {
	dir := t.TempDir()
	c := NewDiskCache(dir)

	key := "https://api.openalex.org/works?per_page=100&cursor=*"
	payload := []byte(`{"results":[{"id":"W123"}],"meta":{"next_cursor":"abc"}}`)

	if got, ok, err := c.Get("openalex", key); err != nil || ok || got != nil {
		t.Fatalf("expected cache miss, got ok=%v err=%v", ok, err)
	}

	if err := c.Put("openalex", key, payload); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	expectedPath := filepath.Join(dir, "openalex", KeyHash(key)+".json.gz")
	if _, err := os.Stat(expectedPath); err != nil {
		t.Fatalf("expected file at %s: %v", expectedPath, err)
	}

	got, ok, err := c.Get("openalex", key)
	if err != nil || !ok {
		t.Fatalf("expected cache hit, got ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %q want %q", got, payload)
	}

	listed, err := c.List("openalex")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(listed) != 1 || !bytes.Equal(listed[0], payload) {
		t.Fatalf("List mismatch: got %d items", len(listed))
	}
}

func TestDiskCacheListRawFixtures(t *testing.T) {
	dir := t.TempDir()
	c := NewDiskCache(dir)

	arxivDir := filepath.Join(dir, "arxiv")
	if err := os.MkdirAll(arxivDir, 0o755); err != nil {
		t.Fatal(err)
	}

	rawXML := []byte(`<OAI-PMH><ListRecords/></OAI-PMH>`)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(rawXML); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(arxivDir, "01_fixture.xml.gz"), buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	rawCorrupt := []byte(`{bad json`)
	if err := os.WriteFile(filepath.Join(arxivDir, "02_corrupt.json"), rawCorrupt, 0o644); err != nil {
		t.Fatal(err)
	}

	items, err := c.List("arxiv")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if !bytes.Equal(items[0], rawXML) {
		t.Fatalf("item 0 mismatch: %q", items[0])
	}
	if !bytes.Equal(items[1], rawCorrupt) {
		t.Fatalf("item 1 mismatch: %q", items[1])
	}
}
