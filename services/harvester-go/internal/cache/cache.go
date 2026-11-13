package cache

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Entry wraps a cached HTTP response in a JSON envelope stored in <sha256>.json.gz.
type Entry struct {
	Key       string    `json:"key"`
	Source    string    `json:"source"`
	FetchedAt time.Time `json:"fetched_at"`
	Body      []byte    `json:"body"`
}

// Store defines cache operations for saving and reading raw API payloads.
type Store interface {
	Get(source, key string) ([]byte, bool, error)
	Put(source, key string, data []byte) error
	List(source string) ([][]byte, error)
}

// DiskCache stores gzipped response entries under <baseDir>/<source>/<sha256>.json.gz.
type DiskCache struct {
	baseDir string
	now     func() time.Time
}

// NewDiskCache creates a DiskCache rooted at baseDir.
func NewDiskCache(baseDir string) *DiskCache {
	if baseDir == "" {
		baseDir = filepath.Join("data", "cache")
	}
	return &DiskCache{
		baseDir: baseDir,
		now:     func() time.Time { return time.Now().UTC() },
	}
}

// KeyHash returns the lowercase hex SHA-256 digest for key.
func KeyHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// PathFor returns the file path for a source and key.
func (c *DiskCache) PathFor(source, key string) string {
	return filepath.Join(c.baseDir, sanitizeSource(source), KeyHash(key)+".json.gz")
}

// Put writes data to <baseDir>/<source>/<sha256>.json.gz atomically.
func (c *DiskCache) Put(source, key string, data []byte) error {
	path := c.PathFor(source, key)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("cache mkdir %s: %w", dir, err)
	}

	entry := Entry{
		Key:       key,
		Source:    source,
		FetchedAt: c.now(),
		Body:      data,
	}
	encoded, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("cache marshal: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".tmp-*.json.gz")
	if err != nil {
		return fmt.Errorf("cache create temp: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	gz := gzip.NewWriter(tmp)
	if _, err := gz.Write(encoded); err != nil {
		_ = gz.Close()
		_ = tmp.Close()
		return fmt.Errorf("cache gzip write: %w", err)
	}
	if err := gz.Close(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("cache gzip close: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("cache temp close: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("cache rename: %w", err)
	}
	cleanup = false
	return nil
}

// Get reads a cached payload by source and key.
func (c *DiskCache) Get(source, key string) ([]byte, bool, error) {
	path := c.PathFor(source, key)
	payload, err := readCacheFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return payload, true, nil
}

// List returns all cached payloads for a source in deterministic filename order.
// It supports both envelope-wrapped .json.gz files and raw .json/.xml(.gz) fixture files.
func (c *DiskCache) List(source string) ([][]byte, error) {
	dir := filepath.Join(c.baseDir, sanitizeSource(source))
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("cache readdir %s: %w", dir, err)
	}

	var names []string
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, ".json.gz") ||
			strings.HasSuffix(name, ".xml.gz") ||
			strings.HasSuffix(name, ".json") ||
			strings.HasSuffix(name, ".xml") {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	out := make([][]byte, 0, len(names))
	for _, name := range names {
		payload, err := readCacheFile(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("cache read %s: %w", name, err)
		}
		out = append(out, payload)
	}
	return out, nil
}

func readCacheFile(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	data := raw
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			return nil, fmt.Errorf("gunzip open %s: %w", path, err)
		}
		defer gz.Close()
		decompressed, err := io.ReadAll(gz)
		if err != nil {
			return nil, fmt.Errorf("gunzip read %s: %w", path, err)
		}
		data = decompressed
	}

	// Unwrap envelope if this file was written by Put.
	var env struct {
		Key    *string `json:"key"`
		Source *string `json:"source"`
		Body   []byte  `json:"body"`
	}
	if err := json.Unmarshal(data, &env); err == nil && env.Key != nil && env.Source != nil && env.Body != nil {
		return env.Body, nil
	}
	return data, nil
}

func sanitizeSource(source string) string {
	return filepath.Base(strings.TrimSpace(source))
}
