package avro

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	hambaavro "github.com/hamba/avro/v2"
)

const (
	// MagicByte is the Confluent Schema Registry wire-format version byte.
	MagicByte byte = 0x00
	// HeaderSize is 1 magic byte + 4-byte big-endian schema ID.
	HeaderSize = 5

	schemaRegistryContentType = "application/vnd.schemaregistry.v1+json"
)

// Schema aliases github.com/hamba/avro/v2.Schema for callers.
type Schema = hambaavro.Schema

// Parse parses an Avro schema JSON string.
func Parse(schemaStr string) (Schema, error) {
	return hambaavro.Parse(schemaStr)
}

// Frame prepends the 5-byte Confluent Schema Registry header (0x00 + 4-byte BE schemaID)
// to an Avro binary payload.
func Frame(schemaID int, payload []byte) ([]byte, error) {
	if schemaID <= 0 || schemaID > math.MaxInt32 {
		return nil, fmt.Errorf("avro: invalid schema id %d", schemaID)
	}
	out := make([]byte, HeaderSize+len(payload))
	out[0] = MagicByte
	binary.BigEndian.PutUint32(out[1:HeaderSize], uint32(schemaID))
	copy(out[HeaderSize:], payload)
	return out, nil
}

// Unframe strips and validates the 5-byte Confluent Schema Registry header,
// returning the schema ID and the raw Avro binary payload slice.
func Unframe(data []byte) (int, []byte, error) {
	if len(data) < HeaderSize {
		return 0, nil, fmt.Errorf("avro: wire payload too short (%d bytes)", len(data))
	}
	if data[0] != MagicByte {
		return 0, nil, fmt.Errorf("avro: unexpected magic byte 0x%02x (want 0x%02x)", data[0], MagicByte)
	}
	id := binary.BigEndian.Uint32(data[1:HeaderSize])
	if id == 0 || id > math.MaxInt32 {
		return 0, nil, fmt.Errorf("avro: invalid schema id %d in header", id)
	}
	return int(id), data[HeaderSize:], nil
}

// RegistryError represents an error returned by the Schema Registry HTTP API.
type RegistryError struct {
	StatusCode int
	ErrorCode  int    `json:"error_code"`
	Message    string `json:"message"`
}

func (e *RegistryError) Error() string {
	if e.ErrorCode != 0 || e.Message != "" {
		return fmt.Sprintf("schema registry HTTP %d (code %d): %s", e.StatusCode, e.ErrorCode, e.Message)
	}
	return fmt.Sprintf("schema registry HTTP %d", e.StatusCode)
}

// ClientOption configures a Schema Registry Client.
type ClientOption func(*Client)

// WithHTTPClient sets a custom HTTP client for Schema Registry requests.
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) {
		if hc != nil {
			c.httpClient = hc
		}
	}
}

// WithTimeout configures the HTTP client timeout.
func WithTimeout(d time.Duration) ClientOption {
	return func(c *Client) {
		if d > 0 {
			c.httpClient.Timeout = d
		}
	}
}

// Client is a thread-safe Confluent Schema Registry HTTP client with in-memory caching.
type Client struct {
	baseURL    string
	httpClient *http.Client

	mu                sync.RWMutex
	subjectSchemaToID map[string]int
	idToSchema        map[int]Schema
	idToRaw           map[int]string
}

// NewClient creates a new Schema Registry client for baseURL (e.g. "http://schema-registry:8081").
func NewClient(baseURL string, opts ...ClientOption) *Client {
	c := &Client{
		baseURL:           strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		httpClient:        &http.Client{Timeout: 10 * time.Second},
		subjectSchemaToID: make(map[string]int),
		idToSchema:        make(map[int]Schema),
		idToRaw:           make(map[int]string),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

type registerRequest struct {
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType,omitempty"`
}

type registerResponse struct {
	ID int `json:"id"`
}

type schemaByIDResponse struct {
	Schema string `json:"schema"`
}

type latestSchemaResponse struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	ID      int    `json:"id"`
	Schema  string `json:"schema"`
}

type compatibilityResponse struct {
	IsCompatible bool `json:"is_compatible"`
}

type configRequest struct {
	Compatibility string `json:"compatibility"`
}

func subjectCacheKey(subject, schemaStr string) string {
	return subject + "\x00" + schemaStr
}

// RegisterSchema registers schemaStr under subject (idempotently, cached locally) and returns its schema ID.
func (c *Client) RegisterSchema(ctx context.Context, subject, schemaStr string) (int, error) {
	if strings.TrimSpace(subject) == "" {
		return 0, errors.New("avro: subject must not be empty")
	}
	parsed, err := hambaavro.Parse(schemaStr)
	if err != nil {
		return 0, fmt.Errorf("avro: invalid avro schema for subject %q: %w", subject, err)
	}

	key := subjectCacheKey(subject, schemaStr)
	c.mu.RLock()
	if id, ok := c.subjectSchemaToID[key]; ok {
		c.mu.RUnlock()
		return id, nil
	}
	c.mu.RUnlock()

	endpoint := fmt.Sprintf("%s/subjects/%s/versions", c.baseURL, url.PathEscape(subject))
	var resp registerResponse
	if err := c.doJSON(ctx, http.MethodPost, endpoint, registerRequest{Schema: schemaStr}, &resp); err != nil {
		return 0, err
	}
	if resp.ID <= 0 {
		return 0, fmt.Errorf("avro: schema registry returned invalid id %d for subject %q", resp.ID, subject)
	}

	c.mu.Lock()
	c.subjectSchemaToID[key] = resp.ID
	c.idToSchema[resp.ID] = parsed
	c.idToRaw[resp.ID] = schemaStr
	c.mu.Unlock()

	return resp.ID, nil
}

// GetSchemaByID fetches and caches the parsed Avro schema and raw JSON string for schema id.
func (c *Client) GetSchemaByID(ctx context.Context, id int) (Schema, string, error) {
	if id <= 0 {
		return nil, "", fmt.Errorf("avro: invalid schema id %d", id)
	}

	c.mu.RLock()
	if s, ok := c.idToSchema[id]; ok {
		raw := c.idToRaw[id]
		c.mu.RUnlock()
		return s, raw, nil
	}
	c.mu.RUnlock()

	endpoint := fmt.Sprintf("%s/schemas/ids/%s", c.baseURL, strconv.Itoa(id))
	var resp schemaByIDResponse
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &resp); err != nil {
		return nil, "", err
	}

	parsed, err := hambaavro.Parse(resp.Schema)
	if err != nil {
		return nil, "", fmt.Errorf("avro: failed to parse schema id %d from registry: %w", id, err)
	}

	c.mu.Lock()
	c.idToSchema[id] = parsed
	c.idToRaw[id] = resp.Schema
	c.mu.Unlock()

	return parsed, resp.Schema, nil
}

// GetLatestSchema retrieves and caches the latest registered schema version for subject.
func (c *Client) GetLatestSchema(ctx context.Context, subject string) (int, Schema, string, error) {
	if strings.TrimSpace(subject) == "" {
		return 0, nil, "", errors.New("avro: subject must not be empty")
	}

	endpoint := fmt.Sprintf("%s/subjects/%s/versions/latest", c.baseURL, url.PathEscape(subject))
	var resp latestSchemaResponse
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &resp); err != nil {
		return 0, nil, "", err
	}

	parsed, err := hambaavro.Parse(resp.Schema)
	if err != nil {
		return 0, nil, "", fmt.Errorf("avro: failed to parse latest schema for %q: %w", subject, err)
	}

	c.mu.Lock()
	c.subjectSchemaToID[subjectCacheKey(subject, resp.Schema)] = resp.ID
	c.idToSchema[resp.ID] = parsed
	c.idToRaw[resp.ID] = resp.Schema
	c.mu.Unlock()

	return resp.ID, parsed, resp.Schema, nil
}

// CheckCompatibility checks whether schemaStr is compatible with the latest version of subject.
func (c *Client) CheckCompatibility(ctx context.Context, subject, schemaStr string) (bool, error) {
	if strings.TrimSpace(subject) == "" {
		return false, errors.New("avro: subject must not be empty")
	}
	if _, err := hambaavro.Parse(schemaStr); err != nil {
		return false, fmt.Errorf("avro: invalid avro schema: %w", err)
	}

	endpoint := fmt.Sprintf("%s/compatibility/subjects/%s/versions/latest", c.baseURL, url.PathEscape(subject))
	var resp compatibilityResponse
	if err := c.doJSON(ctx, http.MethodPost, endpoint, registerRequest{Schema: schemaStr}, &resp); err != nil {
		return false, err
	}
	return resp.IsCompatible, nil
}

// SetCompatibility sets the compatibility level (e.g. "BACKWARD") globally (if subject=="")
// or for a specific subject.
func (c *Client) SetCompatibility(ctx context.Context, subject, level string) error {
	if strings.TrimSpace(level) == "" {
		return errors.New("avro: compatibility level must not be empty")
	}
	endpoint := fmt.Sprintf("%s/config", c.baseURL)
	if strings.TrimSpace(subject) != "" {
		endpoint = fmt.Sprintf("%s/config/%s", c.baseURL, url.PathEscape(subject))
	}
	return c.doJSON(ctx, http.MethodPut, endpoint, configRequest{Compatibility: level}, nil)
}

func (c *Client) doJSON(ctx context.Context, method, endpoint string, reqBody, respBody any) error {
	var bodyReader io.Reader
	if reqBody != nil {
		raw, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("avro: marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, bodyReader)
	if err != nil {
		return fmt.Errorf("avro: build request: %w", err)
	}
	req.Header.Set("Accept", schemaRegistryContentType)
	if reqBody != nil {
		req.Header.Set("Content-Type", schemaRegistryContentType)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("avro: %s %s: %w", method, endpoint, err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("avro: read response from %s: %w", endpoint, err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		regErr := &RegistryError{StatusCode: resp.StatusCode}
		_ = json.Unmarshal(respBytes, regErr)
		if regErr.Message == "" && len(respBytes) > 0 {
			regErr.Message = strings.TrimSpace(string(respBytes))
		}
		return regErr
	}

	if respBody != nil && len(respBytes) > 0 {
		if err := json.Unmarshal(respBytes, respBody); err != nil {
			return fmt.Errorf("avro: decode response from %s: %w", endpoint, err)
		}
	}
	return nil
}

// Codec encodes and decodes Avro messages using Confluent wire framing and a Schema Registry Client.
type Codec struct {
	client *Client
}

// NewCodec creates a Codec backed by client.
func NewCodec(client *Client) *Codec {
	return &Codec{client: client}
}

// Encode ensures schemaStr is registered under subject (using Client's cache),
// serializes v to Avro binary, and wraps it with the 5-byte Confluent wire header.
func (c *Codec) Encode(ctx context.Context, subject, schemaStr string, v any) ([]byte, error) {
	if c.client == nil {
		return nil, errors.New("avro: codec client is nil")
	}
	id, err := c.client.RegisterSchema(ctx, subject, schemaStr)
	if err != nil {
		return nil, err
	}
	schema, _, err := c.client.GetSchemaByID(ctx, id)
	if err != nil {
		return nil, err
	}
	return c.EncodeWithSchema(id, schema, v)
}

// EncodeWithSchema serializes v using a pre-parsed schema and schemaID without network calls.
func (c *Codec) EncodeWithSchema(schemaID int, schema Schema, v any) ([]byte, error) {
	if schema == nil {
		return nil, errors.New("avro: schema must not be nil")
	}
	payload, err := hambaavro.Marshal(schema, v)
	if err != nil {
		return nil, fmt.Errorf("avro: marshal payload: %w", err)
	}
	return Frame(schemaID, payload)
}

// Decode unframes wire, resolves the schema ID via the Schema Registry Client (cached),
// and unmarshals the Avro binary payload into target. It returns the schema ID from the header.
func (c *Codec) Decode(ctx context.Context, wire []byte, target any) (int, error) {
	if c.client == nil {
		return 0, errors.New("avro: codec client is nil")
	}
	id, payload, err := Unframe(wire)
	if err != nil {
		return 0, err
	}
	schema, _, err := c.client.GetSchemaByID(ctx, id)
	if err != nil {
		return 0, err
	}
	if err := hambaavro.Unmarshal(schema, payload, target); err != nil {
		return 0, fmt.Errorf("avro: unmarshal payload (schema id %d): %w", id, err)
	}
	return id, nil
}

// DecodeWithSchema unframes wire and unmarshals the payload using the provided schema.
func (c *Codec) DecodeWithSchema(schema Schema, wire []byte, target any) (int, error) {
	if schema == nil {
		return 0, errors.New("avro: schema must not be nil")
	}
	id, payload, err := Unframe(wire)
	if err != nil {
		return 0, err
	}
	if err := hambaavro.Unmarshal(schema, payload, target); err != nil {
		return 0, fmt.Errorf("avro: unmarshal payload (schema id %d): %w", id, err)
	}
	return id, nil
}
