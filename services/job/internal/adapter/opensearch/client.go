// Package opensearch is the job read model adapter: index/alias management,
// versioned document writes and search (ADR 0009).
package opensearch

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"

	osgo "github.com/opensearch-project/opensearch-go/v4"

	"github.com/nuroqzavitex/smarthire/services/job/internal/projection"
)

// Index and alias names. Searches read ReadAlias; the projector writes every
// index behind WriteAlias (two of them while a rebuild runs).
const (
	ReadAlias   = "jobs"
	WriteAlias  = "jobs_write"
	IndexPrefix = "jobs_v"
)

//go:embed index.json
var indexDefinition []byte

// Client talks to OpenSearch over its REST API.
type Client struct{ os *osgo.Client }

// New builds a client for the cluster at baseURL. Client-side retries are
// off: the projector retries through the Kafka consumer and searches go
// through the circuit breaker.
func New(baseURL string) (*Client, error) {
	c, err := osgo.NewClient(osgo.Config{Addresses: []string{baseURL}, DisableRetry: true})
	if err != nil {
		return nil, fmt.Errorf("opensearch client: %w", err)
	}
	return &Client{os: c}, nil
}

// Error is a non-success OpenSearch response. It carries only the status and
// OpenSearch's error type, never the response body (it can echo documents).
type Error struct {
	Op     string
	Status int
	Type   string
}

func (e *Error) Error() string {
	if e.Type == "" {
		return fmt.Sprintf("opensearch %s: status %d", e.Op, e.Status)
	}
	return fmt.Sprintf("opensearch %s: status %d (%s)", e.Op, e.Status, e.Type)
}

// do sends one request and returns the status and body. Only transport
// failures are errors here; callers judge the status.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, body any) (int, []byte, error) {
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case []byte:
		rd = bytes.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			return 0, nil, fmt.Errorf("encode request: %w", err)
		}
		rd = bytes.NewReader(raw)
	}
	u := path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return 0, nil, err
	}
	if rd != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.os.Stream(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("read response: %w", err)
	}
	return resp.StatusCode, raw, nil
}

func newError(op string, status int, body []byte) error {
	var e struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &e)
	return &Error{Op: op, Status: status, Type: e.Error.Type}
}

// errorType returns the OpenSearch error type of err, if it is an *Error.
func errorType(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Type
	}
	return ""
}

// EnsureIndex creates jobs_v1 with both aliases when the read alias does not
// exist yet. Concurrent callers are safe: "already exists" counts as done.
func (c *Client) EnsureIndex(ctx context.Context) error {
	status, body, err := c.do(ctx, http.MethodHead, "/_alias/"+ReadAlias, nil, nil)
	if err != nil {
		return err
	}
	switch status {
	case http.StatusOK:
		return nil
	case http.StatusNotFound:
	default:
		return newError("check alias", status, body)
	}
	err = c.createIndex(ctx, IndexPrefix+"1", ReadAlias, WriteAlias)
	if errorType(err) == "resource_already_exists_exception" {
		return nil
	}
	return err
}

// createIndex creates name from index.json, attached to the given aliases.
func (c *Client) createIndex(ctx context.Context, name string, aliases ...string) error {
	var def map[string]any
	if err := json.Unmarshal(indexDefinition, &def); err != nil {
		return fmt.Errorf("index definition: %w", err)
	}
	if len(aliases) > 0 {
		al := map[string]any{}
		for _, a := range aliases {
			al[a] = map[string]any{}
		}
		def["aliases"] = al
	}
	status, body, err := c.do(ctx, http.MethodPut, "/"+name, nil, def)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return newError("create index", status, body)
	}
	return nil
}

// aliasIndices lists the indices behind alias, sorted.
func (c *Client) aliasIndices(ctx context.Context, alias string) ([]string, error) {
	status, body, err := c.do(ctx, http.MethodGet, "/_alias/"+alias, nil, nil)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return nil, nil
	}
	if status != http.StatusOK {
		return nil, newError("get alias", status, body)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("decode alias: %w", err)
	}
	out := make([]string, 0, len(m))
	for idx := range m {
		out = append(out, idx)
	}
	sort.Strings(out)
	return out, nil
}

// WriteIndices implements projection.Store.
func (c *Client) WriteIndices(ctx context.Context) ([]string, error) {
	return c.aliasIndices(ctx, WriteAlias)
}

// Put implements projection.Store: it indexes the full document with
// external version d.Version. 409 means the index already has that version
// or a newer one.
func (c *Client) Put(ctx context.Context, index string, d projection.Document) (bool, error) {
	q := url.Values{"version": {strconv.FormatInt(d.Version, 10)}, "version_type": {"external"}}
	status, body, err := c.do(ctx, http.MethodPut, "/"+index+"/_doc/"+url.PathEscape(d.JobID), q, d)
	if err != nil {
		return false, err
	}
	switch status {
	case http.StatusOK, http.StatusCreated:
		return true, nil
	case http.StatusConflict:
		return false, nil
	}
	return false, newError("index document", status, body)
}
