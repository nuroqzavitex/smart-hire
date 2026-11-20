package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/services/job/internal/projection"
)

// Indices implements projection.IndexAdmin.
func (c *Client) Indices(ctx context.Context) ([]string, error) {
	status, body, err := c.do(ctx, http.MethodGet, "/_cat/indices/"+IndexPrefix+"*", url.Values{"h": {"index"}, "format": {"json"}}, nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, newError("list indices", status, body)
	}
	var rows []struct {
		Index string `json:"index"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("decode indices: %w", err)
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if strings.HasPrefix(r.Index, IndexPrefix) {
			out = append(out, r.Index)
		}
	}
	sort.Strings(out)
	return out, nil
}

// CreateIndex implements projection.IndexAdmin (no aliases).
func (c *Client) CreateIndex(ctx context.Context, name string) error {
	return c.createIndex(ctx, name)
}

// AddToWriteAlias implements projection.IndexAdmin.
func (c *Client) AddToWriteAlias(ctx context.Context, index string) error {
	return c.aliases(ctx, []any{map[string]any{"add": map[string]any{"index": index, "alias": WriteAlias}}})
}

// SwapAliases implements projection.IndexAdmin in one atomic _aliases call.
func (c *Client) SwapAliases(ctx context.Context, newIndex string, old []string) error {
	actions := []any{
		map[string]any{"add": map[string]any{"index": newIndex, "alias": ReadAlias}},
		map[string]any{"add": map[string]any{"index": newIndex, "alias": WriteAlias}},
	}
	for _, o := range old {
		actions = append(actions,
			map[string]any{"remove": map[string]any{"index": o, "alias": ReadAlias, "must_exist": false}},
			map[string]any{"remove": map[string]any{"index": o, "alias": WriteAlias, "must_exist": false}})
	}
	return c.aliases(ctx, actions)
}

func (c *Client) aliases(ctx context.Context, actions []any) error {
	status, body, err := c.do(ctx, http.MethodPost, "/_aliases", nil, map[string]any{"actions": actions})
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return newError("update aliases", status, body)
	}
	return nil
}

// DeleteIndex implements projection.IndexAdmin.
func (c *Client) DeleteIndex(ctx context.Context, name string) error {
	status, body, err := c.do(ctx, http.MethodDelete, "/"+name, nil, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusNotFound {
		return newError("delete index", status, body)
	}
	return nil
}

// BulkPut implements projection.IndexAdmin: external versions, per-item 409
// (a newer version is already there) is not an error.
func (c *Client) BulkPut(ctx context.Context, index string, docs []projection.Document) error {
	if len(docs) == 0 {
		return nil
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, d := range docs {
		meta := map[string]any{"index": map[string]any{
			"_index": index, "_id": d.JobID, "version": d.Version, "version_type": "external",
		}}
		if err := enc.Encode(meta); err != nil {
			return fmt.Errorf("encode bulk action: %w", err)
		}
		if err := enc.Encode(d); err != nil {
			return fmt.Errorf("encode bulk document: %w", err)
		}
	}
	status, body, err := c.do(ctx, http.MethodPost, "/_bulk", nil, buf.Bytes())
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return newError("bulk", status, body)
	}
	var r struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			Status int `json:"status"`
			Error  struct {
				Type string `json:"type"`
			} `json:"error"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return fmt.Errorf("decode bulk response: %w", err)
	}
	if !r.Errors {
		return nil
	}
	for _, item := range r.Items {
		for _, res := range item {
			if res.Status >= 300 && res.Status != http.StatusConflict {
				return &Error{Op: "bulk item", Status: res.Status, Type: res.Error.Type}
			}
		}
	}
	return nil
}

// OpenVersions implements projection.IndexAdmin: every open document behind
// the read alias, paged with search_after on job_id.
func (c *Client) OpenVersions(ctx context.Context) (map[uuid.UUID]int64, error) {
	out := map[uuid.UUID]int64{}
	var after []any
	for {
		req := map[string]any{
			"size": 1000, "_source": []string{"job_id", "version"},
			"query": term("status", projection.StatusOpen),
			"sort":  []any{map[string]any{"job_id": "asc"}},
		}
		if after != nil {
			req["search_after"] = after
		}
		status, body, err := c.do(ctx, http.MethodPost, "/"+ReadAlias+"/_search", nil, req)
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			return nil, newError("scan open documents", status, body)
		}
		var r struct {
			Hits struct {
				Hits []struct {
					Source projection.Document `json:"_source"`
					Sort   []any               `json:"sort"`
				} `json:"hits"`
			} `json:"hits"`
		}
		if err := json.Unmarshal(body, &r); err != nil {
			return nil, fmt.Errorf("decode open documents: %w", err)
		}
		for _, h := range r.Hits.Hits {
			id, err := uuid.Parse(h.Source.JobID)
			if err != nil {
				return nil, fmt.Errorf("read model document has invalid job_id")
			}
			out[id] = h.Source.Version
		}
		if len(r.Hits.Hits) < 1000 {
			return out, nil
		}
		after = r.Hits.Hits[len(r.Hits.Hits)-1].Sort
	}
}
