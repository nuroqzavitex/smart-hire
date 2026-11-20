//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/opensearch"
	"github.com/nuroqzavitex/smarthire/services/job/internal/projection"
)

// osDo sends a raw request to the shared test cluster.
func osDo(t *testing.T, method, path, body string) (int, []byte) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, osURL+path, rd)
	require.NoError(t, err)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, b
}

// resetIndices deletes every jobs_v* index and recreates jobs_v1 + aliases.
func resetIndices(t *testing.T) *opensearch.Client {
	t.Helper()
	status, body := osDo(t, http.MethodGet, "/_cat/indices/jobs_v*?h=index", "")
	if status == http.StatusOK {
		for _, idx := range strings.Fields(string(body)) {
			osDo(t, http.MethodDelete, "/"+idx, "")
		}
	}
	c, err := opensearch.New(osURL)
	require.NoError(t, err)
	require.NoError(t, c.EnsureIndex(context.Background()))
	return c
}

func refresh(t *testing.T) {
	t.Helper()
	status, _ := osDo(t, http.MethodPost, "/"+opensearch.ReadAlias+"/_refresh", "")
	require.Equal(t, http.StatusOK, status)
}

// getDoc returns the stored source and _version of job id in index.
func getDoc(t *testing.T, index, id string) (map[string]any, int64) {
	t.Helper()
	status, body := osDo(t, http.MethodGet, "/"+index+"/_doc/"+id, "")
	require.Equal(t, http.StatusOK, status, string(body))
	var r struct {
		Version int64          `json:"_version"`
		Source  map[string]any `json:"_source"`
	}
	require.NoError(t, json.Unmarshal(body, &r))
	return r.Source, r.Version
}

const pid = "5f0c8a1e-2b3d-4e5f-8a9b-0c1d2e3f4a5b"

func doc(version int64, title string) projection.Document {
	return projection.Document{JobID: pid, Version: version, Status: "open", Title: title, Location: "Hà Nội"}
}

func TestPut_DuplicateIsNoop(t *testing.T) {
	c := resetIndices(t)
	ctx := context.Background()
	applied, err := c.Put(ctx, "jobs_v1", doc(1, "first"))
	require.NoError(t, err)
	assert.True(t, applied)
	applied, err = c.Put(ctx, "jobs_v1", doc(1, "first"))
	require.NoError(t, err)
	assert.False(t, applied)
	_, v := getDoc(t, "jobs_v1", pid)
	assert.Equal(t, int64(1), v)
}

func TestPut_OutOfOrderKeepsNewest(t *testing.T) {
	c := resetIndices(t)
	ctx := context.Background()
	_, err := c.Put(ctx, "jobs_v1", doc(3, "third"))
	require.NoError(t, err)
	applied, err := c.Put(ctx, "jobs_v1", doc(2, "second"))
	require.NoError(t, err)
	assert.False(t, applied)
	src, v := getDoc(t, "jobs_v1", pid)
	assert.Equal(t, int64(3), v)
	assert.Equal(t, "third", src["title"])
}

func TestPut_OldUpdateAfterTombstone(t *testing.T) {
	c := resetIndices(t)
	ctx := context.Background()
	_, err := c.Put(ctx, "jobs_v1", projection.Document{JobID: pid, Version: 4, Status: projection.StatusDeleted})
	require.NoError(t, err)
	applied, err := c.Put(ctx, "jobs_v1", doc(3, "resurrected?"))
	require.NoError(t, err)
	assert.False(t, applied)
	src, _ := getDoc(t, "jobs_v1", pid)
	assert.Equal(t, projection.StatusDeleted, src["status"])
	assert.NotContains(t, src, "title")
}

func TestEnsureIndex_Idempotent(t *testing.T) {
	c := resetIndices(t)
	require.NoError(t, c.EnsureIndex(context.Background()))
	_, body := osDo(t, http.MethodGet, "/_cat/aliases/jobs*?h=alias,index", "")
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	for i := range lines {
		lines[i] = strings.Join(strings.Fields(lines[i]), " ")
	}
	sort.Strings(lines)
	assert.Equal(t, []string{"jobs jobs_v1", "jobs_write jobs_v1"}, lines)
	indices, err := c.WriteIndices(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"jobs_v1"}, indices)
}
