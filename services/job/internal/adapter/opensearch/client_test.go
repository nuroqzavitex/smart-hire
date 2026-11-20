package opensearch_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/opensearch"
	"github.com/nuroqzavitex/smarthire/services/job/internal/projection"
)

func TestPut_ExternalVersionAndConflict(t *testing.T) {
	var gotPath, gotQuery string
	var gotBody map[string]any
	status := http.StatusCreated
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery = r.URL.Path, r.URL.RawQuery
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c, err := opensearch.New(srv.URL)
	require.NoError(t, err)

	d := projection.Document{JobID: "j1", Version: 7, Status: "closed"}
	applied, err := c.Put(context.Background(), "jobs_v1", d)
	require.NoError(t, err)
	assert.True(t, applied)
	assert.Equal(t, "/jobs_v1/_doc/j1", gotPath)
	assert.Contains(t, gotQuery, "version=7")
	assert.Contains(t, gotQuery, "version_type=external")
	assert.Equal(t, map[string]any{"job_id": "j1", "version": float64(7), "status": "closed"}, gotBody)

	status = http.StatusConflict
	applied, err = c.Put(context.Background(), "jobs_v1", d)
	require.NoError(t, err, "409 means a newer or equal version is stored")
	assert.False(t, applied)

	status = http.StatusInternalServerError
	_, err = c.Put(context.Background(), "jobs_v1", d)
	assert.Error(t, err)
}
