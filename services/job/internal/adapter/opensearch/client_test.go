package opensearch_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func TestTransient_Classification(t *testing.T) {
	for _, c := range []struct {
		err  error
		want bool
	}{
		{errors.New("dial tcp: connection refused"), true},
		{context.DeadlineExceeded, true},
		{&opensearch.Error{Op: "index document", Status: 503}, true},
		{&opensearch.Error{Op: "index document", Status: 429}, true},
		{&opensearch.Error{Op: "index document", Status: 404, Type: "index_not_found_exception"}, true},
		{&opensearch.Error{Op: "index document", Status: 400, Type: "mapper_parsing_exception"}, false},
		{fmt.Errorf("index job document: %w", &opensearch.Error{Status: 400}), false},
	} {
		assert.Equal(t, c.want, opensearch.Transient(c.err), "%v", c.err)
	}
}
