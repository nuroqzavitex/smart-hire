package httpx_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/httpx"
)

func get(h http.Handler, path string) int {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

func TestEndpoints_OK(t *testing.T) {
	h := httpx.NewHandler(func(context.Context) error { return nil })
	assert.Equal(t, http.StatusOK, get(h, "/healthz"))
	assert.Equal(t, http.StatusOK, get(h, "/readyz"))
	assert.Equal(t, http.StatusNotFound, get(h, "/nope"))
}

func TestReadyz_503WhenDBDown(t *testing.T) {
	h := httpx.NewHandler(func(context.Context) error { return errors.New("db down") })
	assert.Equal(t, http.StatusServiceUnavailable, get(h, "/readyz"))
	assert.Equal(t, http.StatusOK, get(h, "/healthz"))
}
