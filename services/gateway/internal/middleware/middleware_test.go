package middleware_test

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/logger"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/middleware"
)

func TestRequestID_GeneratesAndAccepts(t *testing.T) {
	var seen string
	h := middleware.RequestID(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = logger.RequestIDFrom(r.Context())
	}))
	do := func(hdr string) (string, string) {
		r := httptest.NewRequest("GET", "/", nil)
		if hdr != "" {
			r.Header.Set("X-Request-ID", hdr)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec.Header().Get("X-Request-ID"), seen
	}
	got, ctxID := do("")
	assert.Len(t, got, 26)
	assert.Equal(t, got, ctxID)

	got, ctxID = do("abc-1")
	assert.Equal(t, "abc-1", got)
	assert.Equal(t, "abc-1", ctxID)

	got, _ = do("bad id!")
	assert.Len(t, got, 26)
	got, _ = do(string(bytes.Repeat([]byte("a"), 65)))
	assert.Len(t, got, 26)
}

func TestRecovery_Returns500Envelope(t *testing.T) {
	var buf bytes.Buffer
	log := logger.New("gateway", &buf, slog.LevelInfo)
	h := middleware.RequestID(middleware.Recovery(log)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	assert.Equal(t, 500, rec.Code)
	assert.Contains(t, rec.Body.String(), `"INTERNAL"`)
	assert.NotContains(t, rec.Body.String(), "boom")
	assert.Contains(t, buf.String(), "boom")
}

func TestLogging_LogsWithoutQuery(t *testing.T) {
	var buf bytes.Buffer
	log := logger.New("gateway", &buf, slog.LevelInfo)
	h := middleware.Logging(log)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/x?token=secret", nil))
	require.Equal(t, http.StatusTeapot, rec.Code)
	out := buf.String()
	assert.Contains(t, out, `"path":"/v1/x"`)
	assert.Contains(t, out, `"status":418`)
	assert.Contains(t, out, `"method":"GET"`)
	assert.Contains(t, out, "duration_ms")
	assert.NotContains(t, out, "secret")
}
