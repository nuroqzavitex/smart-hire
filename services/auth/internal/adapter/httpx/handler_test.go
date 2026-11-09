package httpx_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/crypto"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/httpx"
)

func keys(t *testing.T) *crypto.KeySet {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der, err := x509.MarshalPKCS8PrivateKey(k)
	require.NoError(t, err)
	p := filepath.Join(t.TempDir(), "k.pem")
	require.NoError(t, os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600))
	ks, err := crypto.LoadKeys(p)
	require.NoError(t, err)
	return ks
}

func get(h http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestEndpoints_OK(t *testing.T) {
	h := httpx.NewHandler(keys(t), func(context.Context) error { return nil })
	assert.Equal(t, http.StatusOK, get(h, "/healthz").Code)
	assert.Equal(t, http.StatusOK, get(h, "/readyz").Code)
	rec := get(h, "/.well-known/jwks.json")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"keys"`)
	assert.Equal(t, http.StatusNotFound, get(h, "/nope").Code)
}

func TestReadyz_503WhenDBDown(t *testing.T) {
	h := httpx.NewHandler(keys(t), func(context.Context) error { return errors.New("db down") })
	assert.Equal(t, http.StatusServiceUnavailable, get(h, "/readyz").Code)
	assert.Equal(t, http.StatusOK, get(h, "/healthz").Code)
}
