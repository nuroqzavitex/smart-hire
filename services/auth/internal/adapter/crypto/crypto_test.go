package crypto_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alexedwards/argon2id"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/crypto"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

func newKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return k
}

func writePEM(t *testing.T, typ string, der []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "key.pem")
	require.NoError(t, os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600))
	return p
}

func privPath(t *testing.T, k *rsa.PrivateKey) string {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(k)
	require.NoError(t, err)
	return writePEM(t, "PRIVATE KEY", der)
}

func pubPath(t *testing.T, k *rsa.PrivateKey) string {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(&k.PublicKey)
	require.NoError(t, err)
	return writePEM(t, "PUBLIC KEY", der)
}

func genKeySet(t *testing.T) *crypto.KeySet {
	t.Helper()
	ks, err := crypto.LoadKeys(privPath(t, newKey(t)))
	require.NoError(t, err)
	return ks
}

func TestIssuer_TokenVerifiesAgainstJWKS(t *testing.T) {
	keys := genKeySet(t)
	iss := crypto.NewIssuer(keys, "smarthire-auth", "smarthire-api", 15*time.Minute)
	srv := httptest.NewServer(crypto.JWKSHandler(keys))
	t.Cleanup(srv.Close)
	uid := uuid.New()

	tok, ttl, err := iss.IssueAccess(domain.User{ID: uid, Role: authz.RoleRecruiter}, time.Now())
	require.NoError(t, err)
	assert.Equal(t, 15*time.Minute, ttl)

	v, err := authz.NewVerifier(context.Background(), srv.URL, "smarthire-auth", "smarthire-api")
	require.NoError(t, err)
	t.Cleanup(v.Close)
	c, err := v.Verify(tok)
	require.NoError(t, err)
	assert.Equal(t, uid.String(), c.Subject)
	assert.Equal(t, authz.RoleRecruiter, c.Role)
	assert.NotEmpty(t, c.TokenID)
}

func TestJWKS_IncludesExtraKeys(t *testing.T) {
	cur, old := newKey(t), newKey(t)
	keys, err := crypto.LoadKeys(privPath(t, cur), pubPath(t, old))
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	crypto.JWKSHandler(keys).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "public, max-age=300", rec.Header().Get("Cache-Control"))

	var body struct {
		Keys []struct {
			Kid string `json:"kid"`
			Kty string `json:"kty"`
			Alg string `json:"alg"`
		} `json:"keys"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Keys, 2)
	assert.NotEqual(t, body.Keys[0].Kid, body.Keys[1].Kid)
	assert.Len(t, body.Keys[0].Kid, 16)
	assert.Equal(t, "RSA", body.Keys[0].Kty)
	assert.Equal(t, "RS256", body.Keys[0].Alg)
	assert.NotContains(t, rec.Body.String(), `"d"`)
}

func TestLoadKeys_Errors(t *testing.T) {
	_, err := crypto.LoadKeys(filepath.Join(t.TempDir(), "missing.pem"))
	assert.Error(t, err)
	bad := filepath.Join(t.TempDir(), "bad.pem")
	require.NoError(t, os.WriteFile(bad, []byte("not pem"), 0o600))
	_, err = crypto.LoadKeys(bad)
	assert.Error(t, err)
	_, err = crypto.LoadKeys(privPath(t, newKey(t)), bad)
	assert.Error(t, err)
}

func TestHasher_RoundTrip(t *testing.T) {
	h := crypto.NewHasher(argon2id.Params{Memory: 19456, Iterations: 2, Parallelism: 1, SaltLength: 16, KeyLength: 32})
	hash, err := h.Hash("Password123!")
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(hash, "$argon2id$"))

	ok, err := h.Verify("Password123!", hash)
	require.NoError(t, err)
	assert.True(t, ok)
	ok, err = h.Verify("wrong", hash)
	require.NoError(t, err)
	assert.False(t, ok)
}
