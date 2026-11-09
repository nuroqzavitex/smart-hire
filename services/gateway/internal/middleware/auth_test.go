package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/authz/authztest"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/middleware"
)

func newVerifier(t *testing.T) (*authz.Verifier, *authztest.KeyIssuer) {
	t.Helper()
	is := authztest.NewIssuer(t)
	v, err := authz.NewVerifier(t.Context(), is.JWKSURL(), authztest.Issuer, authztest.Audience)
	require.NoError(t, err)
	t.Cleanup(v.Close)
	return v, is
}

func serve(h http.Handler, authHeader string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", "/v1/me", nil)
	if authHeader != "" {
		r.Header.Set("Authorization", authHeader)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestOptionalAuth(t *testing.T) {
	v, is := newVerifier(t)
	var called, ok bool
	var claims authz.Claims
	h := middleware.OptionalAuth(v)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		called = true
		claims, ok = authz.ClaimsFrom(r.Context())
	}))

	t.Run("no header passes through without claims", func(t *testing.T) {
		called, ok = false, false
		w := serve(h, "")
		assert.True(t, called)
		assert.False(t, ok)
		assert.Equal(t, 200, w.Code)
	})
	t.Run("valid token sets claims", func(t *testing.T) {
		called, ok = false, false
		w := serve(h, "Bearer "+is.Sign(t, "user-1", authz.RoleCandidate, time.Minute))
		assert.True(t, called)
		require.True(t, ok)
		assert.Equal(t, "user-1", claims.Subject)
		assert.Equal(t, 200, w.Code)
	})
	// invalid credentials continue without claims; RejectInvalidAuth answers 401
	full := middleware.OptionalAuth(v)(middleware.RejectInvalidAuth(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		called = true
		_, ok = authz.ClaimsFrom(r.Context())
	})))
	for name, hdr := range map[string]string{
		"invalid token":     "Bearer x",
		"expired token":     "Bearer " + is.Sign(t, "user-1", authz.RoleCandidate, -time.Minute),
		"non-bearer scheme": "Basic abc",
	} {
		t.Run(name+" continues without claims", func(t *testing.T) {
			called, ok = false, true
			w := serve(h, hdr)
			assert.True(t, called)
			assert.False(t, ok)
			assert.Equal(t, 200, w.Code)
		})
		t.Run(name+" is 401 via RejectInvalidAuth", func(t *testing.T) {
			called = false
			w := serve(full, hdr)
			assert.False(t, called)
			assert.Equal(t, 401, w.Code)
			assert.Contains(t, w.Body.String(), `"UNAUTHENTICATED"`)
		})
	}
	t.Run("RejectInvalidAuth passes valid and anonymous", func(t *testing.T) {
		called = false
		assert.Equal(t, 200, serve(full, "Bearer "+is.Sign(t, "user-1", authz.RoleCandidate, time.Minute)).Code)
		assert.True(t, called)
		called = false
		assert.Equal(t, 200, serve(full, "").Code)
		assert.True(t, called)
	})
}

func TestRequireAuth(t *testing.T) {
	var called bool
	h := middleware.RequireAuth(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))

	w := serve(h, "")
	assert.False(t, called)
	assert.Equal(t, 401, w.Code)
	assert.Contains(t, w.Body.String(), `"UNAUTHENTICATED"`)

	r := httptest.NewRequest("GET", "/", nil)
	r = r.WithContext(authz.WithClaims(r.Context(), authz.Claims{Subject: "u"}))
	h.ServeHTTP(httptest.NewRecorder(), r)
	assert.True(t, called)
}
