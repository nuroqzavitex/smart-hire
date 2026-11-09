package authz_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/authz/authztest"
)

func newVerifier(t *testing.T, is *authztest.KeyIssuer) *authz.Verifier {
	t.Helper()
	v, err := authz.NewVerifier(context.Background(), is.JWKSURL(), authztest.Issuer, authztest.Audience)
	require.NoError(t, err)
	t.Cleanup(v.Close)
	return v
}

func TestVerify(t *testing.T) {
	is := authztest.NewIssuer(t)
	v := newVerifier(t, is)

	c, err := v.Verify(is.Sign(t, "u1", authz.RoleRecruiter, time.Minute))
	require.NoError(t, err)
	assert.Equal(t, "u1", c.Subject)
	assert.Equal(t, authz.RoleRecruiter, c.Role)
	assert.NotEmpty(t, c.TokenID)
	assert.False(t, c.ExpiresAt.IsZero())

	for name, tok := range map[string]string{
		"expired":   is.Sign(t, "u1", authz.RoleCandidate, -time.Minute),
		"wrong aud": is.Sign(t, "u1", authz.RoleCandidate, time.Minute, func(m jwt.MapClaims) { m["aud"] = "x" }),
		"wrong iss": is.Sign(t, "u1", authz.RoleCandidate, time.Minute, func(m jwt.MapClaims) { m["iss"] = "x" }),
		"no exp":    is.Sign(t, "u1", authz.RoleCandidate, time.Minute, func(m jwt.MapClaims) { delete(m, "exp") }),
		"no sub":    is.Sign(t, "", authz.RoleCandidate, time.Minute),
		"bad role":  is.Sign(t, "u1", authz.Role("root"), time.Minute),
		"other key": authztest.NewIssuer(t).Sign(t, "u1", authz.RoleCandidate, time.Minute),
		"garbage":   "abc.def.ghi",
		"empty":     "",
	} {
		_, err := v.Verify(tok)
		assert.ErrorIs(t, err, apperr.ErrUnauthenticated, name)
	}
}

func TestVerify_RejectsHS256(t *testing.T) {
	is := authztest.NewIssuer(t)
	v := newVerifier(t, is)
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "u1", "role": "admin", "iss": authztest.Issuer, "aud": authztest.Audience,
		"exp": time.Now().Add(time.Minute).Unix(),
	})
	tok.Header["kid"] = is.KID()
	raw, err := tok.SignedString([]byte("secret"))
	require.NoError(t, err)
	_, err = v.Verify(raw)
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
}

func TestVerify_RejectsAlgNone(t *testing.T) {
	is := authztest.NewIssuer(t)
	v := newVerifier(t, is)
	tok := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{
		"sub": "u1", "role": "admin", "iss": authztest.Issuer, "aud": authztest.Audience,
		"exp": time.Now().Add(time.Minute).Unix(),
	})
	tok.Header["kid"] = is.KID()
	raw, err := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
	require.NoError(t, err)
	_, err = v.Verify(raw)
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
}

func TestNewVerifier_FailsWhenJWKSUnreachable(t *testing.T) {
	is := authztest.NewIssuer(t)
	url := is.JWKSURL()
	is.Close()
	_, err := authz.NewVerifier(context.Background(), url, authztest.Issuer, authztest.Audience)
	assert.Error(t, err)
}

func TestRequireRole(t *testing.T) {
	assert.ErrorIs(t, authz.RequireRole(context.Background()), apperr.ErrUnauthenticated)
	ctx := authz.WithClaims(context.Background(), authz.Claims{Subject: "u", Role: authz.RoleCandidate})
	assert.NoError(t, authz.RequireRole(ctx))
	assert.NoError(t, authz.RequireRole(ctx, authz.RoleCandidate, authz.RoleAdmin))
	assert.ErrorIs(t, authz.RequireRole(ctx, authz.RoleRecruiter), apperr.ErrForbidden)
}

func TestClaimsFrom(t *testing.T) {
	_, ok := authz.ClaimsFrom(context.Background())
	assert.False(t, ok)
	ctx := authz.WithClaims(context.Background(), authz.Claims{Subject: "u"})
	c, ok := authz.ClaimsFrom(ctx)
	assert.True(t, ok)
	assert.Equal(t, "u", c.Subject)
}

func TestBearerToken(t *testing.T) {
	tok, ok := authz.BearerToken("bearer abc")
	assert.True(t, ok)
	assert.Equal(t, "abc", tok)
	tok, ok = authz.BearerToken("Bearer abc")
	assert.True(t, ok)
	assert.Equal(t, "abc", tok)
	_, ok = authz.BearerToken("Basic abc")
	assert.False(t, ok)
	_, ok = authz.BearerToken("Bearer ")
	assert.False(t, ok)
	_, ok = authz.BearerToken("")
	assert.False(t, ok)
}

func TestVerify_SurvivesCancelledConstructorCtx(t *testing.T) {
	is := authztest.NewIssuer(t)
	ctx, cancel := context.WithCancel(context.Background())
	v, err := authz.NewVerifier(ctx, is.JWKSURL(), authztest.Issuer, authztest.Audience)
	require.NoError(t, err)
	t.Cleanup(v.Close)
	cancel()

	c, err := v.Verify(is.Sign(t, "u1", authz.RoleAdmin, time.Minute))
	require.NoError(t, err)
	assert.Equal(t, "u1", c.Subject)
}
