package authz_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/authz/authztest"
)

func TestNewVerifierWithRetry_SucceedsWhenAvailable(t *testing.T) {
	is := authztest.NewIssuer(t)
	v, err := authz.NewVerifierWithRetry(t.Context(), is.JWKSURL(), authztest.Issuer, authztest.Audience,
		5*time.Second, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(v.Close)
	c, err := v.Verify(is.Sign(t, "u1", authz.RoleRecruiter, time.Minute))
	require.NoError(t, err)
	assert.Equal(t, "u1", c.Subject)
}

func TestNewVerifierWithRetry_GivesUpAfterMaxWait(t *testing.T) {
	start := time.Now()
	_, err := authz.NewVerifierWithRetry(t.Context(), "http://127.0.0.1:1/jwks", authztest.Issuer, authztest.Audience,
		1500*time.Millisecond, slog.New(slog.DiscardHandler))
	require.Error(t, err)
	assert.Less(t, time.Since(start), 4*time.Second)
}

func TestNewVerifierWithRetry_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := authz.NewVerifierWithRetry(ctx, "http://127.0.0.1:1/jwks", authztest.Issuer, authztest.Audience,
		time.Minute, slog.New(slog.DiscardHandler))
	require.Error(t, err)
}
