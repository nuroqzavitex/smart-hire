package ratelimit_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/authz/authztest"
)

func newChainVerifier(t *testing.T) (*authz.Verifier, *authztest.KeyIssuer) {
	t.Helper()
	is := authztest.NewIssuer(t)
	v, err := authz.NewVerifier(t.Context(), is.JWKSURL(), authztest.Issuer, authztest.Audience)
	require.NoError(t, err)
	t.Cleanup(v.Close)
	return v, is
}
