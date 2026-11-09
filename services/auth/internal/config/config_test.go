package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/auth/internal/config"
)

func TestLoad_DefaultsAndRequired(t *testing.T) {
	_, err := config.Load()
	require.Error(t, err)

	t.Setenv("AUTH_DB_DSN", "postgres://x")
	t.Setenv("AUTH_JWT_PRIVATE_KEY_PATH", "/k.pem")
	t.Setenv("AUTH_JWT_EXTRA_PUBLIC_KEY_PATHS", "/a.pem,/b.pem")
	c, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, 9001, c.GRPCPort)
	assert.Equal(t, 8001, c.HTTPPort)
	assert.Equal(t, 15*time.Minute, c.AccessTTL)
	assert.Equal(t, 168*time.Hour, c.RefreshTTL)
	assert.Equal(t, "smarthire-auth", c.JWTIssuer)
	assert.Equal(t, []string{"/a.pem", "/b.pem"}, c.JWTExtraPublicKeyPaths)
	assert.Equal(t, uint32(19456), c.Argon2Params().Memory)
}

func TestLoadSeed_Required(t *testing.T) {
	_, err := config.LoadSeed()
	require.Error(t, err)
	t.Setenv("SEED_ADMIN_EMAIL", "admin@smarthire.local")
	t.Setenv("SEED_ADMIN_PASSWORD", "x")
	s, err := config.LoadSeed()
	require.NoError(t, err)
	assert.Equal(t, "admin@smarthire.local", s.AdminEmail)
}
