package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/gateway/internal/config"
)

func TestLoad_Defaults(t *testing.T) {
	c, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, 8080, c.HTTPPort)
	assert.Equal(t, "auth:9001", c.AuthGRPCAddr)
	assert.Equal(t, []string{"http://localhost:3001"}, c.CORSOrigins)
	assert.True(t, c.CookieSecure)
	assert.Empty(t, c.TrustedProxies)
	assert.Equal(t, 100, c.RateLimitUserPerMin)
	assert.Equal(t, 10, c.RateLimitLoginPerMin)
	assert.Equal(t, "info", c.LogLevel)
}

func TestLoad_Override(t *testing.T) {
	t.Setenv("GATEWAY_HTTP_PORT", "9090")
	t.Setenv("GATEWAY_CORS_ORIGINS", "http://a.test,http://b.test")
	t.Setenv("GATEWAY_COOKIE_SECURE", "false")
	t.Setenv("HTTP_PORT", "1") // unprefixed must be ignored
	c, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, 9090, c.HTTPPort)
	assert.Equal(t, []string{"http://a.test", "http://b.test"}, c.CORSOrigins)
	assert.False(t, c.CookieSecure)
}

func TestLoad_RejectsWildcardCORSOrigin(t *testing.T) {
	for _, v := range []string{"*", "http://a.test,*", "https://*.example.com"} {
		t.Setenv("GATEWAY_CORS_ORIGINS", v)
		_, err := config.Load()
		require.Error(t, err, v)
		assert.Contains(t, err.Error(), "GATEWAY_CORS_ORIGINS", v)
	}
}
