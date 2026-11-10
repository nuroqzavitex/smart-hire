package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/job/internal/config"
)

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("JOB_DB_DSN", "postgres://x")
	c, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, 9002, c.GRPCPort)
	assert.Equal(t, 8002, c.HTTPPort)
	assert.Equal(t, []string{"redpanda:9092"}, c.KafkaBrokers)
	assert.Equal(t, 500*time.Millisecond, c.OutboxInterval)
	assert.Equal(t, "smarthire-auth", c.JWTIssuer)
	assert.Equal(t, "smarthire-api", c.JWTAudience)
	assert.Equal(t, "http://auth:8001/.well-known/jwks.json", c.JWKSURL)
	assert.Equal(t, "info", c.LogLevel)
}

func TestLoad_Overrides(t *testing.T) {
	t.Setenv("JOB_DB_DSN", "postgres://x")
	t.Setenv("JOB_KAFKA_BROKERS", "a:1,b:2")
	c, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, []string{"a:1", "b:2"}, c.KafkaBrokers)
}

func TestLoad_RequiresDSN(t *testing.T) {
	t.Setenv("DB_DSN", "postgres://unprefixed") // unprefixed must not satisfy
	_, err := config.Load()
	require.Error(t, err)
}
