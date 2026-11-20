package config_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/candidate/internal/config"
)

func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("CANDIDATE_DB_DSN", "postgres://x")
	t.Setenv("CANDIDATE_S3_ACCESS_KEY", "ak")
	t.Setenv("CANDIDATE_S3_SECRET_KEY", "sk")
}

func TestLoad_MatchingDefaults(t *testing.T) {
	setRequired(t)
	c, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "ai-api:9100", c.AIAddr)
	assert.Equal(t, "job:9002", c.JobAddr)
	assert.Equal(t, 2*time.Second, c.AITimeout)
	assert.Equal(t, 2*time.Second, c.JobTimeout)
	assert.Equal(t, uint32(5), c.AIBreakerFailures)
	assert.Equal(t, 30*time.Second, c.AIBreakerOpen)
}

func TestLoad_Defaults(t *testing.T) {
	setRequired(t)
	c, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, 9003, c.GRPCPort)
	assert.Equal(t, 8003, c.HTTPPort)
	assert.Equal(t, "postgres://x", c.DBDSN)
	assert.Equal(t, "http://auth:8001/.well-known/jwks.json", c.JWKSURL)
	assert.Equal(t, "smarthire-auth", c.JWTIssuer)
	assert.Equal(t, "smarthire-api", c.JWTAudience)
	assert.Equal(t, []string{"redpanda:9092"}, c.KafkaBrokers)
	assert.Equal(t, 500*time.Millisecond, c.OutboxInterval)
	assert.Equal(t, "minio:9000", c.S3Endpoint)
	assert.Equal(t, "http://127.0.0.1:9000", c.S3PublicEndpoint)
	assert.Equal(t, "ak", c.S3AccessKey)
	assert.Equal(t, "sk", c.S3SecretKey)
	assert.Equal(t, "cvs", c.S3Bucket)
	assert.Equal(t, "us-east-1", c.S3Region)
	assert.False(t, c.S3UseSSL)
	assert.Equal(t, 10*time.Minute, c.UploadURLTTL)
	assert.Equal(t, "info", c.LogLevel)
}

func TestLoad_Overrides(t *testing.T) {
	setRequired(t)
	t.Setenv("CANDIDATE_KAFKA_BROKERS", "a:1,b:2")
	t.Setenv("CANDIDATE_UPLOAD_URL_TTL", "5m")
	t.Setenv("CANDIDATE_S3_USE_SSL", "true")
	c, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, []string{"a:1", "b:2"}, c.KafkaBrokers)
	assert.Equal(t, 5*time.Minute, c.UploadURLTTL)
	assert.True(t, c.S3UseSSL)
}

func TestLoad_RequiresDSN(t *testing.T) {
	t.Setenv("CANDIDATE_S3_ACCESS_KEY", "ak")
	t.Setenv("CANDIDATE_S3_SECRET_KEY", "sk")
	t.Setenv("DB_DSN", "postgres://unprefixed") // unprefixed must not satisfy
	_, err := config.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CANDIDATE_DB_DSN")
}

func TestLoad_RequiresS3Credentials(t *testing.T) {
	for _, missing := range []string{"CANDIDATE_S3_ACCESS_KEY", "CANDIDATE_S3_SECRET_KEY"} {
		t.Run(missing+"/unset", func(t *testing.T) {
			setRequired(t)
			require.NoError(t, os.Unsetenv(missing)) // t.Setenv restores it
			_, err := config.Load()
			require.Error(t, err)
			assert.Contains(t, err.Error(), missing)
		})
		t.Run(missing+"/empty", func(t *testing.T) {
			setRequired(t)
			t.Setenv(missing, "")
			_, err := config.Load()
			require.Error(t, err)
			assert.Contains(t, err.Error(), missing)
		})
	}
	// Unprefixed credentials never satisfy the config.
	t.Setenv("CANDIDATE_DB_DSN", "postgres://x")
	t.Setenv("S3_ACCESS_KEY", "ak")
	t.Setenv("S3_SECRET_KEY", "sk")
	_, err := config.Load()
	require.Error(t, err)
}
