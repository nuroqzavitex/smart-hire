// Package config defines the candidate service configuration (env prefix
// CANDIDATE).
package config

import (
	"time"

	"github.com/nuroqzavitex/smarthire/pkg/config"
)

// EnvPrefix is the environment variable prefix of the candidate service.
const EnvPrefix = "CANDIDATE"

// Config is the server configuration.
type Config struct {
	GRPCPort       int           `env:"GRPC_PORT" envDefault:"9003"`
	HTTPPort       int           `env:"HTTP_PORT" envDefault:"8003"`
	DBDSN          string        `env:"DB_DSN,required"`
	JWKSURL        string        `env:"JWKS_URL" envDefault:"http://auth:8001/.well-known/jwks.json"`
	JWTIssuer      string        `env:"JWT_ISSUER" envDefault:"smarthire-auth"`
	JWTAudience    string        `env:"JWT_AUDIENCE" envDefault:"smarthire-api"`
	KafkaBrokers   []string      `env:"KAFKA_BROKERS" envDefault:"redpanda:9092" envSeparator:","`
	OutboxInterval time.Duration `env:"OUTBOX_INTERVAL" envDefault:"500ms"`
	// S3Endpoint is the in-network MinIO host:port for server-side calls;
	// S3PublicEndpoint (scheme://host[:port]) is what clients reach and is
	// used only to sign presigned upload URLs.
	S3Endpoint       string        `env:"S3_ENDPOINT" envDefault:"minio:9000"`
	S3PublicEndpoint string        `env:"S3_PUBLIC_ENDPOINT" envDefault:"http://127.0.0.1:9000"`
	S3AccessKey      string        `env:"S3_ACCESS_KEY,required,notEmpty"`
	S3SecretKey      string        `env:"S3_SECRET_KEY,required,notEmpty"`
	S3Bucket         string        `env:"S3_BUCKET" envDefault:"cvs"`
	S3Region         string        `env:"S3_REGION" envDefault:"us-east-1"`
	S3UseSSL         bool          `env:"S3_USE_SSL" envDefault:"false"`
	UploadURLTTL     time.Duration `env:"UPLOAD_URL_TTL" envDefault:"10m"`
	LogLevel         string        `env:"LOG_LEVEL" envDefault:"info"`

	// Job recommendations (ADR 0010): the AI service ranks, the job service
	// supplies job details. Either being down only fails that endpoint (503).
	AIAddr            string        `env:"AI_ADDR" envDefault:"ai-api:9100"`
	AITimeout         time.Duration `env:"AI_TIMEOUT" envDefault:"2s"`
	AIBreakerFailures uint32        `env:"AI_BREAKER_FAILURES" envDefault:"5"`
	AIBreakerOpen     time.Duration `env:"AI_BREAKER_OPEN" envDefault:"30s"`
	JobAddr           string        `env:"JOB_ADDR" envDefault:"job:9002"`
	JobTimeout        time.Duration `env:"JOB_TIMEOUT" envDefault:"2s"`
}

// Load reads the server configuration from CANDIDATE_* variables.
func Load() (Config, error) {
	var c Config
	if err := config.Load(EnvPrefix, &c); err != nil {
		return Config{}, err
	}
	return c, nil
}
