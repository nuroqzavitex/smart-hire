// Package config defines the job service configuration (env prefix JOB).
package config

import (
	"time"

	"github.com/nuroqzavitex/smarthire/pkg/config"
)

// EnvPrefix is the environment variable prefix of the job service.
const EnvPrefix = "JOB"

// Config is the server configuration.
type Config struct {
	GRPCPort       int           `env:"GRPC_PORT" envDefault:"9002"`
	HTTPPort       int           `env:"HTTP_PORT" envDefault:"8002"`
	DBDSN          string        `env:"DB_DSN,required"`
	JWKSURL        string        `env:"JWKS_URL" envDefault:"http://auth:8001/.well-known/jwks.json"`
	JWTIssuer      string        `env:"JWT_ISSUER" envDefault:"smarthire-auth"`
	JWTAudience    string        `env:"JWT_AUDIENCE" envDefault:"smarthire-api"`
	KafkaBrokers   []string      `env:"KAFKA_BROKERS" envDefault:"redpanda:9092" envSeparator:","`
	OutboxInterval time.Duration `env:"OUTBOX_INTERVAL" envDefault:"500ms"`
	LogLevel       string        `env:"LOG_LEVEL" envDefault:"info"`

	// Search read model (ADR 0009). OpenSearch being down at startup is not
	// fatal: ListJobs falls back to Postgres.
	OpenSearchURL         string        `env:"OPENSEARCH_URL" envDefault:"http://opensearch:9200"`
	SearchTimeout         time.Duration `env:"SEARCH_TIMEOUT" envDefault:"2s"`
	SearchBreakerFailures uint32        `env:"SEARCH_BREAKER_FAILURES" envDefault:"5"`
	SearchBreakerOpen     time.Duration `env:"SEARCH_BREAKER_OPEN" envDefault:"30s"`

	// AI matching service (ADR 0010). Down at startup is not fatal: only
	// ListMatchedCandidates fails (503) until it is reachable.
	AIAddr            string        `env:"AI_ADDR" envDefault:"ai-api:9100"`
	AITimeout         time.Duration `env:"AI_TIMEOUT" envDefault:"2s"`
	AIBreakerFailures uint32        `env:"AI_BREAKER_FAILURES" envDefault:"5"`
	AIBreakerOpen     time.Duration `env:"AI_BREAKER_OPEN" envDefault:"30s"`
}

// ProjectorConfig configures cmd/projector (consumer, rebuild, reconcile).
type ProjectorConfig struct {
	DBDSN         string        `env:"DB_DSN,required"`
	KafkaBrokers  []string      `env:"KAFKA_BROKERS" envDefault:"redpanda:9092" envSeparator:","`
	OpenSearchURL string        `env:"OPENSEARCH_URL" envDefault:"http://opensearch:9200"`
	HTTPPort      int           `env:"PROJECTOR_HTTP_PORT" envDefault:"8012"`
	AliasTTL      time.Duration `env:"PROJECTOR_ALIAS_TTL" envDefault:"5s"`
	LogLevel      string        `env:"LOG_LEVEL" envDefault:"info"`
}

// LoadProjector reads the projector configuration from JOB_* variables.
func LoadProjector() (ProjectorConfig, error) {
	var c ProjectorConfig
	if err := config.Load(EnvPrefix, &c); err != nil {
		return ProjectorConfig{}, err
	}
	return c, nil
}

// Load reads the server configuration from JOB_* variables.
func Load() (Config, error) {
	var c Config
	if err := config.Load(EnvPrefix, &c); err != nil {
		return Config{}, err
	}
	return c, nil
}
