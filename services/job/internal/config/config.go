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
}

// Load reads the server configuration from JOB_* variables.
func Load() (Config, error) {
	var c Config
	if err := config.Load(EnvPrefix, &c); err != nil {
		return Config{}, err
	}
	return c, nil
}
