// Package config defines the application service configuration (env prefix
// APPLICATION).
package config

import (
	"time"

	"github.com/nuroqzavitex/smarthire/pkg/config"
)

// EnvPrefix is the environment variable prefix of the application service.
const EnvPrefix = "APPLICATION"

// Config is the server configuration.
type Config struct {
	GRPCPort       int           `env:"GRPC_PORT" envDefault:"9004"`
	HTTPPort       int           `env:"HTTP_PORT" envDefault:"8004"`
	DBDSN          string        `env:"DB_DSN,required"`
	JWKSURL        string        `env:"JWKS_URL" envDefault:"http://auth:8001/.well-known/jwks.json"`
	JWTIssuer      string        `env:"JWT_ISSUER" envDefault:"smarthire-auth"`
	JWTAudience    string        `env:"JWT_AUDIENCE" envDefault:"smarthire-api"`
	KafkaBrokers   []string      `env:"KAFKA_BROKERS" envDefault:"redpanda:9092" envSeparator:","`
	OutboxInterval time.Duration `env:"OUTBOX_INTERVAL" envDefault:"500ms"`
	LogLevel       string        `env:"LOG_LEVEL" envDefault:"info"`

	// Outbound calls (spec §14.2): Job/Candidate 3s, AI 5s; retries come
	// from the gRPC retry policy, the AI also sits behind a breaker.
	JobAddr           string        `env:"JOB_ADDR" envDefault:"job:9002"`
	CandidateAddr     string        `env:"CANDIDATE_ADDR" envDefault:"candidate:9003"`
	AIAddr            string        `env:"AI_ADDR" envDefault:"ai-api:9100"`
	CallTimeout       time.Duration `env:"CALL_TIMEOUT" envDefault:"3s"`
	AITimeout         time.Duration `env:"AI_TIMEOUT" envDefault:"5s"`
	AIBreakerFailures uint32        `env:"AI_BREAKER_FAILURES" envDefault:"5"`
	AIBreakerOpen     time.Duration `env:"AI_BREAKER_OPEN" envDefault:"30s"`

	// Submit saga worker (ADR 0011).
	SagaPoll            time.Duration `env:"SAGA_POLL" envDefault:"500ms"`
	SagaBatch           int           `env:"SAGA_BATCH" envDefault:"10"`
	SagaLease           time.Duration `env:"SAGA_LEASE" envDefault:"30s"`
	SagaDeadline        time.Duration `env:"SAGA_DEADLINE" envDefault:"10m"`
	ScoreNotFoundWindow time.Duration `env:"SCORE_NOT_FOUND_WINDOW" envDefault:"2m"`
}

// Load reads the server configuration from APPLICATION_* variables.
func Load() (Config, error) {
	var c Config
	if err := config.Load(EnvPrefix, &c); err != nil {
		return Config{}, err
	}
	return c, nil
}
