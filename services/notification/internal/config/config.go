// Package config defines the notification service configuration (env prefix
// NOTIFICATION).
package config

import (
	"time"

	"github.com/nuroqzavitex/smarthire/pkg/config"
)

// EnvPrefix is the environment variable prefix of the notification service.
const EnvPrefix = "NOTIFICATION"

// Config is the server configuration.
type Config struct {
	GRPCPort     int      `env:"GRPC_PORT" envDefault:"9005"`
	HTTPPort     int      `env:"HTTP_PORT" envDefault:"8005"`
	DBDSN        string   `env:"DB_DSN,required"`
	JWKSURL      string   `env:"JWKS_URL" envDefault:"http://auth:8001/.well-known/jwks.json"`
	JWTIssuer    string   `env:"JWT_ISSUER" envDefault:"smarthire-auth"`
	JWTAudience  string   `env:"JWT_AUDIENCE" envDefault:"smarthire-api"`
	KafkaBrokers []string `env:"KAFKA_BROKERS" envDefault:"redpanda:9092" envSeparator:","`
	LogLevel     string   `env:"LOG_LEVEL" envDefault:"info"`
	// OutboxInterval paces the relay of user.data_purged (ADR 0014).
	OutboxInterval time.Duration `env:"OUTBOX_INTERVAL" envDefault:"500ms"`

	// Email outbox (ADR 0012): SMTP without auth (Mailpit in compose).
	SMTPAddr    string        `env:"SMTP_ADDR" envDefault:"mailpit:1025"`
	MailFrom    string        `env:"MAIL_FROM" envDefault:"SmartHire <no-reply@smarthire.local>"`
	SMTPTimeout time.Duration `env:"SMTP_TIMEOUT" envDefault:"10s"`
	EmailPoll   time.Duration `env:"EMAIL_POLL" envDefault:"1s"`

	// WSQueue bounds the messages buffered per WebSocket connection; a full
	// queue drops that connection.
	WSQueue int `env:"WS_QUEUE" envDefault:"16"`
}

// Load reads the server configuration from NOTIFICATION_* variables.
func Load() (Config, error) {
	var c Config
	if err := config.Load(EnvPrefix, &c); err != nil {
		return Config{}, err
	}
	return c, nil
}
