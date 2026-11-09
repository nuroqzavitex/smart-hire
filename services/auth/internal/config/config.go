// Package config defines the auth service configuration (env prefix AUTH).
package config

import (
	"time"

	"github.com/alexedwards/argon2id"

	"github.com/nuroqzavitex/smarthire/pkg/config"
)

// EnvPrefix is the environment variable prefix of the auth service.
const EnvPrefix = "AUTH"

// Core is the part of the configuration shared by the server and the seed
// command: the database and the password hashing cost.
type Core struct {
	DBDSN             string `env:"DB_DSN,required"`
	Argon2MemoryKiB   uint32 `env:"ARGON2_MEMORY_KIB" envDefault:"19456"`
	Argon2Iterations  uint32 `env:"ARGON2_ITERATIONS" envDefault:"2"`
	Argon2Parallelism uint8  `env:"ARGON2_PARALLELISM" envDefault:"1"`
	LogLevel          string `env:"LOG_LEVEL" envDefault:"info"`
}

// Config is the server configuration.
type Config struct {
	Core
	GRPCPort               int           `env:"GRPC_PORT" envDefault:"9001"`
	HTTPPort               int           `env:"HTTP_PORT" envDefault:"8001"`
	JWTPrivateKeyPath      string        `env:"JWT_PRIVATE_KEY_PATH,required"`
	JWTExtraPublicKeyPaths []string      `env:"JWT_EXTRA_PUBLIC_KEY_PATHS" envSeparator:","`
	JWTIssuer              string        `env:"JWT_ISSUER" envDefault:"smarthire-auth"`
	JWTAudience            string        `env:"JWT_AUDIENCE" envDefault:"smarthire-api"`
	AccessTTL              time.Duration `env:"ACCESS_TTL" envDefault:"15m"`
	RefreshTTL             time.Duration `env:"REFRESH_TTL" envDefault:"168h"`
	JWKSURL                string        `env:"JWKS_URL" envDefault:"http://localhost:8001/.well-known/jwks.json"`
}

// Load reads the server configuration from AUTH_* variables.
func Load() (Config, error) {
	var c Config
	if err := config.Load(EnvPrefix, &c); err != nil {
		return Config{}, err
	}
	return c, nil
}

// LoadCore reads only the shared part (used by the seed command).
func LoadCore() (Core, error) {
	var c Core
	if err := config.Load(EnvPrefix, &c); err != nil {
		return Core{}, err
	}
	return c, nil
}

// Argon2Params converts the hashing settings to argon2id parameters.
func (c Core) Argon2Params() argon2id.Params {
	return argon2id.Params{
		Memory: c.Argon2MemoryKiB, Iterations: c.Argon2Iterations, Parallelism: c.Argon2Parallelism,
		SaltLength: 16, KeyLength: 32,
	}
}

// Seed is the configuration of the seed command (SEED_* variables).
type Seed struct {
	AdminEmail    string `env:"ADMIN_EMAIL,required"`
	AdminPassword string `env:"ADMIN_PASSWORD,required"`
}

// LoadSeed reads SEED_ADMIN_EMAIL and SEED_ADMIN_PASSWORD.
func LoadSeed() (Seed, error) {
	var s Seed
	if err := config.Load("SEED", &s); err != nil {
		return Seed{}, err
	}
	return s, nil
}
