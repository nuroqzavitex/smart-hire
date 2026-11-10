// Package config defines the gateway configuration.
package config

import (
	"fmt"
	"strings"

	"github.com/nuroqzavitex/smarthire/pkg/config"
)

// Config holds gateway settings, read from GATEWAY_* env vars.
type Config struct {
	HTTPPort             int      `env:"HTTP_PORT" envDefault:"8080"`
	AuthGRPCAddr         string   `env:"AUTH_GRPC_ADDR" envDefault:"auth:9001"`
	JobGRPCAddr          string   `env:"JOB_GRPC_ADDR" envDefault:"job:9002"`
	AuthJWKSURL          string   `env:"AUTH_JWKS_URL" envDefault:"http://auth:8001/.well-known/jwks.json"`
	JWTIssuer            string   `env:"JWT_ISSUER" envDefault:"smarthire-auth"`
	JWTAudience          string   `env:"JWT_AUDIENCE" envDefault:"smarthire-api"`
	RedisAddr            string   `env:"REDIS_ADDR" envDefault:"redis:6379"`
	CORSOrigins          []string `env:"CORS_ORIGINS" envDefault:"http://localhost:3001" envSeparator:","`
	CookieSecure         bool     `env:"COOKIE_SECURE" envDefault:"true"`
	TrustedProxies       []string `env:"TRUSTED_PROXIES" envSeparator:","`
	RateLimitUserPerMin  int      `env:"RATE_LIMIT_USER_PER_MIN" envDefault:"100"`
	RateLimitLoginPerMin int      `env:"RATE_LIMIT_LOGIN_PER_MIN" envDefault:"10"`
	LogLevel             string   `env:"LOG_LEVEL" envDefault:"info"`
}

// Load reads the configuration from the environment (prefix GATEWAY).
func Load() (Config, error) {
	var c Config
	if err := config.Load("GATEWAY", &c); err != nil {
		return c, err
	}
	// CORSOrigins is also the Origin allow-list of the cookie routes and CORS
	// runs with credentials, so wildcards would defeat the CSRF defence.
	for _, o := range c.CORSOrigins {
		if strings.Contains(o, "*") {
			return c, fmt.Errorf("GATEWAY_CORS_ORIGINS: wildcard origin %q is not allowed", o)
		}
	}
	return c, nil
}
