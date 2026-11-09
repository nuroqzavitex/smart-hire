// Package config loads service configuration from environment variables.
package config

import (
	"fmt"

	"github.com/caarlos0/env/v11"
)

// Load fills dst (a pointer to a struct with `env` tags) from env vars named PREFIX_KEY.
// Unprefixed variables are never consulted.
func Load(prefix string, dst any) error {
	if err := env.ParseWithOptions(dst, env.Options{Prefix: prefix + "_"}); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	return nil
}
