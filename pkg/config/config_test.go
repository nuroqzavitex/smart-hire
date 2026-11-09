package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/config"
)

func TestLoad(t *testing.T) {
	type C struct {
		Port int    `env:"GRPC_PORT" envDefault:"9001"`
		DSN  string `env:"DB_DSN,required"`
	}
	t.Setenv("X_DB_DSN", "postgres://x")
	var c C
	require.NoError(t, config.Load("X", &c))
	assert.Equal(t, 9001, c.Port)
	assert.Equal(t, "postgres://x", c.DSN)
}

func TestLoad_MissingRequired(t *testing.T) {
	type C struct {
		DSN string `env:"DB_DSN,required"`
	}
	var c C
	err := config.Load("Y", &c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Y_DB_DSN")
}

func TestLoad_IgnoresUnprefixed(t *testing.T) {
	type C struct {
		DSN string `env:"DB_DSN,required"`
	}
	t.Setenv("DB_DSN", "postgres://bare")
	var c C
	err := config.Load("Z", &c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Z_DB_DSN")
}
