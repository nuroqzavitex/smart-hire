package bootstrap

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadiness_NotReadyUntilServing(t *testing.T) {
	var serving atomic.Bool
	pings := 0
	check := readiness(&serving, func(context.Context) error { pings++; return nil })

	require.ErrorIs(t, check(context.Background()), errNotServing)
	assert.Zero(t, pings, "the DB is not pinged before gRPC serves")

	serving.Store(true)
	require.NoError(t, check(context.Background()))
	assert.Equal(t, 1, pings)

	serving.Store(false) // draining
	require.ErrorIs(t, check(context.Background()), errNotServing)
}

func TestReadiness_DBDown(t *testing.T) {
	var serving atomic.Bool
	serving.Store(true)
	down := errors.New("db down")
	check := readiness(&serving, func(context.Context) error { return down })
	require.ErrorIs(t, check(context.Background()), down)
}
