//go:build integration

package idempotency_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/idempotency"
	"github.com/nuroqzavitex/smarthire/pkg/internal/testinfra"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	ctx := context.Background()
	dsn, stop, err := testinfra.Postgres(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "postgres:", err)
		return 1
	}
	defer stop()
	pool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pool:", err)
		return 1
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, idempotency.SchemaSQL); err != nil {
		fmt.Fprintln(os.Stderr, "schema:", err)
		return 1
	}
	return m.Run()
}

func TestOnce_FirstThenDuplicate(t *testing.T) {
	ctx := context.Background()
	first, err := idempotency.Once(ctx, pool, "e1")
	require.NoError(t, err)
	assert.True(t, first)
	first, err = idempotency.Once(ctx, pool, "e1")
	require.NoError(t, err)
	assert.False(t, first)
}

func TestOnce_RollbackForgets(t *testing.T) {
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	first, err := idempotency.Once(ctx, tx, "e2")
	require.NoError(t, err)
	assert.True(t, first)
	require.NoError(t, tx.Rollback(ctx))
	first, err = idempotency.Once(ctx, pool, "e2")
	require.NoError(t, err)
	assert.True(t, first)
}

func TestOnce_EmptyID(t *testing.T) {
	_, err := idempotency.Once(context.Background(), pool, "")
	assert.Error(t, err)
}
