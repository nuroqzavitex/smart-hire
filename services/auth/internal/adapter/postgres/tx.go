// Package postgres implements the auth persistence ports on PostgreSQL.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/postgres/sqlc"
)

const pgUniqueViolation = "23505"

// NewPool opens a pgx pool with OpenTelemetry tracing and verifies it.
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.ConnConfig.Tracer = otelpgx.NewTracer()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}

type txKey struct{}

// TxManager implements app.TxManager; the active transaction travels in ctx.
type TxManager struct{ pool *pgxpool.Pool }

// NewTxManager returns a TxManager over pool.
func NewTxManager(pool *pgxpool.Pool) *TxManager { return &TxManager{pool: pool} }

// WithTx commits when fn returns nil and rolls back otherwise. A nested call
// joins the transaction already in ctx.
func (m *TxManager) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if _, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return fn(ctx)
	}
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	// Roll back if fn panics, so the connection and its row locks are released.
	returned := false
	defer func() {
		if !returned {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	err = fn(context.WithValue(ctx, txKey{}, tx))
	returned = true
	if err != nil {
		if rbErr := tx.Rollback(context.WithoutCancel(ctx)); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			return errors.Join(err, fmt.Errorf("rollback: %w", rbErr))
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// queries returns sqlc queries bound to the tx in ctx, or to the pool.
func queries(ctx context.Context, pool *pgxpool.Pool) *sqlc.Queries {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return sqlc.New(tx)
	}
	return sqlc.New(pool)
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation
}

func mapNotFound(err error, msg string) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return apperr.New(apperr.ErrNotFound, msg)
	}
	return err
}
