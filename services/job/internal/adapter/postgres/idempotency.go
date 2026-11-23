package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nuroqzavitex/smarthire/pkg/idempotency"
)

// Idempotency implements app.Idempotency over processed_events.
type Idempotency struct{ pool *pgxpool.Pool }

// NewIdempotency returns an Idempotency over pool.
func NewIdempotency(pool *pgxpool.Pool) *Idempotency { return &Idempotency{pool: pool} }

// Once records eventID in the tx carried by ctx.
func (i *Idempotency) Once(ctx context.Context, eventID string) (bool, error) {
	return idempotency.Once(ctx, dbtx(ctx, i.pool), eventID)
}
