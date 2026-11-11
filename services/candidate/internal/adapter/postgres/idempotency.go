package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nuroqzavitex/smarthire/pkg/idempotency"
)

// Idempotency implements app.Idempotency over the processed_events table.
type Idempotency struct{ pool *pgxpool.Pool }

// NewIdempotency returns an Idempotency over pool.
func NewIdempotency(pool *pgxpool.Pool) *Idempotency { return &Idempotency{pool: pool} }

// Once records eventID in the transaction carried by ctx and reports whether
// it was seen for the first time.
func (i *Idempotency) Once(ctx context.Context, eventID string) (bool, error) {
	return idempotency.Once(ctx, dbtx(ctx, i.pool), eventID)
}
