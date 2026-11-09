// Package idempotency de-duplicates event handling through a processed_events
// table. Once must run inside the consumer's DB transaction so the marker and
// the business writes commit or roll back together.
package idempotency

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// SchemaSQL is the processed_events DDL every consuming service migration mirrors.
const SchemaSQL = `
CREATE TABLE processed_events (
    event_id text PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);
`

// Querier is satisfied by pgx.Tx and *pgxpool.Pool.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Once records eventID and reports whether this is the first time it was seen.
func Once(ctx context.Context, q Querier, eventID string) (bool, error) {
	if eventID == "" {
		return false, errors.New("idempotency: empty event id")
	}
	tag, err := q.Exec(ctx,
		`INSERT INTO processed_events (event_id, processed_at) VALUES ($1, now()) ON CONFLICT (event_id) DO NOTHING`,
		eventID)
	if err != nil {
		return false, fmt.Errorf("idempotency: record event: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
