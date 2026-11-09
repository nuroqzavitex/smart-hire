// Package outbox implements the transactional outbox: Writer stores events in
// the business transaction, Relay publishes them to Kafka (at-least-once).
package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/nuroqzavitex/smarthire/pkg/event"
)

// SchemaSQL is the outbox DDL every service migration mirrors (spec §3.3).
const SchemaSQL = `
CREATE TABLE IF NOT EXISTS outbox (
    id uuid PRIMARY KEY,
    aggregate_id text NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    headers jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (created_at) WHERE published_at IS NULL;
`

// Querier is satisfied by pgx.Tx and *pgxpool.Pool.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Writer inserts events into the outbox table.
type Writer struct{}

// Write stores env and the W3C trace carrier of ctx using q, normally the
// business transaction.
func (Writer) Write(ctx context.Context, q Querier, env event.Envelope, aggregateID string) error {
	payload, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	headers, err := json.Marshal(carrier)
	if err != nil {
		return fmt.Errorf("marshal headers: %w", err)
	}
	_, err = q.Exec(ctx, `INSERT INTO outbox (id, aggregate_id, event_type, payload, headers, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		uuid.New(), aggregateID, env.EventType, payload, headers, env.OccurredAt)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}
