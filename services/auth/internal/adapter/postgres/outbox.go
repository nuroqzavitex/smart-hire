package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/postgres/sqlc"
)

// OutboxWriter implements app.OutboxWriter.
type OutboxWriter struct{ pool *pgxpool.Pool }

// NewOutboxWriter returns an OutboxWriter over pool.
func NewOutboxWriter(pool *pgxpool.Pool) *OutboxWriter { return &OutboxWriter{pool: pool} }

// Write stores the whole envelope as the outbox payload. Call it inside the
// transaction of the state change it describes.
func (w *OutboxWriter) Write(ctx context.Context, e event.Envelope, aggregateID string) error {
	payload, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	err = queries(ctx, w.pool).InsertOutbox(ctx, sqlc.InsertOutboxParams{
		ID: uuid.New(), AggregateID: aggregateID, EventType: e.EventType,
		Payload: payload, CreatedAt: e.OccurredAt,
	})
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}
