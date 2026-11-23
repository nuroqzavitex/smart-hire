package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/outbox"
)

// OutboxWriter implements app.OutboxWriter.
type OutboxWriter struct{ pool *pgxpool.Pool }

// NewOutboxWriter returns an OutboxWriter over pool.
func NewOutboxWriter(pool *pgxpool.Pool) *OutboxWriter { return &OutboxWriter{pool: pool} }

// Write stores the envelope and trace headers through pkg/outbox. Call it
// inside the transaction of the state change it describes.
func (w *OutboxWriter) Write(ctx context.Context, e event.Envelope, aggregateID string) error {
	return outbox.Writer{}.Write(ctx, dbtx(ctx, w.pool), e, aggregateID)
}

// DeleteCVUpdates removes the CV's cv.updated rows (they hold the parsed
// CV); run it in the purge transaction before the DELETED event is written.
func (w *OutboxWriter) DeleteCVUpdates(ctx context.Context, aggregateID string) error {
	if _, err := dbtx(ctx, w.pool).Exec(ctx,
		`DELETE FROM outbox WHERE aggregate_id = $1 AND event_type = 'cv.updated'`, aggregateID); err != nil {
		return fmt.Errorf("delete cv.updated outbox rows: %w", err)
	}
	return nil
}
