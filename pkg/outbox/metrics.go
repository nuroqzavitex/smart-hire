package outbox

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/metric"
)

const pendingQueryTimeout = 2 * time.Second

// RowQuerier runs a query returning one row (satisfied by *pgxpool.Pool).
type RowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// RegisterPendingGauge registers the observable gauge outbox.pending: the
// number of events not yet published (alert: backlog > 500, ADR 0013). A
// failed count skips the observation instead of reporting a wrong value.
func RegisterPendingGauge(m metric.Meter, q RowQuerier) (metric.Registration, error) {
	g, err := m.Int64ObservableGauge("outbox.pending",
		metric.WithDescription("Outbox events not yet published to Kafka"))
	if err != nil {
		return nil, err
	}
	return m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, pendingQueryTimeout)
		defer cancel()
		var n int64
		if err := q.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE published_at IS NULL").Scan(&n); err != nil {
			return nil //nolint:nilerr // skip this observation; the next collection retries
		}
		o.ObserveInt64(g, n)
		return nil
	}, g)
}
