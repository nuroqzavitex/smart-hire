package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/nuroqzavitex/smarthire/pkg/kafka"
)

// Publisher publishes one record synchronously; *kafka.Producer satisfies it.
type Publisher interface {
	Produce(ctx context.Context, r kafka.Record) error
}

// RelayOptions tunes the relay; zero values select the defaults.
type RelayOptions struct {
	BatchSize int
	Interval  time.Duration
	Logger    *slog.Logger
}

// Relay polls the outbox and publishes pending events.
type Relay struct {
	pool *pgxpool.Pool
	pub  Publisher
	opts RelayOptions
}

// NewRelay builds a Relay over pool and pub.
func NewRelay(pool *pgxpool.Pool, pub Publisher, opts RelayOptions) *Relay {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 100
	}
	if opts.Interval <= 0 {
		opts.Interval = 500 * time.Millisecond
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Relay{pool: pool, pub: pub, opts: opts}
}

type row struct {
	ID          uuid.UUID
	AggregateID string
	EventType   string
	Payload     []byte
	Headers     map[string]string
}

// recordFromRow extracts the stored trace into the returned ctx and strips
// traceparent/tracestate from the record headers, so the producer's publish
// span becomes a child of the stored trace and injects the new context.
func recordFromRow(ctx context.Context, r row) (context.Context, kafka.Record) {
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(r.Headers))
	hdrs := make(map[string]string, len(r.Headers))
	for k, v := range r.Headers {
		if k == "traceparent" || k == "tracestate" {
			continue
		}
		hdrs[k] = v
	}
	return ctx, kafka.Record{Topic: r.EventType, Key: []byte(r.AggregateID), Value: r.Payload, Headers: hdrs}
}

// Run polls until ctx is cancelled, then returns nil. Tick errors are logged.
func (r *Relay) Run(ctx context.Context) error {
	for {
		n, err := r.tick(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			r.opts.Logger.WarnContext(ctx, "outbox relay tick failed", "selected", n, "error", err)
		}
		if err == nil && n == r.opts.BatchSize {
			continue
		}
		t := time.NewTimer(r.opts.Interval)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
	}
}

func (r *Relay) tick(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }() // no-op after Commit

	rows, err := selectPending(ctx, tx, r.opts.BatchSize)
	if err != nil {
		return 0, err
	}
	var sent []uuid.UUID
	var pubErr error
	for _, rw := range rows {
		pctx, rec := recordFromRow(ctx, rw)
		if pubErr = r.pub.Produce(pctx, rec); pubErr != nil {
			break
		}
		sent = append(sent, rw.ID)
	}
	if len(sent) > 0 {
		if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, sent); err != nil {
			return len(rows), errors.Join(pubErr, fmt.Errorf("mark published: %w", err))
		}
		if err := tx.Commit(ctx); err != nil {
			return len(rows), errors.Join(pubErr, fmt.Errorf("commit: %w", err))
		}
	}
	return len(rows), pubErr
}

func selectPending(ctx context.Context, tx pgx.Tx, limit int) ([]row, error) {
	rs, err := tx.Query(ctx, `SELECT id, aggregate_id, event_type, payload, headers FROM outbox
		WHERE published_at IS NULL ORDER BY created_at, id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, fmt.Errorf("select pending: %w", err)
	}
	defer rs.Close()
	var out []row
	for rs.Next() {
		var rw row
		if err := rs.Scan(&rw.ID, &rw.AggregateID, &rw.EventType, &rw.Payload, &rw.Headers); err != nil {
			return nil, fmt.Errorf("scan outbox row: %w", err)
		}
		out = append(out, rw)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox: %w", err)
	}
	return out, nil
}
