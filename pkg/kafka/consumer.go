package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/nuroqzavitex/smarthire/pkg/event"
)

// Handler processes one decoded event. Return Permanent(err) to skip retries.
type Handler func(ctx context.Context, env event.Envelope, rec *kgo.Record) error

// ConsumerConfig configures a Consumer. Zero values: MaxAttempts 3,
// BaseBackoff 200ms, MaxBackoff 2s, Logger slog.Default().
type ConsumerConfig struct {
	Brokers     []string
	Group       string
	Topics      []string
	Handler     Handler
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	Logger      *slog.Logger
}

// Consumer is a consumer-group member with manual commits, in-process retry
// and DLQ.
type Consumer struct {
	cl  *kgo.Client
	cfg ConsumerConfig
}

// NewConsumer builds the client; the group is joined on the first poll.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if cfg.Handler == nil {
		return nil, errors.New("kafka consumer: nil handler")
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 200 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 2 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.RecordDeliveryTimeout(10*time.Second),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &Consumer{cl: cl, cfg: cfg}, nil
}

// Close leaves the group and closes the client.
func (c *Consumer) Close() { c.cl.Close() }

// pollBatch bounds how many records are processed between rebalance windows.
// Rebalances are blocked while a batch is in flight (BlockRebalanceOnPoll), so
// pollBatch * (MaxAttempts * (handler time + MaxBackoff)) must stay well under
// the group rebalance timeout (default 60s), or the member is kicked out.
const pollBatch = 10

// Run blocks until ctx is cancelled (returns nil) or a DLQ write fails.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.cl.PollRecords(ctx, pollBatch)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			c.cl.AllowRebalance()
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.cfg.Logger.Error("kafka fetch error", "topic", t, "partition", p, "error", err)
		})
		err := c.settle(ctx, fetches)
		c.cl.AllowRebalance()
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

// settle processes a batch in order, committing after each record. It returns
// nil early (without committing the current record) when ctx is cancelled.
func (c *Consumer) settle(ctx context.Context, fetches kgo.Fetches) error {
	iter := fetches.RecordIter()
	for !iter.Done() {
		rec := iter.Next()
		done, err := c.process(ctx, rec)
		if err != nil {
			return err
		}
		if !done {
			return nil
		}
		if err := c.cl.CommitRecords(ctx, rec); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("commit %s/%d@%d: %w", rec.Topic, rec.Partition, rec.Offset, err)
		}
	}
	return nil
}

// process returns done=false when ctx was cancelled before the record finished.
func (c *Consumer) process(parent context.Context, rec *kgo.Record) (bool, error) {
	hdrs := rec.Headers
	ctx := otel.GetTextMapPropagator().Extract(parent, headerCarrier{&hdrs})
	ctx, span := tracer().Start(ctx, rec.Topic+" process", trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", rec.Topic),
			attribute.Int64("messaging.kafka.offset", rec.Offset)))
	defer span.End()

	var env event.Envelope
	if err := json.Unmarshal(rec.Value, &env); err != nil {
		return c.toDLQ(ctx, parent, rec, fmt.Errorf("decode envelope: %w", err), 0)
	}
	if env.EventID == "" || env.EventType == "" {
		return c.toDLQ(ctx, parent, rec, errors.New("decode envelope: missing event_id or event_type"), 0)
	}

	for attempt := 1; ; attempt++ {
		err := c.cfg.Handler(ctx, env, rec)
		if err == nil {
			return true, nil
		}
		if parent.Err() != nil { // shutting down: leave the record uncommitted
			return false, nil
		}
		c.cfg.Logger.Warn("kafka handler failed", "topic", rec.Topic, "partition", rec.Partition,
			"offset", rec.Offset, "event_id", env.EventID, "attempt", attempt, "error", err)
		if IsPermanent(err) || attempt >= c.cfg.MaxAttempts {
			span.SetStatus(codes.Error, "handler failed")
			return c.toDLQ(ctx, parent, rec, err, attempt)
		}
		select {
		case <-parent.Done():
			return false, nil
		case <-time.After(backoff(attempt, c.cfg.BaseBackoff, c.cfg.MaxBackoff)):
		}
	}
}

// toDLQ writes the DLQ record; a failure caused by shutdown is not an error and
// leaves the record uncommitted (it will be redelivered).
func (c *Consumer) toDLQ(ctx, parent context.Context, rec *kgo.Record, cause error, attempts int) (bool, error) {
	if err := c.dlq(ctx, rec, cause, attempts); err != nil {
		if parent.Err() != nil {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *Consumer) dlq(ctx context.Context, rec *kgo.Record, cause error, attempts int) error {
	hdrs := make([]kgo.RecordHeader, 0, len(rec.Headers)+5)
	hdrs = append(hdrs, rec.Headers...)
	for k, v := range dlqHeaders(rec, cause, attempts) {
		hdrs = append(hdrs, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	out := &kgo.Record{Topic: DLQTopic(rec.Topic), Key: rec.Key, Value: rec.Value, Headers: hdrs}
	if err := c.cl.ProduceSync(ctx, out).FirstErr(); err != nil {
		return fmt.Errorf("dlq %s: %w", out.Topic, err)
	}
	c.cfg.Logger.Error("kafka record sent to dlq", "topic", rec.Topic, "partition", rec.Partition,
		"offset", rec.Offset, "attempts", attempts)
	return nil
}

// backoff returns min(base<<(attempt-1), ceiling); attempt starts at 1.
func backoff(attempt int, base, ceiling time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift >= 32 {
		return ceiling
	}
	d := base << shift
	if d <= 0 || d > ceiling {
		return ceiling
	}
	return d
}
