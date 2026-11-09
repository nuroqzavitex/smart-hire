package kafka

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Producer is an idempotent, acks=all synchronous producer.
type Producer struct{ cl *kgo.Client }

// NewProducer connects to brokers; opts are applied after the defaults.
func NewProducer(brokers []string, opts ...kgo.Opt) (*Producer, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(10 * time.Second),
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &Producer{cl: cl}, nil
}

// Produce publishes r synchronously, injecting the trace context into the
// headers unless r.Headers already carries a traceparent.
func (p *Producer) Produce(ctx context.Context, r Record) error {
	ctx, span := tracer().Start(ctx, r.Topic+" publish", trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", r.Topic)))
	defer span.End()

	keys := make([]string, 0, len(r.Headers))
	for k := range r.Headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	hdrs := make([]kgo.RecordHeader, 0, len(keys)+2)
	for _, k := range keys {
		hdrs = append(hdrs, kgo.RecordHeader{Key: k, Value: []byte(r.Headers[k])})
	}
	if _, ok := r.Headers["traceparent"]; !ok {
		otel.GetTextMapPropagator().Inject(ctx, headerCarrier{&hdrs})
	}
	rec := &kgo.Record{Topic: r.Topic, Key: r.Key, Value: r.Value, Headers: hdrs}
	if err := p.cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "produce failed")
		return fmt.Errorf("produce %s: %w", r.Topic, err)
	}
	return nil
}

// Close flushes and closes the client.
func (p *Producer) Close() { p.cl.Close() }
