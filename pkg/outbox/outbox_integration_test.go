//go:build integration

package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/internal/testinfra"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
	"github.com/nuroqzavitex/smarthire/pkg/outbox"
)

var (
	broker string
	pool   *pgxpool.Pool
)

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	ctx := context.Background()
	b, stopK, err := testinfra.Redpanda(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "redpanda:", err)
		return 1
	}
	defer stopK()
	broker = b
	dsn, stopP, err := testinfra.Postgres(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "postgres:", err)
		return 1
	}
	defer stopP()
	pool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pool:", err)
		return 1
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, outbox.SchemaSQL); err != nil {
		fmt.Fprintln(os.Stderr, "schema:", err)
		return 1
	}
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return m.Run()
}

type fixture struct{ topic string }

func newFixture(t *testing.T) fixture {
	t.Helper()
	_, err := pool.Exec(context.Background(), "TRUNCATE outbox")
	require.NoError(t, err)
	f := fixture{topic: "t-" + ulid.Make().String()}
	require.NoError(t, testinfra.CreateTopics(context.Background(), broker, f.topic))
	return f
}

func newProducer(t *testing.T, brokers ...string) *kafka.Producer {
	t.Helper()
	if len(brokers) == 0 {
		brokers = []string{broker}
	}
	p, err := kafka.NewProducer(brokers)
	require.NoError(t, err)
	t.Cleanup(p.Close)
	return p
}

// write stores n events for f.topic in one committed tx and returns their event IDs.
func write(t *testing.T, ctx context.Context, f fixture, n int) []string {
	t.Helper()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		env, err := event.New(ctx, "test", f.topic, 1, map[string]int{"i": i}, time.Now().Add(time.Duration(i)*time.Microsecond))
		require.NoError(t, err)
		require.NoError(t, outbox.Writer{}.Write(ctx, tx, env, fmt.Sprintf("agg-%d", i)))
		ids = append(ids, env.EventID)
	}
	require.NoError(t, tx.Commit(ctx))
	return ids
}

func pending(t *testing.T) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), "SELECT count(*) FROM outbox WHERE published_at IS NULL").Scan(&n))
	return n
}

func readAll(t *testing.T, topic string, n int, wait time.Duration) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		fs.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func quiet() outbox.RelayOptions { return outbox.RelayOptions{Logger: slog.New(slog.DiscardHandler)} }

func tickOnce(t *testing.T, r *outbox.Relay) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	require.Eventually(t, func() bool { return pending(t) == 0 }, 20*time.Second, 20*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

func TestWriter_RolledBackTxNotPublished(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	env, err := event.New(ctx, "test", f.topic, 1, map[string]int{"i": 1}, time.Now())
	require.NoError(t, err)
	require.NoError(t, outbox.Writer{}.Write(ctx, tx, env, "agg"))
	require.NoError(t, tx.Rollback(ctx))

	rctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	require.NoError(t, outbox.NewRelay(pool, newProducer(t), quiet()).Run(rctx))
	assert.Empty(t, readAll(t, f.topic, 1, 2*time.Second))
	var n int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM outbox").Scan(&n))
	assert.Zero(t, n)
}

func TestWriter_StoresTraceHeaders(t *testing.T) {
	f := newFixture(t)
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("t").Start(context.Background(), "req")
	defer span.End()
	write(t, ctx, f, 1)
	var got string
	require.NoError(t, pool.QueryRow(context.Background(), "SELECT headers->>'traceparent' FROM outbox").Scan(&got))
	assert.Contains(t, got, span.SpanContext().TraceID().String())
}

func TestRelay_PublishesAndMarks(t *testing.T) {
	f := newFixture(t)
	ids := write(t, context.Background(), f, 5)
	r := outbox.NewRelay(pool, newProducer(t), quiet())
	tickOnce(t, r)
	assert.Zero(t, pending(t))
	recs := readAll(t, f.topic, 5, 20*time.Second)
	require.Len(t, recs, 5)
	for _, rec := range recs {
		var env event.Envelope
		require.NoError(t, json.Unmarshal(rec.Value, &env))
		assert.Contains(t, ids, env.EventID)
		assert.Equal(t, f.topic, env.EventType)
		assert.Contains(t, string(rec.Key), "agg-")
	}
}

func TestRelay_ConcurrentRelaysNoDuplicates(t *testing.T) {
	f := newFixture(t)
	write(t, context.Background(), f, 300)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		o := quiet()
		o.BatchSize = 20
		r := outbox.NewRelay(pool, newProducer(t), o)
		wg.Add(1)
		go func() { defer wg.Done(); _ = r.Run(ctx) }()
	}
	require.Eventually(t, func() bool { return pending(t) == 0 }, 60*time.Second, 50*time.Millisecond)
	cancel()
	wg.Wait()
	recs := readAll(t, f.topic, 300, 30*time.Second)
	seen := map[string]int{}
	for _, rec := range recs {
		var env event.Envelope
		require.NoError(t, json.Unmarshal(rec.Value, &env))
		seen[env.EventID]++
	}
	assert.Len(t, recs, 300)
	assert.Len(t, seen, 300)
	for id, c := range seen {
		assert.Equal(t, 1, c, id)
	}
}

type flaky struct {
	inner outbox.Publisher
	n     atomic.Int32
	failN int32
}

func (f *flaky) Produce(ctx context.Context, r kafka.Record) error {
	if f.n.Add(1) == f.failN {
		return errors.New("injected failure")
	}
	return f.inner.Produce(ctx, r)
}

func TestRelay_MidBatchFailureRetriesRest(t *testing.T) {
	f := newFixture(t)
	write(t, context.Background(), f, 5)
	pub := &flaky{inner: newProducer(t), failN: 3}
	o := quiet()
	o.Interval = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- outbox.NewRelay(pool, pub, o).Run(ctx) }()
	// After the first (failed) tick 2 are marked; the next tick finishes the rest.
	require.Eventually(t, func() bool { return pending(t) == 0 }, 20*time.Second, 20*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	assert.Equal(t, int32(6), pub.n.Load(), "2 ok + 1 failed + 3 retried")
	assert.Len(t, readAll(t, f.topic, 6, 3*time.Second), 5)
}

func TestRelay_FullBatchContinuesImmediately(t *testing.T) {
	f := newFixture(t)
	write(t, context.Background(), f, 25)
	o := quiet()
	o.BatchSize = 10
	o.Interval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- outbox.NewRelay(pool, newProducer(t), o).Run(ctx) }()
	require.Eventually(t, func() bool { return pending(t) == 0 }, 5*time.Second, 20*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

func TestRelay_PreservesTraceAcrossKafka(t *testing.T) {
	f := newFixture(t)
	exp := sdktrace.NewTracerProvider()
	oldTP := otel.GetTracerProvider()
	otel.SetTracerProvider(exp)
	t.Cleanup(func() { otel.SetTracerProvider(oldTP) })

	ctx, span := exp.Tracer("t").Start(context.Background(), "req")
	write(t, ctx, f, 1)
	span.End()
	tickOnce(t, outbox.NewRelay(pool, newProducer(t), quiet()))

	recs := readAll(t, f.topic, 1, 20*time.Second)
	require.Len(t, recs, 1)
	tp := header(recs[0], "traceparent")
	require.NotEmpty(t, tp)
	sc := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(),
		propagation.MapCarrier{"traceparent": tp}))
	assert.Equal(t, span.SpanContext().TraceID(), sc.TraceID())
	assert.NotEqual(t, span.SpanContext().SpanID(), sc.SpanID(), "producer span is a child")
}

func TestRelay_PublishSpanIsChildOfStoredTrace(t *testing.T) {
	f := newFixture(t)
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	oldTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(oldTP) })

	ctx, span := tp.Tracer("t").Start(context.Background(), "req")
	write(t, ctx, f, 1)
	span.End()
	tickOnce(t, outbox.NewRelay(pool, newProducer(t), quiet()))

	var pub sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.Name() == f.topic+" publish" {
			pub = s
		}
	}
	require.NotNil(t, pub, "publish span recorded")
	assert.Equal(t, span.SpanContext().TraceID(), pub.SpanContext().TraceID())
	assert.Equal(t, span.SpanContext().SpanID(), pub.Parent().SpanID(), "publish is a direct child of the request span")

	recs := readAll(t, f.topic, 1, 20*time.Second)
	require.Len(t, recs, 1)
	sc := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(),
		propagation.MapCarrier{"traceparent": header(recs[0], "traceparent")}))
	assert.Equal(t, pub.SpanContext().SpanID(), sc.SpanID(), "consumer parent is the publish span")
}

func TestRelay_StopsPromptlyWhenBrokerDown(t *testing.T) {
	f := newFixture(t)
	write(t, context.Background(), f, 1)
	r := outbox.NewRelay(pool, newProducer(t, "127.0.0.1:1"), quiet())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
	assert.Equal(t, 1, pending(t))
}
