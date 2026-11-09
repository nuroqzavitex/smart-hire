//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/internal/testinfra"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
)

var broker string

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	b, terminate, err := testinfra.Redpanda(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, "redpanda:", err)
		return 1
	}
	defer terminate()
	broker = b
	return m.Run()
}

type fixture struct {
	topic, dlq, group string
}

func newFixture(t *testing.T, withDLQ bool) fixture {
	t.Helper()
	id := ulid.Make().String()
	f := fixture{topic: "t-" + id, group: "g-" + id}
	f.dlq = kafka.DLQTopic(f.topic)
	topics := []string{f.topic}
	if withDLQ {
		topics = append(topics, f.dlq)
	}
	require.NoError(t, testinfra.CreateTopics(context.Background(), broker, topics...))
	return f
}

func mustEnv(t *testing.T, payload any) []byte {
	t.Helper()
	env, err := event.New(context.Background(), "test", "x.happened", 1, payload, time.Now())
	require.NoError(t, err)
	b, err := json.Marshal(env)
	require.NoError(t, err)
	return b
}

func newProducer(t *testing.T) *kafka.Producer {
	t.Helper()
	p, err := kafka.NewProducer([]string{broker})
	require.NoError(t, err)
	t.Cleanup(p.Close)
	return p
}

type runner struct {
	c    *kafka.Consumer
	done chan struct{}
	err  error
	stop context.CancelFunc
}

func startConsumer(t *testing.T, f fixture, h kafka.Handler) *runner {
	t.Helper()
	c, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: []string{broker}, Group: f.group, Topics: []string{f.topic}, Handler: h,
		BaseBackoff: 10 * time.Millisecond, Logger: slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	r := &runner{c: c, done: make(chan struct{}), stop: cancel}
	go func() { r.err = c.Run(ctx); close(r.done) }()
	t.Cleanup(func() { r.shutdown(t) })
	return r
}

// shutdown cancels Run and asserts it stopped without error. If Run already
// stopped on its own (failure tests), the error is the test's concern.
func (r *runner) shutdown(t *testing.T) {
	t.Helper()
	select {
	case <-r.done:
		r.c.Close()
		return
	default:
	}
	r.stop()
	select {
	case <-r.done:
		assert.NoError(t, r.err)
	case <-time.After(10 * time.Second):
		t.Error("Run did not stop after cancel")
	}
	r.c.Close()
}

// readAll reads n records of topic from the start with a fresh client.
func readAll(t *testing.T, topic string, n int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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

// expectNone asserts topic has no records after a short wait.
func expectNone(t *testing.T, topic string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	fs := cl.PollFetches(ctx)
	assert.Zero(t, fs.NumRecords())
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func committed(t *testing.T, group, topic string) (int64, bool) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	require.NoError(t, err)
	defer cl.Close()
	offs, err := kadm.NewClient(cl).FetchOffsets(context.Background(), group)
	require.NoError(t, err)
	var sum int64
	var found bool
	offs.Each(func(o kadm.OffsetResponse) {
		if o.Topic == topic && o.Err == nil && o.At >= 0 {
			sum += o.At
			found = true
		}
	})
	return sum, found
}

func eventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	require.Eventually(t, cond, 20*time.Second, 50*time.Millisecond, msg)
}

func TestRoundTrip(t *testing.T) {
	f := newFixture(t, true)
	got := make(chan event.Envelope, 1)
	keys := make(chan string, 1)
	startConsumer(t, f, func(_ context.Context, env event.Envelope, rec *kgo.Record) error {
		got <- env
		keys <- string(rec.Key)
		return nil
	})
	val := mustEnv(t, map[string]string{"a": "b"})
	var sent event.Envelope
	require.NoError(t, json.Unmarshal(val, &sent))
	require.NoError(t, newProducer(t).Produce(context.Background(), kafka.Record{Topic: f.topic, Key: []byte("k1"), Value: val}))
	select {
	case env := <-got:
		assert.Equal(t, sent.EventID, env.EventID)
		assert.Equal(t, "k1", <-keys)
	case <-time.After(20 * time.Second):
		t.Fatal("timeout")
	}
}

func TestTracePropagates(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	oldTP, oldProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(oldTP)
		otel.SetTextMapPropagator(oldProp)
		_ = tp.Shutdown(context.Background())
	})

	f := newFixture(t, true)
	got := make(chan trace.TraceID, 1)
	startConsumer(t, f, func(ctx context.Context, _ event.Envelope, _ *kgo.Record) error {
		got <- trace.SpanContextFromContext(ctx).TraceID()
		return nil
	})
	ctx, span := tp.Tracer("test").Start(context.Background(), "root")
	require.NoError(t, newProducer(t).Produce(ctx, kafka.Record{Topic: f.topic, Key: []byte("k"), Value: mustEnv(t, 1)}))
	span.End()
	select {
	case id := <-got:
		assert.Equal(t, span.SpanContext().TraceID(), id)
	case <-time.After(20 * time.Second):
		t.Fatal("timeout")
	}
}

func TestTransientErrorRetriedThenSucceeds(t *testing.T) {
	f := newFixture(t, true)
	var mu sync.Mutex
	calls := 0
	startConsumer(t, f, func(context.Context, event.Envelope, *kgo.Record) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls < 3 {
			return fmt.Errorf("transient %d", calls)
		}
		return nil
	})
	require.NoError(t, newProducer(t).Produce(context.Background(), kafka.Record{Topic: f.topic, Key: []byte("k"), Value: mustEnv(t, 1)}))
	eventually(t, func() bool { _, ok := committed(t, f.group, f.topic); return ok }, "offset committed")
	mu.Lock()
	assert.Equal(t, 3, calls)
	mu.Unlock()
	expectNone(t, f.dlq)
}

func TestPermanentErrorGoesToDLQ(t *testing.T) {
	f := newFixture(t, true)
	var mu sync.Mutex
	calls := 0
	startConsumer(t, f, func(context.Context, event.Envelope, *kgo.Record) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return kafka.Permanent(fmt.Errorf("nope"))
	})
	val := mustEnv(t, 1)
	require.NoError(t, newProducer(t).Produce(context.Background(), kafka.Record{Topic: f.topic, Key: []byte("k"), Value: val}))
	orig := readAll(t, f.topic, 1)
	require.Len(t, orig, 1)
	recs := readAll(t, f.dlq, 1)
	require.Len(t, recs, 1)
	d := recs[0]
	assert.Equal(t, "k", string(d.Key))
	assert.Equal(t, val, d.Value)
	assert.Equal(t, "nope", header(d, "dlq-error"))
	assert.Equal(t, "1", header(d, "dlq-attempts"))
	assert.Equal(t, f.topic, header(d, "dlq-original-topic"))
	assert.Equal(t, fmt.Sprint(orig[0].Partition), header(d, "dlq-original-partition"))
	assert.Equal(t, fmt.Sprint(orig[0].Offset), header(d, "dlq-original-offset"))
	mu.Lock()
	assert.Equal(t, 1, calls)
	mu.Unlock()
	eventually(t, func() bool { sum, ok := committed(t, f.group, f.topic); return ok && sum == 1 }, "offset committed after DLQ write")
}

func TestExhaustedRetriesGoToDLQ(t *testing.T) {
	f := newFixture(t, true)
	var mu sync.Mutex
	calls := 0
	startConsumer(t, f, func(context.Context, event.Envelope, *kgo.Record) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return fmt.Errorf("always")
	})
	require.NoError(t, newProducer(t).Produce(context.Background(), kafka.Record{Topic: f.topic, Key: []byte("k"), Value: mustEnv(t, 1)}))
	recs := readAll(t, f.dlq, 1)
	require.Len(t, recs, 1)
	assert.Equal(t, "3", header(recs[0], "dlq-attempts"))
	assert.Equal(t, "always", header(recs[0], "dlq-error"))
	mu.Lock()
	assert.Equal(t, 3, calls)
	mu.Unlock()
}

func TestUndecodableGoesToDLQ(t *testing.T) {
	f := newFixture(t, true)
	var mu sync.Mutex
	calls := 0
	startConsumer(t, f, func(context.Context, event.Envelope, *kgo.Record) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return nil
	})
	p := newProducer(t)
	require.NoError(t, p.Produce(context.Background(), kafka.Record{Topic: f.topic, Key: []byte("k"), Value: []byte("not json")}))
	require.NoError(t, p.Produce(context.Background(), kafka.Record{Topic: f.topic, Key: []byte("k"), Value: []byte("{}")}))
	recs := readAll(t, f.dlq, 2)
	require.Len(t, recs, 2)
	for _, r := range recs {
		assert.Equal(t, "0", header(r, "dlq-attempts"))
		assert.Contains(t, header(r, "dlq-error"), "decode envelope")
	}
	mu.Lock()
	assert.Zero(t, calls)
	mu.Unlock()
}

func TestOrderPerKey(t *testing.T) {
	f := newFixture(t, true)
	var mu sync.Mutex
	var seen []int
	startConsumer(t, f, func(_ context.Context, env event.Envelope, _ *kgo.Record) error {
		var p struct{ Seq int }
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return kafka.Permanent(err)
		}
		mu.Lock()
		seen = append(seen, p.Seq)
		mu.Unlock()
		return nil
	})
	p := newProducer(t)
	for i := 0; i < 50; i++ {
		require.NoError(t, p.Produce(context.Background(), kafka.Record{Topic: f.topic, Key: []byte("same"), Value: mustEnv(t, map[string]int{"Seq": i})}))
	}
	eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) == 50 }, "50 records")
	mu.Lock()
	defer mu.Unlock()
	for i, s := range seen {
		assert.Equal(t, i, s)
	}
}

func TestCommittedOffsetsSurviveRestart(t *testing.T) {
	f := newFixture(t, true)
	var mu sync.Mutex
	n1 := 0
	r1 := startConsumer(t, f, func(context.Context, event.Envelope, *kgo.Record) error {
		mu.Lock()
		n1++
		mu.Unlock()
		return nil
	})
	p := newProducer(t)
	for i := 0; i < 5; i++ {
		require.NoError(t, p.Produce(context.Background(), kafka.Record{Topic: f.topic, Key: []byte("k"), Value: mustEnv(t, i)}))
	}
	eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return n1 == 5 }, "5 handled")
	eventually(t, func() bool { sum, ok := committed(t, f.group, f.topic); return ok && sum == 5 }, "5 committed")
	r1.shutdown(t)

	var n2 int
	startConsumer(t, f, func(context.Context, event.Envelope, *kgo.Record) error {
		mu.Lock()
		n2++
		mu.Unlock()
		return nil
	})
	time.Sleep(3 * time.Second)
	mu.Lock()
	assert.Zero(t, n2)
	mu.Unlock()
}

func TestDLQWriteFailureStopsRun(t *testing.T) {
	f := newFixture(t, false) // no .dlq topic, auto-create disabled
	r := startConsumer(t, f, func(context.Context, event.Envelope, *kgo.Record) error {
		return kafka.Permanent(fmt.Errorf("nope"))
	})
	require.NoError(t, newProducer(t).Produce(context.Background(), kafka.Record{Topic: f.topic, Key: []byte("k"), Value: mustEnv(t, 1)}))
	select {
	case <-r.done:
		require.Error(t, r.err)
		assert.Contains(t, r.err.Error(), "dlq")
	case <-time.After(60 * time.Second):
		t.Fatal("Run did not stop")
	}
	_, ok := committed(t, f.group, f.topic)
	assert.False(t, ok, "offset must not be committed")
}

func TestShutdownDuringHandlerDoesNotDLQOrCommit(t *testing.T) {
	f := newFixture(t, true)
	entered := make(chan struct{}, 1)
	c, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: []string{broker}, Group: f.group, Topics: []string{f.topic}, MaxAttempts: 1,
		Logger: slog.New(slog.DiscardHandler),
		Handler: func(ctx context.Context, _ event.Envelope, _ *kgo.Record) error {
			entered <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		},
	})
	require.NoError(t, err)
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	require.NoError(t, newProducer(t).Produce(context.Background(), kafka.Record{Topic: f.topic, Key: []byte("k"), Value: mustEnv(t, 1)}))
	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		t.Fatal("handler not called")
	}
	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not stop")
	}
	expectNone(t, f.dlq)
	_, ok := committed(t, f.group, f.topic)
	assert.False(t, ok, "offset must not be committed")
}

type handled struct {
	member    string
	partition int32
	offset    int64
	key       string
	seq       int
}

func TestRebalanceKeepsPerKeyOrder(t *testing.T) {
	f := newFixture(t, true)
	const keys, perKey = 9, 30
	var mu sync.Mutex
	var log []handled // append order == completion order across both members
	handler := func(member string) kafka.Handler {
		return func(_ context.Context, env event.Envelope, rec *kgo.Record) error {
			var p struct{ Seq int }
			if err := json.Unmarshal(env.Payload, &p); err != nil {
				return kafka.Permanent(err)
			}
			time.Sleep(30 * time.Millisecond)
			mu.Lock()
			log = append(log, handled{member, rec.Partition, rec.Offset, string(rec.Key), p.Seq})
			mu.Unlock()
			return nil
		}
	}
	// Build a backlog first so member A holds a sizeable polled batch when B joins.
	p := newProducer(t)
	for seq := 0; seq < perKey; seq++ {
		for k := 0; k < keys; k++ {
			require.NoError(t, p.Produce(context.Background(), kafka.Record{
				Topic: f.topic, Key: []byte(fmt.Sprintf("key-%d", k)), Value: mustEnv(t, map[string]int{"Seq": seq})}))
		}
	}
	startConsumer(t, f, handler("A"))
	eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(log) >= 20 }, "A started handling")
	startConsumer(t, f, handler("B")) // second member joins mid-stream
	eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		last := map[string]int{}
		for _, h := range log {
			if h.seq > last[h.key] {
				last[h.key] = h.seq
			}
		}
		for k := 0; k < keys; k++ {
			if last[fmt.Sprintf("key-%d", k)] != perKey-1 {
				return false
			}
		}
		return true
	}, "all records handled")
	mu.Lock()
	defer mu.Unlock()

	// (b) the rebalance really happened: B handled something.
	bCount := 0
	for _, h := range log {
		if h.member == "B" {
			bCount++
		}
	}
	require.Positive(t, bCount, "member B never received records, rebalance not exercised")

	// (c) after B handled offset X of a partition, A never handles an offset > X there.
	maxB := map[int32]int64{}
	for i, h := range log {
		if h.member == "B" {
			if cur, ok := maxB[h.partition]; !ok || h.offset > cur {
				maxB[h.partition] = h.offset
			}
			continue
		}
		if x, ok := maxB[h.partition]; ok {
			assert.LessOrEqual(t, h.offset, x,
				"log[%d]: A handled partition %d offset %d after B handled offset %d (concurrent ownership)", i, h.partition, h.offset, x)
		}
	}

	// (d) at most one duplicate per partition (the record in flight at handover).
	type po struct {
		p int32
		o int64
	}
	times := map[po]int{}
	dupPerPartition := map[int32]int{}
	for _, h := range log {
		k := po{h.partition, h.offset}
		times[k]++
		if times[k] == 2 {
			dupPerPartition[h.partition]++
		}
		assert.LessOrEqual(t, times[k], 2, "partition %d offset %d handled more than twice", h.partition, h.offset)
	}
	for part, n := range dupPerPartition {
		assert.LessOrEqual(t, n, 1, "partition %d has %d duplicates at handover", part, n)
	}

	// (e) per key: seq increases, with at most one replay at handover; no skips, none missing.
	perKeySeqs := map[string][]int{}
	for _, h := range log {
		perKeySeqs[h.key] = append(perKeySeqs[h.key], h.seq)
	}
	for k, s := range perKeySeqs {
		replays, hi := 0, -1
		have := map[int]bool{}
		for _, v := range s {
			assert.LessOrEqual(t, v, hi+1, "key %s: skipped ahead (%v)", k, s)
			if v <= hi {
				replays++
			} else {
				hi = v
			}
			have[v] = true
		}
		assert.LessOrEqual(t, replays, 1, "key %s: more than one replay (%v)", k, s)
		assert.Len(t, have, perKey, "key %s missing records", k)
	}
}
