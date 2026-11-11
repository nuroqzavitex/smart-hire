//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/consumer"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

// resultTopics are per-test topics standing in for cv.parsed and
// cv.parse_failed, so a consumer group starting at the earliest offset never
// sees another test's records. The handler routes by envelope event_type.
type resultTopics struct{ parsed, failed, group string }

func startConsumer(t *testing.T) resultTopics {
	t.Helper()
	id := ulid.Make().String()
	rt := resultTopics{parsed: "cv.parsed." + id, failed: "cv.parse_failed." + id, group: "candidate-" + id}
	require.NoError(t, createTopics(context.Background(),
		rt.parsed, kafka.DLQTopic(rt.parsed), rt.failed, kafka.DLQTopic(rt.failed)))
	c, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: []string{broker}, Group: rt.group, Topics: []string{rt.parsed, rt.failed},
		Handler: consumer.NewHandler(svc), BaseBackoff: 10 * time.Millisecond,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		assert.NoError(t, <-done)
		c.Close()
	})
	return rt
}

func produce(t *testing.T, topic string, key, value []byte) {
	t.Helper()
	p, err := kafka.NewProducer([]string{broker})
	require.NoError(t, err)
	defer p.Close()
	require.NoError(t, p.Produce(context.Background(), kafka.Record{Topic: topic, Key: key, Value: value}))
}

func fullParsedCV(t *testing.T) json.RawMessage {
	t.Helper()
	b, err := os.ReadFile("../../../../contracts/events/testdata/parsed_cv/valid/full.json")
	require.NoError(t, err)
	return b
}

func parsedEvent(t *testing.T, cv domain.CV, attempt string, parsed json.RawMessage) []byte {
	t.Helper()
	env, err := event.New(context.Background(), "ai-service", "cv.parsed", 1, map[string]any{
		"cv_id": cv.ID.String(), "candidate_id": cv.CandidateID.String(), "attempt_id": attempt,
		"parsed": parsed, "model_version": "fake:rules-v1",
	}, time.Now())
	require.NoError(t, err)
	b, err := json.Marshal(env)
	require.NoError(t, err)
	return b
}

func failedEvent(t *testing.T, cv domain.CV, attempt string) ([]byte, string) {
	t.Helper()
	env, err := event.New(context.Background(), "ai-service", "cv.parse_failed", 1, map[string]any{
		"cv_id": cv.ID.String(), "candidate_id": cv.CandidateID.String(), "attempt_id": attempt,
		"reason": "no_text", "detail": "extracted text is too short",
	}, time.Now())
	require.NoError(t, err)
	b, err := json.Marshal(env)
	require.NoError(t, err)
	return b, env.EventID
}

// seedUploaded inserts a CV in UPLOADED with parse attempt attempt.
func seedUploaded(t *testing.T, attempt string) domain.CV {
	t.Helper()
	now := time.Now().UTC()
	cv, err := domain.NewCV(uuid.New(), uuid.New(), "cv.pdf", now)
	require.NoError(t, err)
	require.NoError(t, cv.Complete(100, attempt, now))
	require.NoError(t, postgres.NewCVRepo(pool).Create(context.Background(), cv))
	return cv
}

func getCV(t *testing.T, id uuid.UUID) domain.CV {
	t.Helper()
	cv, err := postgres.NewCVRepo(pool).Get(context.Background(), id)
	require.NoError(t, err)
	return cv
}

// waitCommitted waits until the group has committed n records of topic.
func waitCommitted(t *testing.T, group, topic string, n int64) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	require.NoError(t, err)
	defer cl.Close()
	adm := kadm.NewClient(cl)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := adm.FetchOffsets(context.Background(), group)
		require.NoError(c, err)
		var sum int64
		resp.Each(func(o kadm.OffsetResponse) {
			if o.Topic == topic && o.At > 0 {
				sum += o.At
			}
		})
		assert.Equal(c, n, sum)
	}, 15*time.Second, 100*time.Millisecond)
}

func TestConsumer_AppliesParsed(t *testing.T) {
	truncate(t)
	rt := startConsumer(t)
	attempt := ulid.Make().String()
	cv := seedUploaded(t, attempt)
	full := fullParsedCV(t)
	produce(t, rt.parsed, []byte(cv.ID.String()), parsedEvent(t, cv, attempt, full))

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, domain.StatusParsed, getCV(t, cv.ID).Status)
	}, 10*time.Second, 100*time.Millisecond)
	got := getCV(t, cv.ID)
	assert.JSONEq(t, string(full), string(got.ParsedData))
	assert.Equal(t, cv.Version+1, got.Version)
	assert.NotNil(t, got.ParsedAt)
}

func TestConsumer_StaleAndDuplicateIgnored(t *testing.T) {
	truncate(t)
	rt := startConsumer(t)
	attemptA := ulid.Make().String()
	cv := seedUploaded(t, attemptA)
	key := []byte(cv.ID.String())

	parsed := parsedEvent(t, cv, attemptA, fullParsedCV(t))
	produce(t, rt.parsed, key, parsed)
	produce(t, rt.parsed, key, parsed) // redelivery of the same event
	waitCommitted(t, rt.group, rt.parsed, 2)
	got := getCV(t, cv.ID)
	assert.Equal(t, domain.StatusParsed, got.Status)
	assert.Equal(t, cv.Version+1, got.Version, "a duplicate event must not apply twice")

	// Discriminating dedupe check: an event whose event_id is already in
	// processed_events must be skipped even though it would otherwise apply
	// (CV UPLOADED, matching attempt).
	attemptC := ulid.Make().String()
	cv2 := seedUploaded(t, attemptC)
	seen := parsedEvent(t, cv2, attemptC, fullParsedCV(t))
	var seenEnv event.Envelope
	require.NoError(t, json.Unmarshal(seen, &seenEnv))
	_, err := pool.Exec(context.Background(), `INSERT INTO processed_events (event_id) VALUES ($1)`, seenEnv.EventID)
	require.NoError(t, err)
	produce(t, rt.parsed, []byte(cv2.ID.String()), seen)
	waitCommitted(t, rt.group, rt.parsed, 3)
	got2 := getCV(t, cv2.ID)
	assert.Equal(t, domain.StatusUploaded, got2.Status, "an already processed event_id must not apply")
	assert.Equal(t, cv2.Version, got2.Version)
	assert.Nil(t, got2.ParsedData)

	reparsed, err := svc.ReparseCV(asCandidate(cv.CandidateID), cv.ID)
	require.NoError(t, err)
	require.NotEqual(t, attemptA, reparsed.AttemptID)

	stale, staleID := failedEvent(t, cv, attemptA)
	produce(t, rt.failed, key, stale)
	waitCommitted(t, rt.group, rt.failed, 1)
	got = getCV(t, cv.ID)
	assert.Equal(t, domain.StatusUploaded, got.Status)
	assert.Equal(t, reparsed.AttemptID, got.AttemptID)
	assert.Equal(t, reparsed.Version, got.Version)
	assert.Nil(t, got.ParseError)

	var env event.Envelope
	require.NoError(t, json.Unmarshal(parsed, &env))
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM processed_events WHERE event_id = ANY($1)`, []string{env.EventID, staleID}).Scan(&n))
	assert.Equal(t, 2, n)
}

func TestConsumer_PoisonGoesToDLQ(t *testing.T) {
	truncate(t)
	rt := startConsumer(t)
	unknown := domain.CV{ID: uuid.New(), CandidateID: uuid.New()}
	produce(t, rt.parsed, []byte("k1"), []byte("{}"))
	produce(t, rt.parsed, []byte("k2"), parsedEvent(t, unknown, ulid.Make().String(), fullParsedCV(t)))

	cl, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(kafka.DLQTopic(rt.parsed)),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer cl.Close()
	attempts := map[string]string{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for len(attempts) < 2 && ctx.Err() == nil {
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
			for _, h := range r.Headers {
				if h.Key == "dlq-attempts" {
					attempts[string(r.Key)] = string(h.Value)
				}
				if h.Key == "dlq-error" {
					assert.NotContains(t, string(h.Value), unknown.CandidateID.String())
				}
			}
		})
	}
	assert.Equal(t, map[string]string{"k1": "0", "k2": "1"}, attempts)
}
