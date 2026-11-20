package projection_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
	"github.com/nuroqzavitex/smarthire/services/job/internal/projection"
)

const jobID = "0b6f4c38-7c5e-4d6c-9a43-3a5e2b8f1c11"

var occurred = time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)

func envelope(t *testing.T, typ string, payload map[string]any) event.Envelope {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	return event.Envelope{
		EventID: "01JD0000000000000000000000", EventType: typ, EventVersion: 1,
		Source: "job-service", OccurredAt: occurred, Payload: raw,
	}
}

func full(status string, version int64, publishedAt any) map[string]any {
	p := map[string]any{
		"job_id": jobID, "recruiter_id": "6a1f0d2e-1111-4c2b-8d3e-222222222222",
		"title": "Kỹ sư Go", "description": "Xây dựng dịch vụ phân tán bằng Go.",
		"required_skills": []string{"Golang"}, "nice_to_have_skills": []string{},
		"level": "senior", "location": "Hà Nội", "salary_min": 1000, "salary_max": nil,
		"status": status, "version": version,
	}
	if publishedAt != "absent" {
		p["published_at"] = publishedAt
	}
	return p
}

func TestFromEnvelope_Table(t *testing.T) {
	pub := time.Date(2026, 11, 19, 8, 0, 0, 0, time.UTC)
	salaryMin := 1000

	d, err := projection.FromEnvelope(envelope(t, "job.created", full("open", 2, pub.Format(time.RFC3339))))
	require.NoError(t, err)
	assert.Equal(t, projection.Document{
		JobID: jobID, Version: 2, Status: "open", RecruiterID: "6a1f0d2e-1111-4c2b-8d3e-222222222222",
		Title: "Kỹ sư Go", Description: "Xây dựng dịch vụ phân tán bằng Go.",
		RequiredSkills: []string{"Golang"}, NiceToHaveSkills: []string{},
		Level: "senior", Location: "Hà Nội", SalaryMin: &salaryMin, PublishedAt: &pub,
	}, d)

	d, err = projection.FromEnvelope(envelope(t, "job.updated", full("open", 3, "absent")))
	require.NoError(t, err)
	require.NotNil(t, d.PublishedAt, "open event without published_at falls back to occurred_at")
	assert.Equal(t, occurred, *d.PublishedAt)

	d, err = projection.FromEnvelope(envelope(t, "job.created", full("draft", 1, nil)))
	require.NoError(t, err)
	assert.Nil(t, d.PublishedAt)
	assert.Equal(t, "draft", d.Status)

	for typ, status := range map[string]string{"job.closed": "closed", "job.deleted": "deleted"} {
		d, err = projection.FromEnvelope(envelope(t, typ, map[string]any{"job_id": jobID, "version": 7}))
		require.NoError(t, err)
		assert.Equal(t, projection.Document{JobID: jobID, Version: 7, Status: status}, d, typ)
	}

	_, err = projection.FromEnvelope(envelope(t, "job.archived", map[string]any{"job_id": jobID, "version": 1}))
	assert.True(t, kafka.IsPermanent(err), "unknown type is permanent")
	bad := envelope(t, "job.created", nil)
	bad.Payload = json.RawMessage(`{"job_id": 42}`)
	_, err = projection.FromEnvelope(bad)
	assert.True(t, kafka.IsPermanent(err), "unparsable payload is permanent")
}

type fakeStore struct {
	mu        sync.Mutex
	indices   []string
	resolves  int
	puts      map[string][]projection.Document
	applied   map[string]bool
	putErr    error
	resolveFn func() ([]string, error)
}

func newFakeStore(indices ...string) *fakeStore {
	return &fakeStore{indices: indices, puts: map[string][]projection.Document{}, applied: map[string]bool{}}
}

func (f *fakeStore) WriteIndices(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resolves++
	if f.resolveFn != nil {
		return f.resolveFn()
	}
	return f.indices, nil
}

func (f *fakeStore) Put(_ context.Context, index string, d projection.Document) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.putErr != nil {
		return false, f.putErr
	}
	f.puts[index] = append(f.puts[index], d)
	applied, ok := f.applied[index]
	return !ok || applied, nil
}

func newProjector(s projection.Store, validate func(string, []byte) error) *projection.Projector {
	if validate == nil {
		validate = func(string, []byte) error { return nil }
	}
	return &projection.Projector{Store: s, Validate: validate, AliasTTL: time.Minute}
}

func TestHandle_InvalidEnvelopeIsPermanent(t *testing.T) {
	secret := "candidate@example.com"
	p := newProjector(newFakeStore("jobs_v1"), func(string, []byte) error {
		return errors.New("at /payload/title: " + secret + " is invalid")
	})
	err := p.Handle(context.Background(), envelope(t, "job.created", full("open", 1, nil)), nil)
	require.Error(t, err)
	assert.True(t, kafka.IsPermanent(err))
	assert.NotContains(t, err.Error(), secret)
}

func TestHandle_WritesEveryWriteIndex(t *testing.T) {
	s := newFakeStore("jobs_v1", "jobs_v2")
	s.applied["jobs_v2"] = false // v2 already holds a newer version: 409
	p := newProjector(s, nil)
	require.NoError(t, p.Handle(context.Background(), envelope(t, "job.updated", full("open", 4, nil)), nil))
	assert.Len(t, s.puts["jobs_v1"], 1)
	assert.Len(t, s.puts["jobs_v2"], 1)
	assert.Equal(t, int64(4), s.puts["jobs_v1"][0].Version)
}

func TestHandle_StoreErrorIsTransient(t *testing.T) {
	s := newFakeStore("jobs_v1")
	s.putErr = errors.New("connection refused")
	err := newProjector(s, nil).Handle(context.Background(), envelope(t, "job.closed", map[string]any{"job_id": jobID, "version": 2}), nil)
	require.Error(t, err)
	assert.False(t, kafka.IsPermanent(err))
}

func TestHandle_ResolveErrorIsTransient(t *testing.T) {
	s := newFakeStore()
	s.resolveFn = func() ([]string, error) { return nil, errors.New("timeout") }
	err := newProjector(s, nil).Handle(context.Background(), envelope(t, "job.closed", map[string]any{"job_id": jobID, "version": 2}), nil)
	require.Error(t, err)
	assert.False(t, kafka.IsPermanent(err))
}

func TestHandle_AliasCachedForTTL(t *testing.T) {
	s := newFakeStore("jobs_v1")
	now := occurred
	p := newProjector(s, nil)
	p.AliasTTL = 5 * time.Second
	p.Now = func() time.Time { return now }
	ev := envelope(t, "job.closed", map[string]any{"job_id": jobID, "version": 2})
	require.NoError(t, p.Handle(context.Background(), ev, nil))
	require.NoError(t, p.Handle(context.Background(), ev, nil))
	assert.Equal(t, 1, s.resolves)
	now = now.Add(6 * time.Second)
	require.NoError(t, p.Handle(context.Background(), ev, nil))
	assert.Equal(t, 2, s.resolves)
}

type goneErr struct{}

func (goneErr) Error() string { return "index gone" }

func TestHandle_IndexGoneRefreshesAlias(t *testing.T) {
	s := newFakeStore()
	resolved := [][]string{{"jobs_v1", "jobs_v2"}, {"jobs_v2"}}
	s.resolveFn = func() ([]string, error) {
		r := resolved[0]
		if len(resolved) > 1 {
			resolved = resolved[1:]
		}
		return r, nil
	}
	p := newProjector(s, nil)
	p.IndexGone = func(err error) bool { return errors.As(err, new(goneErr)) }
	inner := s
	gone := &goneStore{fakeStore: inner, gone: "jobs_v1"}
	p.Store = gone
	require.NoError(t, p.Handle(context.Background(), envelope(t, "job.closed", map[string]any{"job_id": jobID, "version": 2}), nil))
	assert.Equal(t, 2, s.resolves, "re-resolved the write alias once")
	assert.Len(t, s.puts["jobs_v2"], 1, "retried on the fresh alias")
}

// goneStore fails writes to one deleted index.
type goneStore struct {
	*fakeStore
	gone string
}

func (g *goneStore) Put(ctx context.Context, index string, d projection.Document) (bool, error) {
	if index == g.gone {
		return false, goneErr{}
	}
	return g.fakeStore.Put(ctx, index, d)
}

// flakyStore fails Put with err for the first n calls.
type flakyStore struct {
	*fakeStore
	n   int
	err error
}

func (f *flakyStore) Put(ctx context.Context, index string, d projection.Document) (bool, error) {
	if f.n > 0 {
		f.n--
		return false, f.err
	}
	return f.fakeStore.Put(ctx, index, d)
}

func retrying(s projection.Store, transient bool) *projection.Projector {
	p := newProjector(s, nil)
	p.Transient = func(error) bool { return transient }
	p.RetryBase, p.RetryMax = time.Millisecond, 4*time.Millisecond
	return p
}

func TestHandle_RetriesInfrastructureErrorsUntilSuccess(t *testing.T) {
	inner := newFakeStore("jobs_v1")
	s := &flakyStore{fakeStore: inner, n: 6, err: errors.New("connection refused")}
	err := retrying(s, true).Handle(context.Background(), envelope(t, "job.closed", map[string]any{"job_id": jobID, "version": 2}), nil)
	require.NoError(t, err, "an opensearch outage blocks instead of dead-lettering")
	assert.Len(t, inner.puts["jobs_v1"], 1)
}

func TestHandle_NonTransientStoreErrorIsPermanent(t *testing.T) {
	s := &flakyStore{fakeStore: newFakeStore("jobs_v1"), n: 1, err: errors.New("mapper_parsing_exception: title=secret")}
	err := retrying(s, false).Handle(context.Background(), envelope(t, "job.closed", map[string]any{"job_id": jobID, "version": 2}), nil)
	require.Error(t, err)
	assert.True(t, kafka.IsPermanent(err))
	assert.NotContains(t, err.Error(), "secret")
}

func TestHandle_ShutdownStopsRetrying(t *testing.T) {
	s := &flakyStore{fakeStore: newFakeStore("jobs_v1"), n: 1 << 30, err: errors.New("connection refused")}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := retrying(s, true).Handle(ctx, envelope(t, "job.closed", map[string]any{"job_id": jobID, "version": 2}), nil)
	require.Error(t, err)
	assert.False(t, kafka.IsPermanent(err), "shutdown must not dead-letter")
	assert.Less(t, time.Since(start), time.Second)
}

func TestHandle_StoreCallsHaveATimeout(t *testing.T) {
	var deadline bool
	s := &deadlineStore{fakeStore: newFakeStore("jobs_v1"), saw: &deadline}
	p := newProjector(s, nil)
	p.CallTimeout = time.Second
	require.NoError(t, p.Handle(context.Background(), envelope(t, "job.closed", map[string]any{"job_id": jobID, "version": 2}), nil))
	assert.True(t, deadline)
}

type deadlineStore struct {
	*fakeStore
	saw *bool
}

func (d *deadlineStore) Put(ctx context.Context, index string, doc projection.Document) (bool, error) {
	_, *d.saw = ctx.Deadline()
	return d.fakeStore.Put(ctx, index, doc)
}
