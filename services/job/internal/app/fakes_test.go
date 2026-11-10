package app_test

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

type fakeJobs struct {
	mu   sync.Mutex
	m    map[uuid.UUID]domain.Job
	lock int // number of GetForUpdate calls, for ordering assertions
}

func newFakeJobs() *fakeJobs { return &fakeJobs{m: map[uuid.UUID]domain.Job{}} }

func (f *fakeJobs) Create(_ context.Context, j domain.Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[j.ID] = j
	return nil
}

func (f *fakeJobs) get(id uuid.UUID) (domain.Job, error) {
	j, ok := f.m[id]
	if !ok {
		return domain.Job{}, domain.ErrJobNotFound
	}
	return j, nil // deliberately returns deleted jobs: the use case must guard
}

func (f *fakeJobs) Get(_ context.Context, id uuid.UUID) (domain.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.get(id)
}

func (f *fakeJobs) GetForUpdate(_ context.Context, id uuid.UUID) (domain.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lock++
	return f.get(id)
}

func (f *fakeJobs) Update(_ context.Context, j domain.Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[j.ID] = j
	return nil
}

func (f *fakeJobs) Search(_ context.Context, flt app.SearchFilter, _ *app.Cursor, limit int) ([]app.SearchHit, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var all []domain.Job
	for _, j := range f.m {
		if j.Status == domain.StatusOpen && j.DeletedAt == nil {
			all = append(all, j)
		}
	}
	sortJobs(all)
	return f.page(all, nil, limit, func(j domain.Job) app.SearchHit {
		var r float32
		if flt.Query != "" {
			r = 0.5
		}
		return app.SearchHit{Job: j, Rank: r}
	}), nil
}

func (f *fakeJobs) ListByRecruiter(_ context.Context, rid uuid.UUID, st *domain.Status, _ *app.Cursor, limit int) ([]domain.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var all []domain.Job
	for _, j := range f.m {
		if j.RecruiterID == rid && j.DeletedAt == nil && (st == nil || j.Status == *st) {
			all = append(all, j)
		}
	}
	sortJobs(all)
	return all[:min(limit, len(all))], nil
}

// page ignores the cursor except by position: tests that need paging use
// pagingJobs below.
func (f *fakeJobs) page(all []domain.Job, _ *app.Cursor, limit int, mk func(domain.Job) app.SearchHit) []app.SearchHit {
	out := make([]app.SearchHit, 0, limit)
	for _, j := range all[:min(limit, len(all))] {
		out = append(out, mk(j))
	}
	return out
}

func sortJobs(js []domain.Job) {
	sort.Slice(js, func(a, b int) bool {
		if !js[a].CreatedAt.Equal(js[b].CreatedAt) {
			return js[a].CreatedAt.After(js[b].CreatedAt)
		}
		return js[a].ID.String() < js[b].ID.String()
	})
}

// pagingJobs wraps fakeJobs with real keyset semantics (ordering by
// PublishedAt desc, ID desc) for open jobs.
type pagingJobs struct{ *fakeJobs }

func (p pagingJobs) Search(_ context.Context, flt app.SearchFilter, after *app.Cursor, limit int) ([]app.SearchHit, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var all []domain.Job
	for _, j := range p.m {
		if j.Status == domain.StatusOpen && j.DeletedAt == nil {
			all = append(all, j)
		}
	}
	sort.Slice(all, func(a, b int) bool {
		pa, pb := *all[a].PublishedAt, *all[b].PublishedAt
		if !pa.Equal(pb) {
			return pa.After(pb)
		}
		return all[a].ID.String() > all[b].ID.String()
	})
	var out []app.SearchHit
	for _, j := range all {
		if after != nil {
			pj := *j.PublishedAt
			if pj.After(after.At) || pj.Equal(after.At) && j.ID.String() >= after.ID.String() {
				continue
			}
		}
		var r float32
		if flt.Query != "" {
			r = 0.5
		}
		out = append(out, app.SearchHit{Job: j, Rank: r})
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

type fakeOutbox struct {
	mu   sync.Mutex
	evs  []event.Envelope
	keys []string
	err  error
}

func (f *fakeOutbox) Write(_ context.Context, e event.Envelope, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.evs = append(f.evs, e)
	f.keys = append(f.keys, key)
	return nil
}

func (f *fakeOutbox) count() int { return len(f.evs) }
func (f *fakeOutbox) last() event.Envelope {
	return f.evs[len(f.evs)-1]
}
func (f *fakeOutbox) lastKey() string { return f.keys[len(f.keys)-1] }

// fakeTx snapshots jobs and outbox and restores them when fn fails.
type fakeTx struct {
	jobs *fakeJobs
	ob   *fakeOutbox
}

func (t fakeTx) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	snap := make(map[uuid.UUID]domain.Job, len(t.jobs.m))
	for k, v := range t.jobs.m {
		snap[k] = v
	}
	n := len(t.ob.evs)
	if err := fn(ctx); err != nil {
		t.jobs.m = snap
		t.ob.evs, t.ob.keys = t.ob.evs[:n], t.ob.keys[:n]
		return err
	}
	return nil
}

var errBoom = errors.New("boom")

// tickClock returns a strictly increasing clock.
func tickClock() func() time.Time {
	t := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		t = t.Add(time.Second)
		return t
	}
}
