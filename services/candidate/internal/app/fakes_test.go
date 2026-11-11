package app_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/eventschema"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

var errBoom = errors.New("boom")

// fakeClock ticks one second on every Now call; Advance jumps further.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(time.Second)
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// attemptSeq yields distinct, schema-valid ULID-shaped attempt IDs.
type attemptSeq struct {
	mu sync.Mutex
	n  int
}

func (a *attemptSeq) Next() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.n++
	return fmt.Sprintf("01HATTEMPT%016d", a.n)
}

type fakeCandidates struct {
	mu sync.Mutex
	m  map[uuid.UUID]domain.Candidate
}

func (f *fakeCandidates) Get(_ context.Context, id uuid.UUID) (domain.Candidate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.m[id]
	if !ok {
		return domain.Candidate{}, domain.ErrProfileNotFound
	}
	return c, nil
}

func (f *fakeCandidates) Upsert(_ context.Context, c domain.Candidate) (domain.Candidate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if old, ok := f.m[c.UserID]; ok {
		c.CreatedAt = old.CreatedAt
	}
	f.m[c.UserID] = c
	return c, nil
}

type fakeCVs struct {
	mu sync.Mutex
	m  map[uuid.UUID]domain.CV
}

func (f *fakeCVs) Create(_ context.Context, cv domain.CV) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[cv.ID] = cv
	return nil
}

func (f *fakeCVs) Get(_ context.Context, id uuid.UUID) (domain.CV, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cv, ok := f.m[id]
	if !ok {
		return domain.CV{}, domain.ErrCVNotFound
	}
	return cv, nil
}

func (f *fakeCVs) GetForUpdate(ctx context.Context, id uuid.UUID) (domain.CV, error) {
	return f.Get(ctx, id)
}

func (f *fakeCVs) Update(_ context.Context, cv domain.CV) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[cv.ID] = cv
	return nil
}

// ListByCandidate implements real keyset semantics: created_at DESC, id DESC.
func (f *fakeCVs) ListByCandidate(_ context.Context, cid uuid.UUID, after *app.Cursor, limit int) ([]domain.CV, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var all []domain.CV
	for _, cv := range f.m {
		if cv.CandidateID == cid {
			all = append(all, cv)
		}
	}
	sort.Slice(all, func(a, b int) bool {
		if !all[a].CreatedAt.Equal(all[b].CreatedAt) {
			return all[a].CreatedAt.After(all[b].CreatedAt)
		}
		return all[a].ID.String() > all[b].ID.String()
	})
	var out []domain.CV
	for _, cv := range all {
		if after != nil && (cv.CreatedAt.After(after.At) ||
			cv.CreatedAt.Equal(after.At) && cv.ID.String() >= after.ID.String()) {
			continue
		}
		out = append(out, cv)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

type object struct {
	size        int64
	contentType string
	head        string
}

type fakeStorage struct {
	mu                 sync.Mutex
	objs               map[string]object
	presignContentType string
	presignTTL         time.Duration
	stats, deletes     int
}

func (f *fakeStorage) PresignPut(_ context.Context, key, contentType string, ttl time.Duration) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.presignContentType, f.presignTTL = contentType, ttl
	return "https://minio.test/" + key + "?sig=x", nil
}

func (f *fakeStorage) Stat(_ context.Context, key string) (app.ObjectInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stats++
	o, ok := f.objs[key]
	if !ok {
		return app.ObjectInfo{}, app.ErrObjectNotFound
	}
	return app.ObjectInfo{Size: o.size, ContentType: o.contentType}, nil
}

func (f *fakeStorage) ReadHead(_ context.Context, key string, n int64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objs[key]
	if !ok {
		return nil, app.ErrObjectNotFound
	}
	h := []byte(o.head)
	return h[:min(int(n), len(h))], nil
}

func (f *fakeStorage) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes++
	delete(f.objs, key)
	return nil
}

func (f *fakeStorage) put(key string, o object) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objs[key] = o
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

type fakeIdem struct {
	mu   sync.Mutex
	seen map[string]bool
}

func (f *fakeIdem) Once(_ context.Context, id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.seen[id] {
		return false, nil
	}
	f.seen[id] = true
	return true, nil
}

// fakeTx snapshots every fake store and restores them when fn fails.
type fakeTx struct {
	cands *fakeCandidates
	cvs   *fakeCVs
	ob    *fakeOutbox
	idem  *fakeIdem
}

func (t fakeTx) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	cands := cloneMap(t.cands.m)
	cvs := cloneMap(t.cvs.m)
	seen := cloneMap(t.idem.seen)
	n := len(t.ob.evs)
	if err := fn(ctx); err != nil {
		t.cands.m, t.cvs.m, t.idem.seen = cands, cvs, seen
		t.ob.evs, t.ob.keys = t.ob.evs[:n], t.ob.keys[:n]
		return err
	}
	return nil
}

func cloneMap[K comparable, V any](m map[K]V) map[K]V {
	out := make(map[K]V, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// schemaValidator uses the real contracts/events schemas.
type schemaValidator struct{}

func (schemaValidator) ValidateEvent(eventType string, doc []byte) error {
	return eventschema.Validate(eventType, doc)
}

func (schemaValidator) ValidateParsedCV(doc []byte) error {
	return eventschema.ValidateDef("cv.parsed", "ParsedCV", doc)
}
