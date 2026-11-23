package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/eventschema"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

var errBoom = errors.New("boom")

var t0 = time.Date(2026, 11, 23, 9, 0, 0, 0, time.UTC)

// clock is a settable time source; it does not tick by itself.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type fakeApps struct {
	m       map[uuid.UUID]domain.Application
	history map[uuid.UUID][]domain.HistoryEntry
	creates int
	// createErr, when set, is returned once by Create (simulates a race).
	createErr error
}

func (f *fakeApps) Create(_ context.Context, a domain.Application) error {
	if err := f.createErr; err != nil {
		f.createErr = nil
		return err
	}
	for _, o := range f.m {
		if o.CandidateID == a.CandidateID && o.IdempotencyKey == a.IdempotencyKey {
			return domain.ErrIdempotencyKeyConflict
		}
		if o.JobID == a.JobID && o.CandidateID == a.CandidateID && o.Status != domain.StatusCancelled {
			return domain.ErrAlreadyApplied
		}
	}
	f.creates++
	f.m[a.ID] = a
	return nil
}

func (f *fakeApps) Get(_ context.Context, id uuid.UUID) (domain.Application, error) {
	a, ok := f.m[id]
	if !ok {
		return domain.Application{}, domain.ErrApplicationNotFound
	}
	return a, nil
}

func (f *fakeApps) GetForUpdate(ctx context.Context, id uuid.UUID) (domain.Application, error) {
	return f.Get(ctx, id)
}

func (f *fakeApps) GetByKey(_ context.Context, cand uuid.UUID, key string) (domain.Application, error) {
	for _, a := range f.m {
		if a.CandidateID == cand && a.IdempotencyKey == key {
			return a, nil
		}
	}
	return domain.Application{}, domain.ErrApplicationNotFound
}

func (f *fakeApps) Update(_ context.Context, a domain.Application) error {
	f.m[a.ID] = a
	return nil
}

func (f *fakeApps) List(_ context.Context, flt app.ListFilter, after *app.Cursor, limit int) ([]domain.Application, error) {
	var out []domain.Application
	for _, a := range f.m {
		if flt.CandidateID != nil && a.CandidateID != *flt.CandidateID ||
			flt.RecruiterID != nil && a.RecruiterID != *flt.RecruiterID ||
			flt.JobID != nil && a.JobID != *flt.JobID ||
			flt.Status != nil && a.Status != *flt.Status {
			continue
		}
		if after != nil && !a.CreatedAt.Before(after.At) && (!a.CreatedAt.Equal(after.At) || a.ID.String() >= after.ID.String()) {
			continue
		}
		out = append(out, a)
	}
	slices.SortFunc(out, func(x, y domain.Application) int {
		if c := y.CreatedAt.Compare(x.CreatedAt); c != 0 {
			return c
		}
		if x.ID.String() > y.ID.String() {
			return -1
		}
		return 1
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeApps) AddHistory(_ context.Context, id uuid.UUID, h domain.HistoryEntry) error {
	f.history[id] = append(f.history[id], h)
	return nil
}

func (f *fakeApps) History(_ context.Context, id uuid.UUID) ([]domain.HistoryEntry, error) {
	return f.history[id], nil
}

type fakeSagas struct {
	m map[uuid.UUID]domain.Saga // by application id
	// onClaim runs at the end of every Claim.
	onClaim func()
}

func (f *fakeSagas) Create(_ context.Context, s domain.Saga) error {
	f.m[s.ApplicationID] = s
	return nil
}

func (f *fakeSagas) Claim(_ context.Context, now time.Time, lease time.Duration, limit int) ([]domain.Saga, error) {
	var due []domain.Saga
	for _, s := range f.m {
		if s.Active() && !s.NextRunAt.After(now) {
			due = append(due, s)
		}
	}
	slices.SortFunc(due, func(a, b domain.Saga) int { return a.NextRunAt.Compare(b.NextRunAt) })
	if len(due) > limit {
		due = due[:limit]
	}
	for i := range due {
		due[i].NextRunAt = now.Add(lease)
		due[i].Attempts++
		due[i].Version++
		f.m[due[i].ApplicationID] = due[i]
	}
	if f.onClaim != nil {
		f.onClaim()
	}
	return due, nil
}

func (f *fakeSagas) Save(_ context.Context, s domain.Saga) (bool, error) {
	cur, ok := f.m[s.ApplicationID]
	if !ok || cur.Version != s.Version {
		return false, nil
	}
	s.Version++
	f.m[s.ApplicationID] = s
	return true, nil
}

func (f *fakeSagas) GetByApplication(_ context.Context, id uuid.UUID) (domain.Saga, error) {
	s, ok := f.m[id]
	if !ok {
		return domain.Saga{}, domain.ErrApplicationNotFound
	}
	return s, nil
}

func (f *fakeSagas) CancelRunningForJob(_ context.Context, jobID uuid.UUID, reason string, now time.Time) (int, error) {
	n := 0
	for id, s := range f.m {
		if s.JobID == jobID && s.State == domain.SagaRunning {
			s.State, s.Step, s.CancelReason, s.NextRunAt, s.Attempts = domain.SagaCompensating, domain.StepReleaseQuota, reason, now, 0
			s.Version++
			f.m[id] = s
			n++
		}
	}
	return n, nil
}

type fakeOutbox struct {
	evs  []event.Envelope
	keys []string
}

func (f *fakeOutbox) Write(_ context.Context, e event.Envelope, key string) error {
	f.evs = append(f.evs, e)
	f.keys = append(f.keys, key)
	return nil
}

type fakeIdem struct{ seen map[string]bool }

func (f *fakeIdem) Once(_ context.Context, id string) (bool, error) {
	if f.seen[id] {
		return false, nil
	}
	f.seen[id] = true
	return true, nil
}

// fakeTx restores every fake store when fn fails.
type fakeTx struct {
	apps  *fakeApps
	sagas *fakeSagas
	ob    *fakeOutbox
	idem  *fakeIdem
}

func (t fakeTx) WithTx(ctx context.Context, fn func(context.Context) error) error {
	apps, sagas, seen := clone(t.apps.m), clone(t.sagas.m), clone(t.idem.seen)
	hist := make(map[uuid.UUID][]domain.HistoryEntry, len(t.apps.history))
	for k, v := range t.apps.history {
		hist[k] = slices.Clone(v)
	}
	n := len(t.ob.evs)
	if err := fn(ctx); err != nil {
		t.apps.m, t.apps.history, t.sagas.m, t.idem.seen = apps, hist, sagas, seen
		t.ob.evs, t.ob.keys = t.ob.evs[:n], t.ob.keys[:n]
		return err
	}
	return nil
}

func clone[K comparable, V any](m map[K]V) map[K]V {
	out := make(map[K]V, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

type fakeJobs struct {
	m     map[uuid.UUID]app.JobInfo
	err   error
	calls int
}

func (f *fakeJobs) GetJob(_ context.Context, id uuid.UUID) (app.JobInfo, error) {
	f.calls++
	if f.err != nil {
		return app.JobInfo{}, f.err
	}
	j, ok := f.m[id]
	if !ok {
		return app.JobInfo{}, errJobNotFound
	}
	return j, nil
}

type fakeCVs struct {
	m     map[uuid.UUID]app.CVInfo
	err   error
	calls int
}

func (f *fakeCVs) GetCV(_ context.Context, id uuid.UUID) (app.CVInfo, error) {
	f.calls++
	if f.err != nil {
		return app.CVInfo{}, f.err
	}
	c, ok := f.m[id]
	if !ok {
		return app.CVInfo{}, errCVNotFound
	}
	return c, nil
}

type fakeQuota struct {
	// onReserve runs inside Reserve (simulates something happening while
	// the call is in flight).
	onReserve  func()
	reserveErr error
	releaseErr error
	reserved   []uuid.UUID
	released   []uuid.UUID
}

func (f *fakeQuota) Reserve(_ context.Context, appID, _ uuid.UUID) error {
	f.reserved = append(f.reserved, appID)
	if f.onReserve != nil {
		f.onReserve()
	}
	return f.reserveErr
}

func (f *fakeQuota) Release(_ context.Context, appID uuid.UUID) error {
	f.released = append(f.released, appID)
	return f.releaseErr
}

type fakeScorer struct {
	score app.Score
	err   error
	calls int
}

func (f *fakeScorer) Score(context.Context, uuid.UUID, uuid.UUID) (app.Score, error) {
	f.calls++
	return f.score, f.err
}

type env struct {
	svc    *app.Service
	clock  *clock
	apps   *fakeApps
	sagas  *fakeSagas
	ob     *fakeOutbox
	idem   *fakeIdem
	jobs   *fakeJobs
	cvs    *fakeCVs
	quota  *fakeQuota
	scorer *fakeScorer
	wakes  int

	recruiter uuid.UUID
	job       uuid.UUID
	cv        uuid.UUID
}

const (
	deadline = 10 * time.Minute
	lease    = 30 * time.Second
	nfWindow = 2 * time.Minute
)

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{
		clock:     &clock{t: t0},
		apps:      &fakeApps{m: map[uuid.UUID]domain.Application{}, history: map[uuid.UUID][]domain.HistoryEntry{}},
		sagas:     &fakeSagas{m: map[uuid.UUID]domain.Saga{}},
		ob:        &fakeOutbox{},
		idem:      &fakeIdem{seen: map[string]bool{}},
		jobs:      &fakeJobs{m: map[uuid.UUID]app.JobInfo{}},
		cvs:       &fakeCVs{m: map[uuid.UUID]app.CVInfo{}},
		quota:     &fakeQuota{},
		scorer:    &fakeScorer{score: app.Score{Value: 87.5, Breakdown: json.RawMessage(`{"matched_skills":["Go"]}`), ModelVersion: "fake-hash@768"}},
		recruiter: uuid.New(),
		job:       uuid.New(),
		cv:        uuid.New(),
	}
	e.jobs.m[e.job] = app.JobInfo{ID: e.job, RecruiterID: e.recruiter, Open: true}
	e.cvs.m[e.cv] = app.CVInfo{ID: e.cv, Parsed: true}
	e.svc = app.NewService(app.Deps{
		Applications:        e.apps,
		Sagas:               e.sagas,
		Outbox:              e.ob,
		Idem:                e.idem,
		Tx:                  fakeTx{apps: e.apps, sagas: e.sagas, ob: e.ob, idem: e.idem},
		Jobs:                e.jobs,
		CVs:                 e.cvs,
		Quota:               e.quota,
		Scorer:              e.scorer,
		Now:                 e.clock.Now,
		Wake:                func() { e.wakes++ },
		SagaDeadline:        deadline,
		SagaLease:           lease,
		SagaBatch:           10,
		ScoreNotFoundWindow: nfWindow,
	})
	return e
}

func asCandidate(id uuid.UUID) context.Context {
	return authz.WithClaims(context.Background(), authz.Claims{Subject: id.String(), Role: authz.RoleCandidate})
}

func asRecruiter(id uuid.UUID) context.Context {
	return authz.WithClaims(context.Background(), authz.Claims{Subject: id.String(), Role: authz.RoleRecruiter})
}

func asAdmin() context.Context {
	return authz.WithClaims(context.Background(), authz.Claims{Subject: uuid.NewString(), Role: authz.RoleAdmin})
}

// submit applies as a fresh candidate and returns the candidate id and the
// application.
func (e *env) submit(t *testing.T) (uuid.UUID, domain.Application) {
	t.Helper()
	cand := uuid.New()
	a, err := e.svc.Submit(asCandidate(cand), app.SubmitInput{JobID: e.job, CVID: e.cv, IdempotencyKey: "key-" + cand.String()})
	require.NoError(t, err)
	return cand, a
}

// eventsOf returns the decoded payloads of the outbox events of one type,
// after checking every envelope against its JSON Schema.
func (e *env) eventsOf(t *testing.T, typ string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, ev := range e.ob.evs {
		raw, err := json.Marshal(ev)
		require.NoError(t, err)
		require.NoError(t, eventschema.Validate(ev.EventType, raw), "event %s", ev.EventType)
		if ev.EventType != typ {
			continue
		}
		var p map[string]any
		require.NoError(t, json.Unmarshal(ev.Payload, &p))
		out = append(out, p)
	}
	return out
}
