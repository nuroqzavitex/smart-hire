package app_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

var (
	errJobNotFound = apperr.New(apperr.ErrNotFound, "job not found")
	errCVNotFound  = apperr.New(apperr.ErrNotFound, "cv not found")
)

func TestSubmit_CreatesPendingWithSagaAndEvents(t *testing.T) {
	e := newEnv(t)
	cand, a := e.submit(t)

	assert.Equal(t, domain.StatusPending, a.Status)
	assert.Equal(t, cand, a.CandidateID)
	assert.Equal(t, e.recruiter, a.RecruiterID, "recruiter copied from the job")
	assert.Equal(t, e.job, a.JobID)
	assert.Equal(t, e.cv, a.CVID)
	assert.Equal(t, int64(1), a.Version)
	assert.Equal(t, t0, a.CreatedAt)
	assert.Equal(t, a, e.apps.m[a.ID])

	s := e.sagas.m[a.ID]
	assert.Equal(t, domain.StepReserveQuota, s.Step)
	assert.Equal(t, domain.SagaRunning, s.State)
	assert.Equal(t, t0, s.NextRunAt)
	assert.Equal(t, t0.Add(deadline), s.DeadlineAt)
	assert.Equal(t, t0, s.StepStartedAt)
	assert.Equal(t, e.job, s.JobID)
	assert.Equal(t, int64(1), s.Version)

	h := e.apps.history[a.ID]
	require.Len(t, h, 1)
	assert.Nil(t, h[0].From)
	assert.Equal(t, domain.StatusPending, h[0].To)
	assert.Equal(t, cand.String(), h[0].Actor)

	sub := e.eventsOf(t, "application.submitted")
	require.Len(t, sub, 1)
	assert.Equal(t, a.ID.String(), sub[0]["application_id"])
	assert.Equal(t, e.recruiter.String(), sub[0]["recruiter_id"])
	changed := e.eventsOf(t, "application.status_changed")
	require.Len(t, changed, 1)
	assert.Nil(t, changed[0]["from"])
	assert.Equal(t, "PENDING", changed[0]["to"])
	assert.Equal(t, []string{a.ID.String(), a.ID.String()}, e.ob.keys)
	assert.Equal(t, 1, e.wakes, "worker woken after commit")
}

func TestSubmit_SameKeySameRequestReturnsExisting(t *testing.T) {
	e := newEnv(t)
	cand := uuid.New()
	in := app.SubmitInput{JobID: e.job, CVID: e.cv, IdempotencyKey: "k1"}
	first, err := e.svc.Submit(asCandidate(cand), in)
	require.NoError(t, err)
	jobCalls, events := e.jobs.calls, len(e.ob.evs)

	again, err := e.svc.Submit(asCandidate(cand), in)
	require.NoError(t, err)
	assert.Equal(t, first, again)
	assert.Equal(t, 1, e.apps.creates)
	assert.Equal(t, jobCalls, e.jobs.calls, "replay does not call the job service")
	assert.Len(t, e.ob.evs, events)
}

func TestSubmit_SameKeyDifferentRequestRejected(t *testing.T) {
	e := newEnv(t)
	cand := uuid.New()
	_, err := e.svc.Submit(asCandidate(cand), app.SubmitInput{JobID: e.job, CVID: e.cv, IdempotencyKey: "k1"})
	require.NoError(t, err)
	_, err = e.svc.Submit(asCandidate(cand), app.SubmitInput{JobID: e.job, CVID: uuid.New(), IdempotencyKey: "k1"})
	assert.ErrorIs(t, err, apperr.ErrValidation)
}

func TestSubmit_ConcurrentSameKeyReReads(t *testing.T) {
	e := newEnv(t)
	cand := uuid.New()
	winner := domain.Application{
		ID: uuid.New(), JobID: e.job, CandidateID: cand, RecruiterID: e.recruiter, CVID: e.cv,
		Status: domain.StatusPending, IdempotencyKey: "k1", Version: 1, CreatedAt: t0, UpdatedAt: t0,
	}
	winner.RequestHash = app.RequestHash(e.job, e.cv)
	// The other request commits between our key lookup and our insert.
	e.apps.createErr = domain.ErrIdempotencyKeyConflict
	e.svc.Applications = &racingApps{fakeApps: e.apps, winner: winner}

	got, err := e.svc.Submit(asCandidate(cand), app.SubmitInput{JobID: e.job, CVID: e.cv, IdempotencyKey: "k1"})
	require.NoError(t, err)
	assert.Equal(t, winner.ID, got.ID)
	assert.Empty(t, e.ob.evs, "the loser writes nothing")
	assert.Zero(t, e.wakes)
}

// racingApps hides the winner from the first GetByKey, as if it committed
// right after that read.
type racingApps struct {
	*fakeApps
	winner domain.Application
	reads  int
}

func (r *racingApps) GetByKey(context.Context, uuid.UUID, string) (domain.Application, error) {
	r.reads++
	if r.reads == 1 {
		return domain.Application{}, domain.ErrApplicationNotFound
	}
	return r.winner, nil
}

func TestSubmit_KeyRequired(t *testing.T) {
	e := newEnv(t)
	for _, key := range []string{"", strings.Repeat("k", 129)} {
		_, err := e.svc.Submit(asCandidate(uuid.New()), app.SubmitInput{JobID: e.job, CVID: e.cv, IdempotencyKey: key})
		assert.ErrorIs(t, err, apperr.ErrValidation, "key length %d", len(key))
	}
	assert.Zero(t, e.jobs.calls)
}

func TestSubmit_OnlyCandidates(t *testing.T) {
	e := newEnv(t)
	in := app.SubmitInput{JobID: e.job, CVID: e.cv, IdempotencyKey: "k"}
	_, err := e.svc.Submit(asRecruiter(uuid.New()), in)
	assert.ErrorIs(t, err, apperr.ErrForbidden)
	_, err = e.svc.Submit(context.Background(), in)
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
	assert.Zero(t, e.jobs.calls)
}

func TestSubmit_JobNotOpen(t *testing.T) {
	e := newEnv(t)
	e.jobs.m[e.job] = app.JobInfo{ID: e.job, RecruiterID: e.recruiter, Open: false}
	_, err := e.svc.Submit(asCandidate(uuid.New()), app.SubmitInput{JobID: e.job, CVID: e.cv, IdempotencyKey: "k"})
	assert.ErrorIs(t, err, apperr.ErrConflict)
	assert.Empty(t, e.apps.m)
	assert.Empty(t, e.ob.evs)
}

func TestSubmit_CVNotParsed(t *testing.T) {
	e := newEnv(t)
	e.cvs.m[e.cv] = app.CVInfo{ID: e.cv, Parsed: false}
	_, err := e.svc.Submit(asCandidate(uuid.New()), app.SubmitInput{JobID: e.job, CVID: e.cv, IdempotencyKey: "k"})
	assert.ErrorIs(t, err, apperr.ErrConflict)
	assert.Empty(t, e.apps.m)
}

func TestSubmit_NotFoundPassThrough(t *testing.T) {
	e := newEnv(t)
	_, err := e.svc.Submit(asCandidate(uuid.New()), app.SubmitInput{JobID: uuid.New(), CVID: e.cv, IdempotencyKey: "k"})
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	_, err = e.svc.Submit(asCandidate(uuid.New()), app.SubmitInput{JobID: e.job, CVID: uuid.New(), IdempotencyKey: "k"})
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	assert.Empty(t, e.apps.m)
}

func TestSubmit_AlreadyApplied(t *testing.T) {
	e := newEnv(t)
	cand := uuid.New()
	_, err := e.svc.Submit(asCandidate(cand), app.SubmitInput{JobID: e.job, CVID: e.cv, IdempotencyKey: "k1"})
	require.NoError(t, err)
	events := len(e.ob.evs)
	_, err = e.svc.Submit(asCandidate(cand), app.SubmitInput{JobID: e.job, CVID: e.cv, IdempotencyKey: "k2"})
	assert.ErrorIs(t, err, apperr.ErrConflict)
	assert.Len(t, e.apps.m, 1)
	assert.Len(t, e.ob.evs, events)
}

func TestSubmit_DependencyUnavailable(t *testing.T) {
	e := newEnv(t)
	e.cvs.err = apperr.New(apperr.ErrUnavailable, "candidate temporarily unavailable")
	_, err := e.svc.Submit(asCandidate(uuid.New()), app.SubmitInput{JobID: e.job, CVID: e.cv, IdempotencyKey: "k"})
	assert.ErrorIs(t, err, apperr.ErrUnavailable)
	assert.Empty(t, e.apps.m)
	assert.Empty(t, e.sagas.m)
}
