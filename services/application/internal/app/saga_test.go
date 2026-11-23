package app_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

// step runs the worker once and expects n sagas to have been processed.
func (e *env) step(t *testing.T, n int) {
	t.Helper()
	got, err := e.svc.ProcessDue(context.Background())
	require.NoError(t, err)
	require.Equal(t, n, got, "sagas processed")
}

func (e *env) lastHistory(id uuid.UUID) domain.HistoryEntry {
	h := e.apps.history[id]
	return h[len(h)-1]
}

func TestSaga_ReserveOKMovesToScoring(t *testing.T) {
	e := newEnv(t)
	_, a := e.submit(t)
	e.clock.Advance(time.Second)
	e.step(t, 1)

	assert.Equal(t, []uuid.UUID{a.ID}, e.quota.reserved)
	got := e.apps.m[a.ID]
	assert.Equal(t, domain.StatusScreening, got.Status)
	assert.Equal(t, int64(2), got.Version)
	h := e.lastHistory(a.ID)
	assert.Equal(t, domain.StatusPending, *h.From)
	assert.Equal(t, domain.StatusScreening, h.To)
	assert.Equal(t, domain.ActorSystem, h.Actor)

	s := e.sagas.m[a.ID]
	assert.Equal(t, domain.StepScore, s.Step)
	assert.Equal(t, domain.SagaRunning, s.State)
	assert.Equal(t, t0.Add(time.Second), s.StepStartedAt)
	assert.Equal(t, t0.Add(time.Second), s.NextRunAt, "next step is due at once")
	assert.Zero(t, s.Attempts)
	assert.Zero(t, e.scorer.calls, "one step per claim")
}

func TestSaga_ScoreOKScreened(t *testing.T) {
	e := newEnv(t)
	_, a := e.submit(t)
	e.step(t, 1)
	e.step(t, 1)

	got := e.apps.m[a.ID]
	assert.Equal(t, domain.StatusScreened, got.Status)
	require.NotNil(t, got.Score)
	assert.InDelta(t, 87.5, *got.Score, 1e-9)
	assert.JSONEq(t, `{"matched_skills":["Go"]}`, string(got.Breakdown))
	assert.Equal(t, "fake-hash@768", got.ModelVersion)
	assert.Equal(t, domain.SagaCompleted, e.sagas.m[a.ID].State)

	changed := e.eventsOf(t, "application.status_changed")
	last := changed[len(changed)-1]
	assert.Equal(t, "SCREENED", last["to"])
	assert.InDelta(t, 87.5, last["score"], 1e-9)
	assert.Equal(t, "system", last["actor"])
	e.step(t, 0)
}

func TestSaga_QuotaExhaustedCancels(t *testing.T) {
	e := newEnv(t)
	e.quota.reserveErr = app.ErrQuotaExhausted
	_, a := e.submit(t)
	e.step(t, 1)

	got := e.apps.m[a.ID]
	assert.Equal(t, domain.StatusCancelled, got.Status)
	assert.Equal(t, domain.ReasonQuotaExceeded, got.Reason)
	assert.Equal(t, domain.ReasonQuotaExceeded, e.lastHistory(a.ID).Reason)
	assert.Equal(t, domain.SagaCompleted, e.sagas.m[a.ID].State)
	assert.Empty(t, e.quota.released, "nothing was reserved, nothing to give back")
	changed := e.eventsOf(t, "application.status_changed")
	assert.Equal(t, "quota_exceeded", changed[len(changed)-1]["reason"])
}

func TestSaga_ReserveTransientBacksOff(t *testing.T) {
	e := newEnv(t)
	e.quota.reserveErr = errBoom
	_, a := e.submit(t)

	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	for _, d := range want {
		e.step(t, 1)
		s := e.sagas.m[a.ID]
		assert.Equal(t, e.clock.Now().Add(d), s.NextRunAt)
		assert.Equal(t, domain.StepReserveQuota, s.Step)
		assert.Equal(t, "boom", s.LastError)
		e.step(t, 0) // not due yet
		e.clock.Advance(d)
	}
	assert.Equal(t, domain.StatusPending, e.apps.m[a.ID].Status)

	for range 7 {
		e.step(t, 1)
		e.clock.Advance(time.Minute)
	}
	e.clock.Advance(-time.Minute)
	assert.Equal(t, e.clock.Now().Add(30*time.Second), e.sagas.m[a.ID].NextRunAt, "backoff capped at 30s")
}

func TestSaga_ReserveDeadlineCompensates(t *testing.T) {
	e := newEnv(t)
	e.quota.reserveErr = errBoom
	_, a := e.submit(t)
	e.step(t, 1)
	e.clock.Advance(deadline)
	calls := len(e.quota.reserved)

	e.step(t, 1)
	assert.Len(t, e.quota.reserved, calls, "past the deadline the step is not tried again")
	s := e.sagas.m[a.ID]
	assert.Equal(t, domain.SagaCompensating, s.State)
	assert.Equal(t, domain.StepReleaseQuota, s.Step)
	assert.Equal(t, domain.ReasonTimeout, s.CancelReason)
	assert.Equal(t, domain.StatusPending, e.apps.m[a.ID].Status)

	e.step(t, 1)
	assert.Equal(t, []uuid.UUID{a.ID}, e.quota.released)
	got := e.apps.m[a.ID]
	assert.Equal(t, domain.StatusCancelled, got.Status)
	assert.Equal(t, domain.ReasonTimeout, got.Reason)
	assert.Equal(t, domain.SagaCompensated, e.sagas.m[a.ID].State)
}

func TestSaga_ReservationReleasedCompensates(t *testing.T) {
	e := newEnv(t)
	e.quota.reserveErr = app.ErrReservationReleased
	_, a := e.submit(t)
	e.step(t, 1)
	s := e.sagas.m[a.ID]
	assert.Equal(t, domain.SagaCompensating, s.State)
	assert.Equal(t, domain.ReasonTimeout, s.CancelReason)
}

func TestSaga_ScoreUnavailableManualReview(t *testing.T) {
	e := newEnv(t)
	e.scorer.err = app.ErrScorerUnavailable
	_, a := e.submit(t)
	e.step(t, 1)
	e.step(t, 1)

	got := e.apps.m[a.ID]
	assert.Equal(t, domain.StatusNeedsManualReview, got.Status)
	assert.Equal(t, domain.ReasonAIUnavailable, got.Reason)
	assert.Nil(t, got.Score)
	assert.Equal(t, domain.SagaCompleted, e.sagas.m[a.ID].State)
	assert.Empty(t, e.quota.released, "AI failure keeps the quota (spec §6.4)")
	assert.Equal(t, 1, e.scorer.calls)
}

func TestSaga_ScoreNotMatchedRetriesThenManualReview(t *testing.T) {
	e := newEnv(t)
	e.scorer.err = app.ErrNotMatched
	_, a := e.submit(t)
	e.step(t, 1) // reserve
	start := e.clock.Now()

	e.step(t, 1)
	s := e.sagas.m[a.ID]
	assert.Equal(t, domain.StepScore, s.Step)
	assert.Equal(t, e.clock.Now().Add(time.Second), s.NextRunAt)
	assert.Equal(t, domain.StatusScreening, e.apps.m[a.ID].Status)

	e.clock.Advance(nfWindow - time.Second)
	e.step(t, 1)
	assert.Equal(t, domain.StatusScreening, e.apps.m[a.ID].Status, "still inside the window")

	e.clock.t = start.Add(nfWindow + time.Second)
	e.step(t, 1)
	got := e.apps.m[a.ID]
	assert.Equal(t, domain.StatusNeedsManualReview, got.Status)
	assert.Equal(t, domain.ReasonNotMatched, got.Reason)
	assert.Equal(t, domain.SagaCompleted, e.sagas.m[a.ID].State)
}

func TestSaga_ScorePastDeadlineStillManualReviewNotCancel(t *testing.T) {
	e := newEnv(t)
	_, a := e.submit(t)
	e.step(t, 1) // reserve
	e.scorer.err = app.ErrScorerUnavailable
	e.clock.Advance(deadline + time.Minute)
	e.step(t, 1)
	assert.Equal(t, domain.StatusNeedsManualReview, e.apps.m[a.ID].Status)
	assert.Empty(t, e.quota.released)
}

func TestSaga_StaleVersionDiscarded(t *testing.T) {
	e := newEnv(t)
	_, a := e.submit(t)
	history, events := len(e.apps.history[a.ID]), len(e.ob.evs)
	// While Reserve is in flight the job is closed: the consumer bumps the
	// saga's version, so this worker's result must be dropped.
	e.quota.onReserve = func() {
		_, err := e.sagas.CancelRunningForJob(context.Background(), e.job, domain.ReasonJobClosed, e.clock.Now())
		require.NoError(t, err)
	}
	e.step(t, 1)

	assert.Equal(t, domain.StatusPending, e.apps.m[a.ID].Status)
	assert.Len(t, e.apps.history[a.ID], history)
	assert.Len(t, e.ob.evs, events)
	s := e.sagas.m[a.ID]
	assert.Equal(t, domain.SagaCompensating, s.State)
	assert.Equal(t, domain.ReasonJobClosed, s.CancelReason)

	e.quota.onReserve = nil
	e.step(t, 1)
	assert.Equal(t, []uuid.UUID{a.ID}, e.quota.released, "release covers the reserve that landed")
	got := e.apps.m[a.ID]
	assert.Equal(t, domain.StatusCancelled, got.Status)
	assert.Equal(t, domain.ReasonJobClosed, got.Reason)
}

func TestSaga_ReleaseRetriesForever(t *testing.T) {
	e := newEnv(t)
	e.quota.reserveErr = errBoom
	e.quota.releaseErr = errBoom
	_, a := e.submit(t)
	e.clock.Advance(deadline)
	e.step(t, 1) // -> compensating
	for range 12 {
		e.step(t, 1)
		e.clock.Advance(time.Minute)
	}
	s := e.sagas.m[a.ID]
	assert.Equal(t, domain.SagaCompensating, s.State)
	assert.Equal(t, domain.StatusPending, e.apps.m[a.ID].Status)
	assert.Len(t, e.quota.released, 12)

	e.quota.releaseErr = nil
	e.step(t, 1)
	assert.Equal(t, domain.StatusCancelled, e.apps.m[a.ID].Status)
}

func TestSaga_BatchProcessesSeveral(t *testing.T) {
	e := newEnv(t)
	for range 3 {
		e.submit(t)
	}
	e.step(t, 3)
	e.step(t, 3)
	for _, a := range e.apps.m {
		assert.Equal(t, domain.StatusScreened, a.Status)
	}
}
