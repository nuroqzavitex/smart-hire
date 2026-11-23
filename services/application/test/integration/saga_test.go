//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

func TestMigrations_UpDown(t *testing.T) {
	mg, err := newMigrate()
	require.NoError(t, err)
	defer func() { _, _ = mg.Close() }()
	require.NoError(t, mg.Down())
	require.NoError(t, mg.Up())
}

func TestRepo_UniqueIndexes(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	cand, job := uuid.New(), uuid.New()
	now := e.clock.Now()
	base := domain.Application{
		JobID: job, CandidateID: cand, RecruiterID: e.recruiter, CVID: uuid.New(), Status: domain.StatusPending,
		RequestHash: "h", Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	first := base
	first.ID, first.IdempotencyKey = uuid.New(), "k1"
	require.NoError(t, e.apps.Create(ctx, first))

	sameKey := base
	sameKey.ID, sameKey.JobID, sameKey.IdempotencyKey = uuid.New(), uuid.New(), "k1"
	assert.ErrorIs(t, e.apps.Create(ctx, sameKey), domain.ErrIdempotencyKeyConflict)

	sameJob := base
	sameJob.ID, sameJob.IdempotencyKey = uuid.New(), "k2"
	assert.ErrorIs(t, e.apps.Create(ctx, sameJob), domain.ErrAlreadyApplied)

	first.Status, first.Version = domain.StatusCancelled, 2
	require.NoError(t, e.apps.Update(ctx, first))
	require.NoError(t, e.apps.Create(ctx, sameJob), "a cancelled application does not block applying again")
}

func TestRepo_ListAndHistoryRoundTrip(t *testing.T) {
	e := newEnv(t)
	id := e.submit(t)
	e.clock.Advance(time.Second)
	id2 := e.submit(t)
	got, err := e.apps.List(context.Background(), app.ListFilter{RecruiterID: &e.recruiter}, nil, 10)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, id2, got[0].ID, "newest first")
	after := app.Cursor{At: got[0].CreatedAt, ID: got[0].ID}
	page, err := e.apps.List(context.Background(), app.ListFilter{RecruiterID: &e.recruiter}, &after, 10)
	require.NoError(t, err)
	require.Len(t, page, 1)
	assert.Equal(t, id, page[0].ID)
	st := domain.StatusScreened
	none, err := e.apps.List(context.Background(), app.ListFilter{Status: &st}, nil, 10)
	require.NoError(t, err)
	assert.Empty(t, none)

	h, err := e.apps.History(context.Background(), id)
	require.NoError(t, err)
	require.Len(t, h, 1)
	assert.Nil(t, h[0].From)
	assert.Equal(t, domain.StatusPending, h[0].To)
}

func TestClaim_TwoWorkersNoOverlap(t *testing.T) {
	e := newEnv(t)
	for range 20 {
		e.submit(t)
	}
	var wg sync.WaitGroup
	results := make([][]domain.Saga, 2)
	for i := range results {
		wg.Go(func() {
			var err error
			results[i], err = e.sagas.Claim(context.Background(), e.clock.Now(), 30*time.Second, 10)
			assert.NoError(t, err)
		})
	}
	wg.Wait()
	seen := map[uuid.UUID]bool{}
	for _, r := range results {
		for _, s := range r {
			assert.False(t, seen[s.ID], "saga claimed twice")
			seen[s.ID] = true
		}
	}
	assert.Len(t, seen, 20)
}

func TestClaim_LeaseExpiryReclaims(t *testing.T) {
	e := newEnv(t)
	id := e.submit(t)
	ctx := context.Background()
	first, err := e.sagas.Claim(ctx, e.clock.Now(), 30*time.Second, 10)
	require.NoError(t, err)
	require.Len(t, first, 1)

	none, err := e.sagas.Claim(ctx, e.clock.Now().Add(29*time.Second), 30*time.Second, 10)
	require.NoError(t, err)
	assert.Empty(t, none, "leased")

	// The first worker "crashed"; after the lease the saga is claimed again.
	again, err := e.sagas.Claim(ctx, e.clock.Now().Add(31*time.Second), 30*time.Second, 10)
	require.NoError(t, err)
	require.Len(t, again, 1)
	assert.Equal(t, id, again[0].ApplicationID)
	assert.Equal(t, 2, again[0].Attempts)
	assert.Greater(t, again[0].Version, first[0].Version)

	stale := first[0]
	stale.Step = domain.StepScore
	ok, err := e.sagas.Save(ctx, stale)
	require.NoError(t, err)
	assert.False(t, ok, "the crashed worker's late write is rejected")
	ok, err = e.sagas.Save(ctx, again[0])
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestSaga_EndToEndScreened(t *testing.T) {
	e := newEnv(t)
	id := e.submit(t)
	for range 2 {
		n, err := e.svc.ProcessDue(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, n)
	}
	a, err := e.apps.Get(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusScreened, a.Status)
	require.NotNil(t, a.Score)
	assert.InDelta(t, 72.25, *a.Score, 1e-9)
	assert.JSONEq(t, `{"matched_skills":["Go"]}`, string(a.Breakdown))
	assert.Equal(t, int64(3), a.Version)
	assert.Equal(t, 4, count(t, `SELECT count(*) FROM outbox WHERE aggregate_id = $1`, id.String()),
		"submitted + PENDING + SCREENING + SCREENED")
	assert.Equal(t, 3, count(t, `SELECT count(*) FROM application_history WHERE application_id = $1`, id))
	s, err := e.sagas.GetByApplication(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, domain.SagaCompleted, s.State)
}

func TestSaga_CandidateDownUntilDeadlineCancelled(t *testing.T) {
	e := newEnv(t)
	e.quota.reserveErr = context.DeadlineExceeded
	id := e.submit(t)
	for range 20 {
		_, err := e.svc.ProcessDue(context.Background())
		require.NoError(t, err)
		e.clock.Advance(15 * time.Second)
	}
	a, err := e.apps.Get(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusCancelled, a.Status)
	assert.Equal(t, domain.ReasonTimeout, a.Reason)
	assert.Equal(t, []uuid.UUID{id}, e.quota.releasedIDs())
	s, err := e.sagas.GetByApplication(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, domain.SagaCompensated, s.State)
}

func TestSaga_JobClosedMidway(t *testing.T) {
	e := newEnv(t)
	id := e.submit(t)
	a, err := e.apps.Get(context.Background(), id)
	require.NoError(t, err)
	_, err = e.svc.ProcessDue(context.Background()) // quota reserved, now SCREENING
	require.NoError(t, err)

	env, err := event.New(context.Background(), "job-service", "job.closed", 1,
		map[string]any{"job_id": a.JobID.String(), "version": 2}, e.clock.Now())
	require.NoError(t, err)
	raw, err := json.Marshal(env)
	require.NoError(t, err)
	require.NoError(t, e.svc.HandleJobEvent(context.Background(), raw))
	require.NoError(t, e.svc.HandleJobEvent(context.Background(), raw), "redelivery is a no-op")

	_, err = e.svc.ProcessDue(context.Background())
	require.NoError(t, err)
	a, err = e.apps.Get(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusCancelled, a.Status)
	assert.Equal(t, domain.ReasonJobClosed, a.Reason)
	assert.Equal(t, []uuid.UUID{id}, e.quota.releasedIDs())
}

func TestSubmit_ConcurrentSameKeyOneRow(t *testing.T) {
	e := newEnv(t)
	cand, job, cv := uuid.New(), uuid.New(), uuid.New()
	ids := make([]uuid.UUID, 10)
	var wg sync.WaitGroup
	for i := range ids {
		wg.Go(func() {
			a, err := e.svc.Submit(asCandidate(cand), app.SubmitInput{JobID: job, CVID: cv, IdempotencyKey: "same"})
			if assert.NoError(t, err) {
				ids[i] = a.ID
			}
		})
	}
	wg.Wait()
	for _, id := range ids {
		assert.Equal(t, ids[0], id)
	}
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM applications WHERE candidate_id = $1`, cand))
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM saga_instances`))
}

// A job.closed consumed while Submit's transaction was still open finds no
// saga to cancel; the recorded closed job still stops the saga.
func TestSaga_JobClosedBeforeSagaExistedCompensates(t *testing.T) {
	e := newEnv(t)
	id := e.submit(t)
	a, err := e.apps.Get(context.Background(), id)
	require.NoError(t, err)
	require.NoError(t, postgres.NewClosedJobRepo(pool).MarkClosed(context.Background(), a.JobID, e.clock.Now()))
	require.NoError(t, postgres.NewClosedJobRepo(pool).MarkClosed(context.Background(), a.JobID, e.clock.Now()), "idempotent")

	for range 2 {
		_, err := e.svc.ProcessDue(context.Background())
		require.NoError(t, err)
	}
	a, err = e.apps.Get(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusCancelled, a.Status)
	assert.Equal(t, domain.ReasonJobClosed, a.Reason)
	assert.Empty(t, e.quota.reserved)

	_, err = e.svc.Submit(asCandidate(uuid.New()), app.SubmitInput{JobID: a.JobID, CVID: uuid.New(), IdempotencyKey: "k"})
	assert.ErrorIs(t, err, apperr.ErrConflict, "new applications to the closed job are refused")
}
