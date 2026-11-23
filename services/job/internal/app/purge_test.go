package app_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

type fakeIdem struct{ seen map[string]bool }

func (f *fakeIdem) Once(_ context.Context, id string) (bool, error) {
	if f.seen[id] {
		return false, nil
	}
	f.seen[id] = true
	return true, nil
}

func withIdem(d *app.Deps, _ *fakeJobs) { d.Idem = &fakeIdem{seen: map[string]bool{}} }

func userDeleted(t *testing.T, user uuid.UUID) event.Envelope {
	t.Helper()
	env, err := userdeletion.NewDeletedEvent(context.Background(), userdeletion.Deleted{
		UserID: user, Role: "recruiter", RequestID: uuid.New(), RequestedAt: time.Now().UTC(),
	}, time.Now())
	require.NoError(t, err)
	return env
}

func TestHandleUserDeleted_DeletesAndAnonymisesTheRecruitersJobs(t *testing.T) {
	e := newEnv(t, withIdem)
	open := e.create(t, r1)
	_, err := e.svc.Publish(asRecruiter(r1), open.ID)
	require.NoError(t, err)
	draft := e.create(t, r1)
	foreign := e.create(t, r2)
	before := e.ob.count()

	require.NoError(t, e.svc.HandleUserDeleted(context.Background(), userDeleted(t, r1)))

	for _, id := range []uuid.UUID{open.ID, draft.ID} {
		j := e.jobs.m[id]
		assert.Equal(t, uuid.Nil, j.RecruiterID)
		assert.NotNil(t, j.DeletedAt)
		assert.NotEqual(t, domain.StatusOpen, j.Status)
	}
	assert.Equal(t, r2, e.jobs.m[foreign.ID].RecruiterID, "other recruiters untouched")
	assert.Nil(t, e.jobs.m[foreign.ID].DeletedAt)

	evs := e.ob.evs[before:]
	require.Len(t, evs, 3)
	deleted := map[string]int64{}
	for _, ev := range evs[:2] {
		require.Equal(t, "job.deleted", ev.EventType)
		var p struct {
			JobID   string `json:"job_id"`
			Version int64  `json:"version"`
		}
		require.NoError(t, json.Unmarshal(ev.Payload, &p))
		deleted[p.JobID] = p.Version
	}
	assert.Equal(t, e.jobs.m[open.ID].Version, deleted[open.ID.String()])
	assert.Equal(t, e.jobs.m[draft.ID].Version, deleted[draft.ID.String()])
	p, err := userdeletion.ParsePurged(evs[2])
	require.NoError(t, err)
	assert.Equal(t, userdeletion.ServiceJob, p.Service)
	assert.Equal(t, r1.String(), e.ob.lastKey())
}

func TestHandleUserDeleted_CandidateOnlyConfirms(t *testing.T) {
	e := newEnv(t, withIdem)
	e.create(t, r2)
	before := e.ob.count()
	require.NoError(t, e.svc.HandleUserDeleted(context.Background(), userDeleted(t, uuid.New())))
	require.Equal(t, before+1, e.ob.count())
	assert.Equal(t, userdeletion.TopicPurged, e.ob.last().EventType)
}

func TestHandleUserDeleted_DuplicateAndRepublish(t *testing.T) {
	e := newEnv(t, withIdem)
	e.create(t, r1)
	ev := userDeleted(t, r1)
	require.NoError(t, e.svc.HandleUserDeleted(context.Background(), ev))
	n := e.ob.count()
	require.NoError(t, e.svc.HandleUserDeleted(context.Background(), ev))
	assert.Equal(t, n, e.ob.count(), "same event: no-op")
	require.NoError(t, e.svc.HandleUserDeleted(context.Background(), userDeleted(t, r1)))
	require.Equal(t, n+1, e.ob.count(), "republish: confirm again, no second job.deleted")
	assert.Equal(t, userdeletion.TopicPurged, e.ob.last().EventType)
}

func TestHandleUserDeleted_Poison(t *testing.T) {
	e := newEnv(t, withIdem)
	ev := userDeleted(t, r1)
	ev.EventType = "user.registered"
	assert.ErrorIs(t, e.svc.HandleUserDeleted(context.Background(), ev), app.ErrPoisonEvent)
}

func TestHandleUserDeleted_OutboxErrorRollsBack(t *testing.T) {
	e := newEnv(t, withIdem)
	j := e.create(t, r1)
	e.ob.err = errBoom
	assert.ErrorIs(t, e.svc.HandleUserDeleted(context.Background(), userDeleted(t, r1)), errBoom)
	assert.Equal(t, r1, e.jobs.m[j.ID].RecruiterID)
}
