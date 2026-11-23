package app_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/app"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/domain"
)

func deletedRaw(t *testing.T, user uuid.UUID) []byte {
	t.Helper()
	env, err := userdeletion.NewDeletedEvent(context.Background(), userdeletion.Deleted{
		UserID: user, Role: "candidate", RequestID: uuid.New(), RequestedAt: time.Now().UTC(),
	}, time.Now())
	require.NoError(t, err)
	b, err := json.Marshal(env)
	require.NoError(t, err)
	return b
}

func TestUserDeleted_PurgesTombstonesAndConfirms(t *testing.T) {
	e := newEnv(t)
	cand, rec, job := uuid.New(), uuid.New(), uuid.New()
	e.handle(t, "user.registered", map[string]any{"user_id": cand.String(), "email": "c@x.local", "role": "candidate"})
	e.handle(t, "user.registered", map[string]any{"user_id": rec.String(), "email": "r@x.local", "role": "recruiter"})
	e.jobs.m[job] = domain.Job{ID: job, RecruiterID: cand, Title: "Go", Version: 1}
	require.Len(t, e.notes.list, 2)
	require.Len(t, e.emails.m, 2)

	raw := deletedRaw(t, cand)
	require.NoError(t, e.svc.HandleEvent(context.Background(), raw))
	require.NoError(t, e.svc.HandleEvent(context.Background(), raw), "redelivery")

	require.Len(t, e.notes.list, 1)
	assert.Equal(t, rec, e.notes.list[0].UserID, "other users keep theirs")
	require.Len(t, e.emails.m, 1)
	assert.Equal(t, "r@x.local", e.emails.only(t).To)
	u := e.users.m[cand]
	assert.True(t, u.Deleted)
	assert.Empty(t, u.Email, "the address is gone")
	assert.Equal(t, uuid.Nil, e.jobs.m[job].RecruiterID)
	assert.Equal(t, []uuid.UUID{cand}, e.pusher.disconnected, "open sockets are closed after commit")

	require.Len(t, e.ob.evs, 1)
	p, err := userdeletion.ParsePurged(e.ob.evs[0])
	require.NoError(t, err)
	assert.Equal(t, userdeletion.ServiceNotification, p.Service)
	assert.Equal(t, "notification-service", e.ob.evs[0].Source)
	assert.Equal(t, cand.String(), e.ob.keys[0])

	require.NoError(t, e.svc.HandleEvent(context.Background(), deletedRaw(t, cand)), "republish")
	assert.Len(t, e.ob.evs, 2)
}

func TestUserDeleted_LateEventsCreateNothing(t *testing.T) {
	e := newEnv(t)
	cand, rec, job, app1, cv := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	e.jobs.m[job] = domain.Job{ID: job, RecruiterID: rec, Title: "Go", Version: 1}
	require.NoError(t, e.svc.HandleEvent(context.Background(), deletedRaw(t, cand)))
	require.NoError(t, e.svc.HandleEvent(context.Background(), deletedRaw(t, rec)))

	e.handle(t, "cv.parsed", map[string]any{"cv_id": cv.String(), "candidate_id": cand.String(), "attempt_id": "a", "parsed": map[string]any{}, "model_version": "m"})
	e.handle(t, "application.status_changed", map[string]any{"application_id": app1.String(), "job_id": job.String(),
		"candidate_id": cand.String(), "recruiter_id": rec.String(), "from": "SCREENED", "to": "INTERVIEW",
		"reason": nil, "score": nil, "version": 4, "actor": rec.String()})
	e.handle(t, "match.completed", map[string]any{"job_id": job.String(), "model_version": "m",
		"top": []any{map[string]any{"candidate_id": uuid.NewString(), "cv_id": cv.String(), "score": 80}}})
	// A late user.registered must not bring the address back.
	e.handle(t, "user.registered", map[string]any{"user_id": cand.String(), "email": "c@x.local", "role": "candidate"})

	assert.Empty(t, e.notes.list)
	assert.Empty(t, e.emails.m)
	assert.Empty(t, e.pusher.got)
	assert.Empty(t, e.users.m[cand].Email)
}

func TestUserDeleted_Poison(t *testing.T) {
	e := newEnv(t)
	raw := []byte(`{"event_id":"01J0000000000000000000000X","event_type":"user.deleted","event_version":1,"payload":{"user_id":"x"}}`)
	assert.ErrorIs(t, e.svc.HandleEvent(context.Background(), raw), app.ErrPoisonEvent)
}
