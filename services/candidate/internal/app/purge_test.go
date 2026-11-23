package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

func userDeleted(t *testing.T, user uuid.UUID) event.Envelope {
	t.Helper()
	env, err := userdeletion.NewDeletedEvent(context.Background(), userdeletion.Deleted{
		UserID: user, Role: "candidate", RequestID: uuid.New(), RequestedAt: time.Now().UTC(),
	}, time.Now())
	require.NoError(t, err)
	return env
}

func (e *env) withQuota() *fakeQuota {
	q := &fakeQuota{}
	e.svc.Quota = q
	return q
}

func eventsOfType(evs []event.Envelope, typ string) []event.Envelope {
	var out []event.Envelope
	for _, ev := range evs {
		if ev.EventType == typ {
			out = append(out, ev)
		}
	}
	return out
}

func TestHandleUserDeleted_DeletesFilesTombstonesAndConfirms(t *testing.T) {
	e := newEnv(t)
	q := e.withQuota()
	_, err := e.svc.UpsertProfile(asCandidate(u1), domain.ProfileInput{FullName: "Nguyen Van A"})
	require.NoError(t, err)
	parsed := e.parsed(t, u1)
	pending := e.newUpload(t, u1)
	foreign := e.parsed(t, u2)
	before := e.ob.count()

	ev := userDeleted(t, u1)
	require.NoError(t, e.svc.HandleUserDeleted(context.Background(), ev))

	_, err = e.cands.Get(context.Background(), u1)
	assert.ErrorIs(t, err, domain.ErrProfileNotFound, "profile deleted")
	for _, id := range []uuid.UUID{parsed.ID, pending.ID} {
		cv := e.cvs.m[id]
		assert.Equal(t, domain.StatusDeleted, cv.Status)
		assert.Empty(t, cv.FileName)
		assert.Nil(t, cv.ParsedData)
		_, stored := e.storage.objs[cv.ObjectKey]
		assert.False(t, stored, "file removed from storage")
	}
	assert.Equal(t, domain.StatusParsed, e.cvs.m[foreign.ID].Status, "other candidates untouched")
	_, stored := e.storage.objs[foreign.ObjectKey]
	assert.True(t, stored)
	assert.Equal(t, []uuid.UUID{u1}, q.purged)

	evs := e.ob.evs[before:]
	updated := eventsOfType(evs, "cv.updated")
	require.Len(t, updated, 2)
	for _, u := range updated {
		assert.JSONEq(t, `"DELETED"`, string(field(t, u, "status")))
		assert.JSONEq(t, `null`, string(field(t, u, "parsed")))
	}
	purged := eventsOfType(evs, userdeletion.TopicPurged)
	require.Len(t, purged, 1)
	p, err := userdeletion.ParsePurged(purged[0])
	require.NoError(t, err)
	assert.Equal(t, userdeletion.ServiceCandidate, p.Service)
	assert.Equal(t, u1, p.UserID)
	assert.Equal(t, "candidate-service", purged[0].Source)
	assert.Equal(t, u1.String(), e.ob.lastKey(), "keyed by user")
	assert.ElementsMatch(t, []string{parsed.ID.String(), pending.ID.String()}, e.ob.purgedAggregates,
		"older cv.updated rows (they hold the parsed CV) are dropped")
}

func TestHandleUserDeleted_Idempotent(t *testing.T) {
	e := newEnv(t)
	e.withQuota()
	e.parsed(t, u1)
	ev := userDeleted(t, u1)
	require.NoError(t, e.svc.HandleUserDeleted(context.Background(), ev))
	n := e.ob.count()
	require.NoError(t, e.svc.HandleUserDeleted(context.Background(), ev))
	assert.Equal(t, n, e.ob.count(), "redelivery of the same event is a no-op")

	// A republished user.deleted (new event ID) confirms again without
	// re-emitting cv.updated for the tombstones.
	require.NoError(t, e.svc.HandleUserDeleted(context.Background(), userDeleted(t, u1)))
	evs := e.ob.evs[n:]
	require.Len(t, evs, 1)
	assert.Equal(t, userdeletion.TopicPurged, evs[0].EventType)
}

func TestHandleUserDeleted_RecruiterWithNoDataStillConfirms(t *testing.T) {
	e := newEnv(t)
	e.withQuota()
	require.NoError(t, e.svc.HandleUserDeleted(context.Background(), userDeleted(t, uuid.New())))
	require.Equal(t, 1, e.ob.count())
	assert.Equal(t, userdeletion.TopicPurged, e.ob.last().EventType)
}

func TestHandleUserDeleted_StorageErrorRollsBack(t *testing.T) {
	e := newEnv(t)
	e.withQuota()
	cv := e.parsed(t, u1)
	before := e.ob.count()
	e.storage.deleteErr = errors.New("minio down")
	ev := userDeleted(t, u1)
	err := e.svc.HandleUserDeleted(context.Background(), ev)
	require.Error(t, err)
	assert.NotErrorIs(t, err, app.ErrPoisonEvent, "retried, not dead-lettered")
	assert.Equal(t, domain.StatusParsed, e.cvs.m[cv.ID].Status)
	assert.Equal(t, before, e.ob.count())

	e.storage.deleteErr = nil
	require.NoError(t, e.svc.HandleUserDeleted(context.Background(), ev), "the retry succeeds")
	assert.Equal(t, domain.StatusDeleted, e.cvs.m[cv.ID].Status)
}

func TestHandleUserDeleted_Poison(t *testing.T) {
	e := newEnv(t)
	e.withQuota()
	ev := userDeleted(t, u1)
	ev.Payload = json.RawMessage(`{"user_id":"nope"}`)
	assert.ErrorIs(t, e.svc.HandleUserDeleted(context.Background(), ev), app.ErrPoisonEvent)
}

func TestLateParseResultAfterPurgeIsIgnored(t *testing.T) {
	e := newEnv(t)
	e.withQuota()
	cv := e.uploaded(t, u1)
	require.NoError(t, e.svc.HandleUserDeleted(context.Background(), userDeleted(t, u1)))
	n := e.ob.count()
	raw := parsedEvent(t, cv, cv.AttemptID, fixture(t, "valid/full.json"))
	require.NoError(t, e.svc.HandleParseResult(context.Background(), raw))
	assert.Equal(t, domain.StatusDeleted, e.cvs.m[cv.ID].Status)
	assert.Nil(t, e.cvs.m[cv.ID].ParsedData, "no PII comes back")
	assert.Equal(t, n, e.ob.count())
}

func TestGetCV_TombstoneIsNotFound(t *testing.T) {
	e := newEnv(t)
	e.withQuota()
	cv := e.parsed(t, u1)
	require.NoError(t, e.svc.HandleUserDeleted(context.Background(), userDeleted(t, u1)))
	_, err := e.svc.GetCV(asCandidate(u1), cv.ID)
	assert.ErrorIs(t, err, domain.ErrCVNotFound)
}

func field(t *testing.T, ev event.Envelope, name string) json.RawMessage {
	t.Helper()
	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(ev.Payload, &m))
	return m[name]
}
