package app_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
)

type fakePurger struct {
	calls []uuid.UUID
	stats app.PurgeStats
	err   error
}

func (f *fakePurger) PurgeUser(_ context.Context, user uuid.UUID, _ time.Time) (app.PurgeStats, error) {
	f.calls = append(f.calls, user)
	return f.stats, f.err
}

func userDeleted(t *testing.T, user uuid.UUID) event.Envelope {
	t.Helper()
	env, err := userdeletion.NewDeletedEvent(context.Background(), userdeletion.Deleted{
		UserID: user, Role: "candidate", RequestID: uuid.New(), RequestedAt: time.Now().UTC(),
	}, time.Now())
	require.NoError(t, err)
	return env
}

func TestHandleUserDeleted_PurgesAndConfirms(t *testing.T) {
	e := newEnv(t)
	p := &fakePurger{stats: app.PurgeStats{Cancelled: 1, Pseudonymised: 2}}
	e.svc.Purger = p
	user := uuid.New()
	ev := userDeleted(t, user)

	require.NoError(t, e.svc.HandleUserDeleted(context.Background(), ev))
	require.NoError(t, e.svc.HandleUserDeleted(context.Background(), ev), "redelivery")

	assert.Equal(t, []uuid.UUID{user}, p.calls)
	require.Len(t, e.ob.evs, 1, "no application.status_changed for purge cancellations")
	got, err := userdeletion.ParsePurged(e.ob.evs[0])
	require.NoError(t, err)
	assert.Equal(t, userdeletion.ServiceApplication, got.Service)
	assert.Equal(t, user.String(), e.ob.keys[0])

	require.NoError(t, e.svc.HandleUserDeleted(context.Background(), userDeleted(t, user)), "republish")
	assert.Len(t, e.ob.evs, 2)
	assert.Len(t, p.calls, 2)
}

func TestHandleUserDeleted_ErrorsRollBack(t *testing.T) {
	e := newEnv(t)
	boom := errors.New("db down")
	e.svc.Purger = &fakePurger{err: boom}
	ev := userDeleted(t, uuid.New())
	assert.ErrorIs(t, e.svc.HandleUserDeleted(context.Background(), ev), boom)
	assert.Empty(t, e.ob.evs)
	assert.Empty(t, e.idem.seen, "the event is not marked processed")

	ev.Payload = []byte(`{}`)
	assert.ErrorIs(t, e.svc.HandleUserDeleted(context.Background(), ev), app.ErrPoisonEvent)
}
