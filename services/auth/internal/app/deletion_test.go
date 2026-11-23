package app_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
	"github.com/nuroqzavitex/smarthire/pkg/otel/oteltest"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/app"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

type delEnv struct {
	env
	dels  *fakeDeletions
	idem  *fakeIdem
	clock *time.Time
}

func newDelEnv(t *testing.T) delEnv {
	t.Helper()
	clock := now
	e := delEnv{
		env:  env{users: newFakeUsers(), tokens: &fakeTokens{}, outbox: &fakeOutbox{}, hasher: &fakeHasher{}},
		dels: newFakeDeletions(), idem: &fakeIdem{seen: map[string]bool{}}, clock: &clock,
	}
	svc, err := app.NewService(app.Deps{
		Users: e.users, Tokens: e.tokens, Outbox: e.outbox, Tx: fakeTx{},
		Hasher: e.hasher, Issuer: fakeIssuer{},
		Deletions: e.dels, Idem: e.idem,
		DeletionRepublishAfter: 15 * time.Minute, DeletionStuckAfter: 24 * time.Hour,
		Now:        func() time.Time { return *e.clock },
		RefreshTTL: 7 * 24 * time.Hour,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	e.svc = svc
	return e
}

func (e delEnv) register(t *testing.T, email string, role authz.Role) domain.User {
	t.Helper()
	id, err := e.svc.CreateUser(context.Background(), email, "password1", role)
	require.NoError(t, err)
	e.outbox.events = nil
	u, err := e.users.GetByID(context.Background(), id)
	require.NoError(t, err)
	return u
}

func purgedEnv(t *testing.T, r domain.DeletionRequest, service string) event.Envelope {
	t.Helper()
	env, err := userdeletion.NewPurgedEvent(context.Background(),
		userdeletion.Deleted{UserID: r.UserID, Role: string(r.Role), RequestID: r.ID, RequestedAt: r.RequestedAt},
		service, now)
	require.NoError(t, err)
	return env
}

func TestDeleteMe_MarksPendingRevokesAndEmits(t *testing.T) {
	e := newDelEnv(t)
	u := e.register(t, "c@x.com", authz.RoleCandidate)
	_, err := e.svc.Login(context.Background(), "c@x.com", "password1")
	require.NoError(t, err)

	r, err := e.svc.DeleteMe(context.Background(), u.ID, "password1")
	require.NoError(t, err)

	assert.Equal(t, domain.DeletionPending, r.Status)
	assert.Equal(t, userdeletion.Services, r.Expected)
	got, _ := e.users.GetByID(context.Background(), u.ID)
	assert.Equal(t, domain.StatusDeletionPending, got.Status)
	for _, tok := range e.tokens.all() {
		assert.NotNil(t, tok.RevokedAt, "every refresh token is revoked")
	}
	require.Len(t, e.outbox.events, 1)
	ev := e.outbox.events[0]
	d, err := userdeletion.ParseDeleted(ev)
	require.NoError(t, err)
	assert.Equal(t, userdeletion.Deleted{UserID: u.ID, Role: "candidate", RequestID: r.ID, RequestedAt: now}, d)
	assert.NotContains(t, string(ev.Payload), "c@x.com", "no PII in user.deleted")
}

func TestDeleteMe_WrongPassword(t *testing.T) {
	e := newDelEnv(t)
	u := e.register(t, "c@x.com", authz.RoleCandidate)
	_, err := e.svc.DeleteMe(context.Background(), u.ID, "nope-nope")
	assert.ErrorIs(t, err, domain.ErrInvalidPassword)
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
	assert.Empty(t, e.outbox.events)
	got, _ := e.users.GetByID(context.Background(), u.ID)
	assert.Equal(t, domain.StatusActive, got.Status)
}

func TestDeleteMe_UnknownUser(t *testing.T) {
	e := newDelEnv(t)
	_, err := e.svc.DeleteMe(context.Background(), uuid.New(), "password1")
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
}

func TestDeleteMe_AdminForbidden(t *testing.T) {
	e := newDelEnv(t)
	u := e.register(t, "admin@x.com", authz.RoleAdmin)
	_, err := e.svc.DeleteMe(context.Background(), u.ID, "password1")
	assert.ErrorIs(t, err, domain.ErrAdminDeletion)
	assert.Empty(t, e.outbox.events)
}

func TestDeleteMe_RepeatReturnsSameRequest(t *testing.T) {
	e := newDelEnv(t)
	u := e.register(t, "c@x.com", authz.RoleCandidate)
	first, err := e.svc.DeleteMe(context.Background(), u.ID, "password1")
	require.NoError(t, err)
	again, err := e.svc.DeleteMe(context.Background(), u.ID, "password1")
	require.NoError(t, err)
	assert.Equal(t, first.ID, again.ID)
	assert.Len(t, e.outbox.events, 1, "no second user.deleted")
	_, err = e.svc.DeleteMe(context.Background(), u.ID, "wrong-pass")
	assert.ErrorIs(t, err, domain.ErrInvalidPassword, "the password is checked on a repeat too")
}

func TestDeleteMe_ConcurrentCreateConflictReturnsWinner(t *testing.T) {
	e := newDelEnv(t)
	u := e.register(t, "c@x.com", authz.RoleCandidate)
	winner := domain.NewDeletionRequest(u.ID, u.Role, userdeletion.Services, now)
	// Another request committed between our lookup and insert.
	e.dels.beforeCreate = func() { e.dels.put(winner) }
	got, err := e.svc.DeleteMe(context.Background(), u.ID, "password1")
	require.NoError(t, err)
	assert.Equal(t, winner.ID, got.ID)
}

func TestHandleDataPurged_CompletesAfterAllFive(t *testing.T) {
	e := newDelEnv(t)
	u := e.register(t, "c@x.com", authz.RoleCandidate)
	r, err := e.svc.DeleteMe(context.Background(), u.ID, "password1")
	require.NoError(t, err)

	for i, svc := range userdeletion.Services {
		require.NoError(t, e.svc.HandleDataPurged(context.Background(), purgedEnv(t, r, svc)))
		_, err := e.users.GetByID(context.Background(), u.ID)
		if i < len(userdeletion.Services)-1 {
			require.NoError(t, err, "user kept until every service confirmed")
		} else {
			assert.ErrorIs(t, err, apperr.ErrNotFound, "user hard-deleted")
		}
	}
	got := e.dels.get(r.ID)
	assert.Equal(t, domain.DeletionCompleted, got.Status)
	assert.ElementsMatch(t, userdeletion.Services, got.Purged)
	assert.Equal(t, []uuid.UUID{u.ID}, e.users.hardDeleted)
}

func TestHandleDataPurged_DuplicateAndAfterCompleted(t *testing.T) {
	e := newDelEnv(t)
	u := e.register(t, "c@x.com", authz.RoleCandidate)
	r, err := e.svc.DeleteMe(context.Background(), u.ID, "password1")
	require.NoError(t, err)
	dup := purgedEnv(t, r, "job")
	require.NoError(t, e.svc.HandleDataPurged(context.Background(), dup))
	require.NoError(t, e.svc.HandleDataPurged(context.Background(), dup))
	assert.Equal(t, []string{"job"}, e.dels.get(r.ID).Purged)

	for _, svc := range userdeletion.Services {
		require.NoError(t, e.svc.HandleDataPurged(context.Background(), purgedEnv(t, r, svc)))
	}
	// A confirmation of a republished user.deleted after completion.
	require.NoError(t, e.svc.HandleDataPurged(context.Background(), purgedEnv(t, r, "ai")))
	assert.Len(t, e.users.hardDeleted, 1, "hard delete runs once")
}

func TestHandleDataPurged_UnknownRequestIgnored(t *testing.T) {
	e := newDelEnv(t)
	r := domain.NewDeletionRequest(uuid.New(), authz.RoleCandidate, userdeletion.Services, now)
	assert.NoError(t, e.svc.HandleDataPurged(context.Background(), purgedEnv(t, r, "job")))
}

func TestHandleDataPurged_PoisonEvents(t *testing.T) {
	e := newDelEnv(t)
	u := e.register(t, "c@x.com", authz.RoleCandidate)
	r, err := e.svc.DeleteMe(context.Background(), u.ID, "password1")
	require.NoError(t, err)

	other := r
	other.UserID = uuid.New()
	err = e.svc.HandleDataPurged(context.Background(), purgedEnv(t, other, "job"))
	assert.ErrorIs(t, err, app.ErrPoisonEvent, "user_id does not match the request")

	bad := purgedEnv(t, r, "job")
	bad.Payload = json.RawMessage(`{"user_id":"x"}`)
	err = e.svc.HandleDataPurged(context.Background(), bad)
	assert.ErrorIs(t, err, app.ErrPoisonEvent)

	limited := domain.NewDeletionRequest(u.ID, u.Role, []string{"job"}, now)
	limited.ID = r.ID
	e.dels.put(limited)
	err = e.svc.HandleDataPurged(context.Background(), purgedEnv(t, r, "ai"))
	assert.ErrorIs(t, err, app.ErrPoisonEvent, "service not expected")
}

func TestSweep_RepublishesAndMarksStuck(t *testing.T) {
	e := newDelEnv(t)
	a := e.register(t, "a@x.com", authz.RoleCandidate)
	b := e.register(t, "b@x.com", authz.RoleRecruiter)
	ra, err := e.svc.DeleteMe(context.Background(), a.ID, "password1")
	require.NoError(t, err)

	*e.clock = now.Add(10 * time.Minute)
	rb, err := e.svc.DeleteMe(context.Background(), b.ID, "password1")
	require.NoError(t, err)
	e.outbox.events = nil

	*e.clock = now.Add(16 * time.Minute) // a is due again, b is not
	rep, stuck, err := e.svc.SweepDeletions(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, rep)
	assert.Zero(t, stuck)
	require.Len(t, e.outbox.events, 1)
	d, err := userdeletion.ParseDeleted(e.outbox.events[0])
	require.NoError(t, err)
	assert.Equal(t, ra.ID, d.RequestID, "same request, new event")
	assert.Equal(t, now, d.RequestedAt)
	assert.Equal(t, *e.clock, e.dels.get(ra.ID).LastPublishedAt)

	*e.clock = now.Add(25 * time.Hour)
	_, stuck, err = e.svc.SweepDeletions(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, stuck)
	assert.Equal(t, domain.DeletionStuck, e.dels.get(ra.ID).Status)
	assert.Equal(t, domain.DeletionStuck, e.dels.get(rb.ID).Status)

	// A STUCK request is still republished, so fixing the broken consumer
	// lets it complete.
	e.outbox.events = nil
	*e.clock = now.Add(26 * time.Hour)
	rep, stuck, err = e.svc.SweepDeletions(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, rep)
	assert.Zero(t, stuck)
	assert.Len(t, e.outbox.events, 2)
}

func TestGetDeletionRequest_AdminOnly(t *testing.T) {
	e := newDelEnv(t)
	u := e.register(t, "c@x.com", authz.RoleCandidate)
	r, err := e.svc.DeleteMe(context.Background(), u.ID, "password1")
	require.NoError(t, err)

	admin := authz.WithClaims(context.Background(), authz.Claims{Subject: uuid.NewString(), Role: authz.RoleAdmin})
	got, err := e.svc.GetDeletionRequest(admin, r.ID)
	require.NoError(t, err)
	assert.Equal(t, r.ID, got.ID)

	_, err = e.svc.GetDeletionRequest(admin, uuid.New())
	assert.ErrorIs(t, err, apperr.ErrNotFound)

	cand := authz.WithClaims(context.Background(), authz.Claims{Subject: u.ID.String(), Role: authz.RoleCandidate})
	_, err = e.svc.GetDeletionRequest(cand, r.ID)
	assert.ErrorIs(t, err, apperr.ErrForbidden)
	_, err = e.svc.GetDeletionRequest(context.Background(), r.ID)
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
}

func TestDeletionCounts(t *testing.T) {
	e := newDelEnv(t)
	u := e.register(t, "c@x.com", authz.RoleCandidate)
	_, err := e.svc.DeleteMe(context.Background(), u.ID, "password1")
	require.NoError(t, err)
	counts, err := e.svc.DeletionCounts(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(1), counts[domain.DeletionPending])
}

func TestDeletionGauge_ReportsEveryStatus(t *testing.T) {
	m := oteltest.Install(t)
	e := newDelEnv(t)
	u := e.register(t, "c@x.com", authz.RoleCandidate)
	_, err := e.svc.DeleteMe(context.Background(), u.ID, "password1")
	require.NoError(t, err)
	reg, err := app.RegisterDeletionGauge(otel.GetMeterProvider().Meter("t"), e.svc)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reg.Unregister() })
	for status, want := range map[string]int64{"PENDING": 1, "COMPLETED": 0, "STUCK": 0} {
		v, ok := m.Int64(t, "account.deletions", "status", status)
		assert.True(t, ok, status)
		assert.Equal(t, want, v, status)
	}
}
