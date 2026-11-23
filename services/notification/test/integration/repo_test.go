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

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/eventschema"
	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/app"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/domain"
)

func reset(t *testing.T) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `TRUNCATE email_outbox, notifications, users, jobs, processed_events, outbox`)
	require.NoError(t, err)
}

type nopMailer struct{}

func (nopMailer) Send(context.Context, string, string, string, string) error { return nil }

type countingPusher struct {
	mu sync.Mutex
	n  int
}

func (p *countingPusher) Push(uuid.UUID, domain.Notification) { p.mu.Lock(); p.n++; p.mu.Unlock() }
func (p *countingPusher) Disconnect(uuid.UUID)                {}

func newService(t *testing.T) (*app.Service, *postgres.Repo, *countingPusher) {
	t.Helper()
	reset(t)
	repo := postgres.NewRepo(pool)
	p := &countingPusher{}
	svc := app.NewService(app.Deps{
		Users: repo.Users(), Jobs: repo.Jobs(), Notifications: repo.Notifications(), Emails: repo.Emails(),
		Idem: postgres.NewIdempotency(pool), Tx: postgres.NewTxManager(pool), Mailer: nopMailer{}, Pusher: p,
		Outbox: postgres.NewOutboxWriter(pool),
	})
	return svc, repo, p
}

func raw(t *testing.T, typ string, payload map[string]any) []byte {
	t.Helper()
	env, err := event.New(context.Background(), "test", typ, 1, payload, time.Now())
	require.NoError(t, err)
	b, err := json.Marshal(env)
	require.NoError(t, err)
	return b
}

func count(t *testing.T, q string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), q, args...).Scan(&n))
	return n
}

func TestMigrations_UpDown(t *testing.T) {
	mg, err := newMigrate()
	require.NoError(t, err)
	defer func() { _, _ = mg.Close() }()
	require.NoError(t, mg.Down())
	require.NoError(t, mg.Up())
}

func TestConsumer_EndToEnd(t *testing.T) {
	svc, _, pusher := newService(t)
	ctx := context.Background()
	cand, rec, job := uuid.New(), uuid.New(), uuid.New()
	events := [][]byte{
		raw(t, "user.registered", map[string]any{"user_id": cand.String(), "email": "c@x.local", "role": "candidate"}),
		raw(t, "job.created", map[string]any{"job_id": job.String(), "recruiter_id": rec.String(), "title": "Go dev", "version": 1}),
		raw(t, "application.status_changed", map[string]any{"application_id": uuid.NewString(), "job_id": job.String(),
			"candidate_id": cand.String(), "recruiter_id": rec.String(), "from": "SCREENED", "to": "INTERVIEW",
			"reason": nil, "score": nil, "version": 4, "actor": rec.String()}),
	}
	for _, e := range events {
		require.NoError(t, svc.HandleEvent(ctx, e))
	}
	for _, e := range events { // redelivery
		require.NoError(t, svc.HandleEvent(ctx, e))
	}
	assert.Equal(t, 2, count(t, `SELECT count(*) FROM notifications WHERE user_id = $1`, cand))
	assert.Equal(t, 2, count(t, `SELECT count(*) FROM email_outbox WHERE to_addr = 'c@x.local' AND status = 'PENDING'`))
	assert.Equal(t, 2, pusher.n)

	list, _, unread, err := svc.List(authz.WithClaims(ctx, authz.Claims{Subject: cand.String(), Role: authz.RoleCandidate}), app.ListInput{})
	require.NoError(t, err)
	require.Len(t, list, 2)
	assert.Equal(t, 2, unread)
	assert.Equal(t, domain.TypeApplicationStatus, list[0].Type, "newest first")
	assert.Contains(t, list[0].Title, "Mời phỏng vấn")
	assert.Equal(t, job.String(), list[0].Data["job_id"])
}

func TestRepo_MarkReadKeepsFirstTimestamp(t *testing.T) {
	_, repo, _ := newService(t)
	ctx := context.Background()
	user := uuid.New()
	n := domain.Notification{ID: uuid.New(), UserID: user, Type: domain.TypeWelcome, Title: "t", Body: "b",
		Data: map[string]string{}, CreatedAt: time.Now().UTC().Truncate(time.Microsecond)}
	require.NoError(t, repo.Notifications().Create(ctx, n))
	first := time.Now().UTC().Truncate(time.Microsecond)
	got, err := repo.Notifications().MarkRead(ctx, n.ID, user, first)
	require.NoError(t, err)
	got2, err := repo.Notifications().MarkRead(ctx, n.ID, user, first.Add(time.Hour))
	require.NoError(t, err)
	assert.True(t, got.ReadAt.Equal(first))
	assert.True(t, got2.ReadAt.Equal(first))
	_, err = repo.Notifications().MarkRead(ctx, n.ID, uuid.New(), first)
	assert.ErrorIs(t, err, domain.ErrNotificationNotFound)
	unread, err := repo.Notifications().CountUnread(ctx, user)
	require.NoError(t, err)
	assert.Zero(t, unread)
}

func TestRepo_JobVersionGuardAndTombstone(t *testing.T) {
	_, repo, _ := newService(t)
	ctx := context.Background()
	id, rec := uuid.New(), uuid.New()
	ok, err := repo.Jobs().Upsert(ctx, domain.Job{ID: id, RecruiterID: rec, Title: "v2", Version: 2})
	require.NoError(t, err)
	assert.True(t, ok)
	ok, err = repo.Jobs().Upsert(ctx, domain.Job{ID: id, RecruiterID: rec, Title: "v1", Version: 1})
	require.NoError(t, err)
	assert.False(t, ok)
	_, err = repo.Jobs().Upsert(ctx, domain.Job{ID: id, Version: 3, Deleted: true})
	require.NoError(t, err)
	j, err := repo.Jobs().Get(ctx, id)
	require.NoError(t, err)
	assert.True(t, j.Deleted)
	assert.Equal(t, rec, j.RecruiterID, "tombstone keeps the owner")
	assert.Equal(t, "v2", j.Title)
}

func TestRepo_EmailClaimSkipLocked(t *testing.T) {
	_, repo, _ := newService(t)
	ctx := context.Background()
	now := time.Now().UTC()
	for range 20 {
		n := domain.Notification{ID: uuid.New(), UserID: uuid.New(), Type: domain.TypeWelcome, Title: "t", Body: "b",
			Data: map[string]string{}, CreatedAt: now}
		require.NoError(t, repo.Notifications().Create(ctx, n))
		require.NoError(t, repo.Emails().Enqueue(ctx, domain.Email{ID: uuid.New(), NotificationID: n.ID, To: "a@x.local",
			Subject: "s", HTML: "h", Text: "t", NextAttemptAt: now, CreatedAt: now}))
	}
	var wg sync.WaitGroup
	got := make([][]domain.Email, 2)
	for i := range got {
		wg.Go(func() {
			var err error
			got[i], err = repo.Emails().ClaimDue(ctx, now, time.Minute, 10)
			assert.NoError(t, err)
		})
	}
	wg.Wait()
	seen := map[uuid.UUID]bool{}
	for _, batch := range got {
		for _, e := range batch {
			assert.False(t, seen[e.ID], "claimed twice")
			seen[e.ID] = true
		}
	}
	assert.Len(t, seen, 20)
	again, err := repo.Emails().ClaimDue(ctx, now.Add(30*time.Second), time.Minute, 10)
	require.NoError(t, err)
	assert.Empty(t, again, "leased")

	id := got[0][0].ID
	require.NoError(t, repo.Emails().MarkRetry(ctx, id, 1, now, "smtp down"))
	require.NoError(t, repo.Emails().MarkSent(ctx, got[0][1].ID, now))
	require.NoError(t, repo.Emails().MarkFailed(ctx, got[0][2].ID, 8, "gave up"))
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM email_outbox WHERE status = 'SENT'`))
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM email_outbox WHERE status = 'FAILED' AND attempts = 8`))
	due, err := repo.Emails().ClaimDue(ctx, now, time.Minute, 10)
	require.NoError(t, err)
	require.Len(t, due, 1, "the retried email is due again")
	assert.Equal(t, id, due[0].ID)
	assert.Equal(t, 1, due[0].Attempts)
}

func TestPurge_UserDeleted(t *testing.T) {
	svc, repo, _ := newService(t)
	ctx := context.Background()
	cand, rec, job := uuid.New(), uuid.New(), uuid.New()
	for _, e := range [][]byte{
		raw(t, "user.registered", map[string]any{"user_id": cand.String(), "email": "c@x.local", "role": "candidate"}),
		raw(t, "user.registered", map[string]any{"user_id": rec.String(), "email": "r@x.local", "role": "recruiter"}),
		raw(t, "job.created", map[string]any{"job_id": job.String(), "recruiter_id": cand.String(), "title": "Go dev", "version": 1}),
	} {
		require.NoError(t, svc.HandleEvent(ctx, e))
	}
	del, err := userdeletion.NewDeletedEvent(ctx, userdeletion.Deleted{
		UserID: cand, Role: "candidate", RequestID: uuid.New(), RequestedAt: time.Now(),
	}, time.Now())
	require.NoError(t, err)
	b, err := json.Marshal(del)
	require.NoError(t, err)
	require.NoError(t, svc.HandleEvent(ctx, b))
	require.NoError(t, svc.HandleEvent(ctx, b), "redelivery")

	assert.Zero(t, count(t, `SELECT count(*) FROM notifications WHERE user_id = $1`, cand))
	assert.Zero(t, count(t, `SELECT count(*) FROM email_outbox WHERE to_addr = 'c@x.local'`))
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM email_outbox WHERE to_addr = 'r@x.local'`))
	u, err := repo.Users().Get(ctx, cand)
	require.NoError(t, err)
	assert.True(t, u.Deleted)
	assert.Empty(t, u.Email)
	j, err := repo.Jobs().Get(ctx, job)
	require.NoError(t, err)
	assert.Equal(t, uuid.Nil, j.RecruiterID)

	// A late user.registered cannot bring the address back.
	require.NoError(t, svc.HandleEvent(ctx, raw(t, "user.registered", map[string]any{"user_id": cand.String(), "email": "c@x.local", "role": "candidate"})))
	u, err = repo.Users().Get(ctx, cand)
	require.NoError(t, err)
	assert.Empty(t, u.Email)
	assert.Zero(t, count(t, `SELECT count(*) FROM notifications WHERE user_id = $1`, cand))

	var payload []byte
	require.NoError(t, pool.QueryRow(ctx, `SELECT payload FROM outbox WHERE event_type = $1`, userdeletion.TopicPurged).Scan(&payload))
	assert.NoError(t, eventschema.Validate(userdeletion.TopicPurged, payload))
}
