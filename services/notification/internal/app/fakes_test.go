package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/app"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/domain"
)

var (
	t0      = time.Date(2026, 11, 23, 9, 0, 0, 0, time.UTC)
	errBoom = errors.New("smtp down")
)

type fakeUsers struct{ m map[uuid.UUID]domain.User }

func (f *fakeUsers) Upsert(_ context.Context, u domain.User) error { f.m[u.ID] = u; return nil }
func (f *fakeUsers) Get(_ context.Context, id uuid.UUID) (domain.User, error) {
	u, ok := f.m[id]
	if !ok {
		return domain.User{}, domain.ErrUserNotFound
	}
	return u, nil
}

type fakeJobs struct{ m map[uuid.UUID]domain.Job }

func (f *fakeJobs) Upsert(_ context.Context, j domain.Job) (bool, error) {
	if cur, ok := f.m[j.ID]; ok && cur.Version >= j.Version {
		return false, nil
	}
	f.m[j.ID] = j
	return true, nil
}
func (f *fakeJobs) Get(_ context.Context, id uuid.UUID) (domain.Job, error) {
	j, ok := f.m[id]
	if !ok {
		return domain.Job{}, domain.ErrJobNotFound
	}
	return j, nil
}

type fakeNotifications struct{ list []domain.Notification }

func (f *fakeNotifications) Create(_ context.Context, n domain.Notification) error {
	f.list = append(f.list, n)
	return nil
}

func (f *fakeNotifications) List(_ context.Context, userID uuid.UUID, unreadOnly bool, after *app.Cursor, limit int) ([]domain.Notification, error) {
	var out []domain.Notification
	for _, n := range f.list {
		if n.UserID != userID || unreadOnly && n.ReadAt != nil {
			continue
		}
		if after != nil && !n.CreatedAt.Before(after.At) && (!n.CreatedAt.Equal(after.At) || n.ID.String() >= after.ID.String()) {
			continue
		}
		out = append(out, n)
	}
	slices.SortFunc(out, func(a, b domain.Notification) int {
		if c := b.CreatedAt.Compare(a.CreatedAt); c != 0 {
			return c
		}
		if a.ID.String() > b.ID.String() {
			return -1
		}
		return 1
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeNotifications) CountUnread(_ context.Context, userID uuid.UUID) (int, error) {
	n := 0
	for _, x := range f.list {
		if x.UserID == userID && x.ReadAt == nil {
			n++
		}
	}
	return n, nil
}

func (f *fakeNotifications) MarkRead(_ context.Context, id, userID uuid.UUID, at time.Time) (domain.Notification, error) {
	for i, n := range f.list {
		if n.ID == id && n.UserID == userID {
			if n.ReadAt == nil {
				f.list[i].ReadAt = &at
			}
			return f.list[i], nil
		}
	}
	return domain.Notification{}, domain.ErrNotificationNotFound
}

type fakeEmails struct{ m map[uuid.UUID]domain.Email }

func (f *fakeEmails) Enqueue(_ context.Context, e domain.Email) error { f.m[e.ID] = e; return nil }

func (f *fakeEmails) ClaimDue(_ context.Context, now time.Time, lease time.Duration, limit int) ([]domain.Email, error) {
	var out []domain.Email
	for id, e := range f.m {
		if e.Status == domain.EmailPending && !e.NextAttemptAt.After(now) && len(out) < limit {
			e.NextAttemptAt = now.Add(lease)
			f.m[id] = e
			out = append(out, e)
		}
	}
	return out, nil
}

func (f *fakeEmails) MarkSent(_ context.Context, id uuid.UUID, _ time.Time) error {
	e := f.m[id]
	e.Status = domain.EmailSent
	f.m[id] = e
	return nil
}

func (f *fakeEmails) MarkRetry(_ context.Context, id uuid.UUID, attempts int, next time.Time, lastErr string) error {
	e := f.m[id]
	e.Attempts, e.NextAttemptAt, e.LastError = attempts, next, lastErr
	f.m[id] = e
	return nil
}

func (f *fakeEmails) MarkFailed(_ context.Context, id uuid.UUID, attempts int, lastErr string) error {
	e := f.m[id]
	e.Status, e.Attempts, e.LastError = domain.EmailFailed, attempts, lastErr
	f.m[id] = e
	return nil
}

func (f *fakeEmails) only(t *testing.T) domain.Email {
	t.Helper()
	require.Len(t, f.m, 1)
	for _, e := range f.m {
		return e
	}
	return domain.Email{}
}

type fakeIdem struct{ seen map[string]bool }

func (f *fakeIdem) Once(_ context.Context, id string) (bool, error) {
	if f.seen[id] {
		return false, nil
	}
	f.seen[id] = true
	return true, nil
}

// fakeTx restores the notification/email/user/job stores when fn fails.
type fakeTx struct{ e *env }

func (t fakeTx) WithTx(ctx context.Context, fn func(context.Context) error) error {
	n, emails, users, jobs, seen := slices.Clone(t.e.notes.list), clone(t.e.emails.m), clone(t.e.users.m), clone(t.e.jobs.m), clone(t.e.idem.seen)
	if t.e.txErr != nil {
		return t.e.txErr
	}
	if err := fn(ctx); err != nil {
		t.e.notes.list, t.e.emails.m, t.e.users.m, t.e.jobs.m, t.e.idem.seen = n, emails, users, jobs, seen
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

type fakeMailer struct {
	err  error
	sent []string // recipients
}

func (f *fakeMailer) Send(_ context.Context, to, _, _, _ string) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, to)
	return nil
}

type pushed struct {
	user uuid.UUID
	n    domain.Notification
}

type fakePusher struct{ got []pushed }

func (f *fakePusher) Push(userID uuid.UUID, n domain.Notification) {
	f.got = append(f.got, pushed{userID, n})
}

type env struct {
	svc    *app.Service
	now    time.Time
	users  *fakeUsers
	jobs   *fakeJobs
	notes  *fakeNotifications
	emails *fakeEmails
	idem   *fakeIdem
	mailer *fakeMailer
	pusher *fakePusher
	txErr  error
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{
		now:    t0,
		users:  &fakeUsers{m: map[uuid.UUID]domain.User{}},
		jobs:   &fakeJobs{m: map[uuid.UUID]domain.Job{}},
		notes:  &fakeNotifications{},
		emails: &fakeEmails{m: map[uuid.UUID]domain.Email{}},
		idem:   &fakeIdem{seen: map[string]bool{}},
		mailer: &fakeMailer{},
		pusher: &fakePusher{},
	}
	e.svc = app.NewService(app.Deps{
		Users: e.users, Jobs: e.jobs, Notifications: e.notes, Emails: e.emails, Idem: e.idem,
		Tx: fakeTx{e: e}, Mailer: e.mailer, Pusher: e.pusher, Now: func() time.Time { return e.now },
	})
	return e
}

// evt builds a raw envelope of typ with payload.
func evt(t *testing.T, typ string, payload map[string]any) []byte {
	t.Helper()
	env, err := event.New(context.Background(), "test", typ, 1, payload, t0)
	require.NoError(t, err)
	raw, err := json.Marshal(env)
	require.NoError(t, err)
	return raw
}

func (e *env) handle(t *testing.T, typ string, payload map[string]any) {
	t.Helper()
	require.NoError(t, e.svc.HandleEvent(context.Background(), evt(t, typ, payload)))
}

func as(id uuid.UUID, role authz.Role) context.Context {
	return authz.WithClaims(context.Background(), authz.Claims{Subject: id.String(), Role: role})
}
