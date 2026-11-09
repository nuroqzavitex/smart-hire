//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/app"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

type fastHasher struct{}

func (fastHasher) Hash(pw string) (string, error) { return "h:" + pw, nil }
func (fastHasher) Verify(pw, hash string) (bool, error) {
	return hash == "h:"+pw, nil
}

type fakeIssuer struct{}

func (fakeIssuer) IssueAccess(u domain.User, _ time.Time) (string, time.Duration, error) {
	return "access-" + u.ID.String(), 15 * time.Minute, nil
}

// failingOutbox writes the event and then fails, simulating a late error.
type failingOutbox struct{ inner app.OutboxWriter }

func (f failingOutbox) Write(ctx context.Context, e event.Envelope, id string) error {
	if err := f.inner.Write(ctx, e, id); err != nil {
		return err
	}
	return errors.New("outbox boom")
}

type cancelMarker struct{}

// cancelAfterCreate cancels after a successful Create, but only for calls
// whose ctx carries cancelMarker.
type cancelAfterCreate struct {
	inner  app.RefreshTokenRepo
	cancel func()
}

func (c cancelAfterCreate) Create(ctx context.Context, t domain.RefreshToken) error {
	if err := c.inner.Create(ctx, t); err != nil {
		return err
	}
	if ctx.Value(cancelMarker{}) != nil {
		c.cancel()
	}
	return nil
}

func (c cancelAfterCreate) GetByHashForUpdate(ctx context.Context, h []byte) (domain.RefreshToken, error) {
	return c.inner.GetByHashForUpdate(ctx, h)
}

func (c cancelAfterCreate) Revoke(ctx context.Context, id uuid.UUID, at time.Time, by *uuid.UUID) error {
	return c.inner.Revoke(ctx, id, at, by)
}

func (c cancelAfterCreate) RevokeFamily(ctx context.Context, fam uuid.UUID, at time.Time) error {
	return c.inner.RevokeFamily(ctx, fam, at)
}

func user(email string) domain.User {
	now := time.Now().UTC().Truncate(time.Microsecond)
	return domain.User{
		ID: uuid.New(), Email: email, PasswordHash: "h", Role: authz.RoleCandidate,
		Status: domain.StatusActive, CreatedAt: now, UpdatedAt: now,
	}
}

func login(t *testing.T, svc *app.Service) app.TokenPair {
	t.Helper()
	_, err := svc.Register(context.Background(), "l@x.com", "password1", authz.RoleCandidate)
	require.NoError(t, err)
	p, err := svc.Login(context.Background(), "l@x.com", "password1")
	require.NoError(t, err)
	return p
}

func count(t *testing.T, q string) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), q).Scan(&n))
	return n
}

func TestUserRepo_DuplicateEmailCaseInsensitive(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	users := postgres.NewUserRepo(pool)
	require.NoError(t, users.Create(ctx, user("a@x.com")))
	assert.ErrorIs(t, users.Create(ctx, user("A@X.com")), apperr.ErrConflict)
	u, err := users.GetByEmail(ctx, "A@x.COM")
	require.NoError(t, err)
	assert.Equal(t, "a@x.com", u.Email)
	assert.Equal(t, domain.StatusActive, u.Status)
	got, err := users.GetByID(ctx, u.ID)
	require.NoError(t, err)
	assert.Equal(t, u.ID, got.ID)
	_, err = users.GetByID(ctx, uuid.New())
	assert.ErrorIs(t, err, apperr.ErrNotFound)
}

func TestRegister_WritesOutbox(t *testing.T) {
	truncate(t)
	svc := newService(t, postgres.NewOutboxWriter(pool))
	_, err := svc.Register(context.Background(), "o@x.com", "password1", authz.RoleCandidate)
	require.NoError(t, err)
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM outbox WHERE event_type='user.registered' AND payload->>'event_id' IS NOT NULL`))
}

func TestRegister_AtomicWithOutbox(t *testing.T) {
	truncate(t)
	svc := newService(t, failingOutbox{inner: postgres.NewOutboxWriter(pool)})
	_, err := svc.Register(context.Background(), "x@x.com", "password1", authz.RoleCandidate)
	require.Error(t, err)
	assert.Equal(t, 0, count(t, `SELECT count(*) FROM users`))
	assert.Equal(t, 0, count(t, `SELECT count(*) FROM outbox`))
}

func TestRefresh_RotatesAndLogout(t *testing.T) {
	truncate(t)
	svc := newService(t, postgres.NewOutboxWriter(pool))
	ctx := context.Background()
	p1 := login(t, svc)
	p2, err := svc.Refresh(ctx, p1.RefreshToken)
	require.NoError(t, err)
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM refresh_tokens WHERE revoked_at IS NULL`))
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM refresh_tokens WHERE replaced_by IS NOT NULL`))
	require.NoError(t, svc.Logout(ctx, p2.RefreshToken))
	assert.Equal(t, 0, count(t, `SELECT count(*) FROM refresh_tokens WHERE revoked_at IS NULL`))
}

func TestRefresh_ReuseRevokesFamilyPersisted(t *testing.T) {
	truncate(t)
	svc := newService(t, postgres.NewOutboxWriter(pool))
	p1 := login(t, svc)
	_, err := svc.Refresh(context.Background(), p1.RefreshToken)
	require.NoError(t, err)
	_, err = svc.Refresh(context.Background(), p1.RefreshToken)
	require.ErrorIs(t, err, apperr.ErrUnauthenticated)
	assert.Equal(t, 0, count(t, `SELECT count(*) FROM refresh_tokens WHERE revoked_at IS NULL`))
}

func TestRefresh_CancelledMidRotationKeepsOldToken(t *testing.T) {
	truncate(t)
	cctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := context.WithValue(cctx, cancelMarker{}, true)
	svc := newServiceWithTokens(t, cancelAfterCreate{inner: postgres.NewRefreshTokenRepo(pool), cancel: cancel})
	p1 := login(t, svc) // background ctx has no marker, so no cancel
	_, err := svc.Refresh(ctx, p1.RefreshToken)
	require.Error(t, err)
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM refresh_tokens`)) // new RT rolled back
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM refresh_tokens WHERE revoked_at IS NULL`))
	_, err = newService(t, postgres.NewOutboxWriter(pool)).Refresh(context.Background(), p1.RefreshToken)
	assert.NoError(t, err) // old RT still valid
}

func TestRefresh_ConcurrentSameToken(t *testing.T) {
	truncate(t)
	svc := newService(t, postgres.NewOutboxWriter(pool))
	p1 := login(t, svc)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = svc.Refresh(context.Background(), p1.RefreshToken)
		}()
	}
	wg.Wait()
	ok := 0
	for _, e := range errs {
		if e == nil {
			ok++
		}
	}
	assert.Equal(t, 1, ok)
	assert.Equal(t, 0, count(t, `SELECT count(*) FROM refresh_tokens WHERE revoked_at IS NULL`))
}

func TestWithTx_PanicRollsBack(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	users := postgres.NewUserRepo(pool)
	txm := postgres.NewTxManager(pool)
	u := user("p@x.com")
	assert.Panics(t, func() {
		_ = txm.WithTx(ctx, func(ctx context.Context) error {
			require.NoError(t, users.Create(ctx, u))
			panic("boom")
		})
	})
	assert.Equal(t, 0, count(t, `SELECT count(*) FROM users`))
	assert.Equal(t, int32(0), pool.Stat().AcquiredConns())
}

func TestMigrations_UpDown(t *testing.T) {
	mg, err := newMigrate()
	require.NoError(t, err)
	defer func() { _, _ = mg.Close() }()
	require.NoError(t, mg.Down())
	require.NoError(t, mg.Up())
}
