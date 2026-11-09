package app_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/app"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

// refreshEnv is an env with a movable clock and a commit-recording tx.
type refreshEnv struct {
	env
	clock     *time.Time
	committed *bool
}

func newRefreshEnv(t *testing.T) refreshEnv {
	t.Helper()
	clock := now
	committed := new(bool)
	e := env{users: newFakeUsers(), tokens: &fakeTokens{}, outbox: &fakeOutbox{}, hasher: &fakeHasher{}}
	svc, err := app.NewService(app.Deps{
		Users: e.users, Tokens: e.tokens, Outbox: e.outbox, Tx: fakeTx{lastCommitted: committed},
		Hasher: e.hasher, Issuer: fakeIssuer{},
		Now:        func() time.Time { return clock },
		RefreshTTL: 7 * 24 * time.Hour,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	e.svc = svc
	r := refreshEnv{env: e, clock: &clock, committed: committed}
	_, err = svc.Register(context.Background(), "a@x.com", "password1", authz.RoleCandidate)
	require.NoError(t, err)
	return r
}

func (r refreshEnv) login(t *testing.T) app.TokenPair {
	t.Helper()
	p, err := r.svc.Login(context.Background(), "a@x.com", "password1")
	require.NoError(t, err)
	return p
}

func TestRefresh_Rotates(t *testing.T) {
	r := newRefreshEnv(t)
	p1 := r.login(t)
	p2, err := r.svc.Refresh(context.Background(), p1.RefreshToken)
	require.NoError(t, err)
	assert.NotEqual(t, p1.RefreshToken, p2.RefreshToken)
	assert.NotEmpty(t, p2.AccessToken)
	old, ok := r.tokens.byHash(p1.RefreshToken)
	require.True(t, ok)
	assert.NotNil(t, old.RevokedAt)
	nw, ok := r.tokens.byHash(p2.RefreshToken)
	require.True(t, ok)
	assert.Nil(t, nw.RevokedAt)
	assert.Equal(t, old.FamilyID, nw.FamilyID)
	require.NotNil(t, old.ReplacedBy)
	assert.Equal(t, nw.ID, *old.ReplacedBy)
}

func TestRefresh_ReuseRevokesFamily(t *testing.T) {
	r := newRefreshEnv(t)
	ctx := context.Background()
	p1 := r.login(t)
	p2, err := r.svc.Refresh(ctx, p1.RefreshToken)
	require.NoError(t, err)

	_, err = r.svc.Refresh(ctx, p1.RefreshToken)
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
	assert.True(t, *r.committed, "family revocation must commit although Refresh errors")
	nw, _ := r.tokens.byHash(p2.RefreshToken)
	assert.NotNil(t, nw.RevokedAt)

	_, err = r.svc.Refresh(ctx, p2.RefreshToken)
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
}

func TestRefresh_Expired(t *testing.T) {
	r := newRefreshEnv(t)
	p1 := r.login(t)
	*r.clock = now.Add(7 * 24 * time.Hour)
	_, err := r.svc.Refresh(context.Background(), p1.RefreshToken)
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
	tok, _ := r.tokens.byHash(p1.RefreshToken)
	assert.Nil(t, tok.RevokedAt, "expiry must not revoke the family")
}

func TestRefresh_RevokedAndExpiredIsReuse(t *testing.T) {
	r := newRefreshEnv(t)
	ctx := context.Background()
	p1 := r.login(t)
	p2, err := r.svc.Refresh(ctx, p1.RefreshToken)
	require.NoError(t, err)

	*r.clock = now.Add(7*24*time.Hour + time.Minute) // past p1's ExpiresAt
	_, err = r.svc.Refresh(ctx, p1.RefreshToken)
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
	assert.True(t, *r.committed, "replay of a revoked token must revoke the family and commit even when expired")
	nw, ok := r.tokens.byHash(p2.RefreshToken)
	require.True(t, ok)
	assert.NotNil(t, nw.RevokedAt)
}

func TestRefresh_Unknown(t *testing.T) {
	r := newRefreshEnv(t)
	_, err := r.svc.Refresh(context.Background(), "nope")
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
}

func TestRefresh_InactiveUser(t *testing.T) {
	r := newRefreshEnv(t)
	p1 := r.login(t)
	r.users.setStatus("a@x.com", domain.StatusLocked)
	_, err := r.svc.Refresh(context.Background(), p1.RefreshToken)
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
	tok, _ := r.tokens.byHash(p1.RefreshToken)
	assert.Nil(t, tok.RevokedAt, "inactive user must not revoke the family")
	assert.Len(t, r.tokens.all(), 1)
}

func TestLogout(t *testing.T) {
	r := newRefreshEnv(t)
	ctx := context.Background()
	p1 := r.login(t)
	require.NoError(t, r.svc.Logout(ctx, p1.RefreshToken))
	tok, _ := r.tokens.byHash(p1.RefreshToken)
	assert.NotNil(t, tok.RevokedAt)
	_, err := r.svc.Refresh(ctx, p1.RefreshToken)
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
	assert.NoError(t, r.svc.Logout(ctx, "nope"))
	assert.NoError(t, r.svc.Logout(ctx, p1.RefreshToken))
}

func TestGetMe(t *testing.T) {
	r := newRefreshEnv(t)
	u := r.users.byEmail["a@x.com"]
	got, err := r.svc.GetMe(context.Background(), u.ID)
	require.NoError(t, err)
	assert.Equal(t, u.ID, got.ID)
}

func TestGetMe_NotFound(t *testing.T) {
	r := newRefreshEnv(t)
	_, err := r.svc.GetMe(context.Background(), uuid.New())
	assert.ErrorIs(t, err, apperr.ErrNotFound)
}
