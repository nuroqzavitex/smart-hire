package app_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/app"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

var now = time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)

type env struct {
	svc    *app.Service
	users  *fakeUsers
	tokens *fakeTokens
	outbox *fakeOutbox
	hasher *fakeHasher
}

func newEnv(t *testing.T) env {
	t.Helper()
	e := env{users: newFakeUsers(), tokens: &fakeTokens{}, outbox: &fakeOutbox{}, hasher: &fakeHasher{}}
	svc, err := app.NewService(app.Deps{
		Users: e.users, Tokens: e.tokens, Outbox: e.outbox, Tx: fakeTx{},
		Hasher: e.hasher, Issuer: fakeIssuer{},
		Now:        func() time.Time { return now },
		RefreshTTL: 7 * 24 * time.Hour,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	e.svc = svc
	return e
}

func TestRegister_WritesUserAndOutbox(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	id, err := e.svc.Register(ctx, "A@x.com", "password1", authz.RoleCandidate)
	require.NoError(t, err)
	u := e.users.byEmail["a@x.com"]
	assert.Equal(t, id, u.ID)
	assert.Equal(t, "h:password1", u.PasswordHash)
	assert.Equal(t, domain.StatusActive, u.Status)
	assert.Equal(t, now, u.CreatedAt)
	require.Len(t, e.outbox.events, 1)
	assert.Equal(t, "user.registered", e.outbox.events[0].EventType)
	assert.JSONEq(t, fmt.Sprintf(`{"user_id":%q,"email":"a@x.com","role":"candidate"}`, id), string(e.outbox.events[0].Payload))
}

func TestRegister_RejectsAdminRole(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	_, err := e.svc.Register(ctx, "root@x.com", "password1", authz.RoleAdmin)
	assert.ErrorIs(t, err, apperr.ErrValidation)
	assert.Empty(t, e.users.byEmail)
	assert.Empty(t, e.outbox.events)

	_, err = e.svc.CreateUser(ctx, "root@x.com", "password1", authz.RoleAdmin)
	require.NoError(t, err)
	assert.Equal(t, authz.RoleAdmin, e.users.byEmail["root@x.com"].Role)
}

func TestRegister_InvalidInput(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	_, err := e.svc.Register(ctx, "not-an-email", "password1", authz.RoleCandidate)
	assert.ErrorIs(t, err, apperr.ErrValidation)
	_, err = e.svc.Register(ctx, "a@x.com", "short", authz.RoleCandidate)
	assert.ErrorIs(t, err, apperr.ErrValidation)
	assert.Empty(t, e.outbox.events)
}

func TestRegister_Duplicate(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	_, err := e.svc.Register(ctx, "a@x.com", "password1", authz.RoleCandidate)
	require.NoError(t, err)
	_, err = e.svc.Register(ctx, "A@X.com", "password1", authz.RoleRecruiter)
	assert.ErrorIs(t, err, apperr.ErrConflict)
	assert.Len(t, e.outbox.events, 1)
}

func registered(t *testing.T) env {
	t.Helper()
	e := newEnv(t)
	_, err := e.svc.Register(context.Background(), "a@x.com", "password1", authz.RoleCandidate)
	require.NoError(t, err)
	return e
}

func TestLogin_OK(t *testing.T) {
	e := registered(t)
	p, err := e.svc.Login(context.Background(), "a@x.com", "password1")
	require.NoError(t, err)
	assert.Equal(t, 15*time.Minute, p.ExpiresIn)
	assert.NotEmpty(t, p.AccessToken)
	assert.Len(t, p.RefreshToken, 43) // 32 bytes, base64 RawURL
	require.Len(t, e.tokens.all(), 1)
	stored := e.tokens.all()[0]
	assert.Equal(t, sha256Sum(p.RefreshToken), stored.Hash)
	assert.Equal(t, now.Add(7*24*time.Hour), stored.ExpiresAt)
	assert.Equal(t, now, stored.CreatedAt)
	assert.Equal(t, e.users.byEmail["a@x.com"].ID, stored.UserID)
	assert.NotEqual(t, stored.ID, stored.FamilyID)
	assert.Nil(t, stored.RevokedAt)
}

func TestLogin_NormalizesEmail(t *testing.T) {
	e := registered(t)
	_, err := e.svc.Login(context.Background(), "  A@X.com ", "password1")
	assert.NoError(t, err)
}

func TestLogin_InactiveAccount(t *testing.T) {
	e := registered(t)
	ctx := context.Background()
	e.users.setStatus("a@x.com", domain.StatusLocked)
	_, err := e.svc.Login(ctx, "a@x.com", "wrong-pass")
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated) // wrong password must not reveal status
	_, err = e.svc.Login(ctx, "a@x.com", "password1")
	assert.ErrorIs(t, err, domain.ErrAccountLocked)
	e.users.setStatus("a@x.com", domain.StatusDeletionPending)
	_, err = e.svc.Login(ctx, "a@x.com", "password1")
	assert.ErrorIs(t, err, domain.ErrAccountPendingDeletion)
	assert.Empty(t, e.tokens.all())
}

func TestLogin_UnknownEmailAndWrongPasswordSameError(t *testing.T) {
	e := registered(t)
	ctx := context.Background()
	_, e1 := e.svc.Login(ctx, "nobody@x.com", "password1")
	_, e2 := e.svc.Login(ctx, "a@x.com", "wrong-pass")
	require.Error(t, e1)
	require.Error(t, e2)
	assert.Equal(t, e1.Error(), e2.Error())
	assert.ErrorIs(t, e1, apperr.ErrUnauthenticated)
	assert.ErrorIs(t, e2, apperr.ErrUnauthenticated)
	// Unknown email still burns a Verify against the precomputed dummy hash.
	assert.Equal(t, 1, e.hasher.verifyCallsAgainst("h:dummy-password-for-timing"))
}

func TestLogin_InvalidEmailBurnsDummyVerify(t *testing.T) {
	e := registered(t)
	_, err := e.svc.Login(context.Background(), "not-an-email", "password1")
	assert.ErrorIs(t, err, domain.ErrInvalidCredentials)
	assert.Equal(t, 1, e.hasher.verifyCallsAgainst("h:dummy-password-for-timing"))
}
