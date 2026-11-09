package grpc_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/auth/v1"
	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/event"
	authgrpc "github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/grpc"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/app"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

type tx struct{}

func (tx) WithTx(ctx context.Context, fn func(context.Context) error) error { return fn(ctx) }

type users struct{ byID map[uuid.UUID]domain.User }

func (u *users) Create(_ context.Context, usr domain.User) error {
	u.byID[usr.ID] = usr
	return nil
}

func (u *users) GetByEmail(_ context.Context, email string) (domain.User, error) {
	for _, x := range u.byID {
		if x.Email == email {
			return x, nil
		}
	}
	return domain.User{}, apperr.New(apperr.ErrNotFound, "user not found")
}

func (u *users) GetByID(_ context.Context, id uuid.UUID) (domain.User, error) {
	if x, ok := u.byID[id]; ok {
		return x, nil
	}
	return domain.User{}, apperr.New(apperr.ErrNotFound, "user not found")
}

// tokens stores only Create; other methods report "not found".
type tokens struct{}

func (tokens) Create(context.Context, domain.RefreshToken) error { return nil }
func (tokens) GetByHashForUpdate(context.Context, []byte) (domain.RefreshToken, error) {
	return domain.RefreshToken{}, apperr.New(apperr.ErrNotFound, "not found")
}
func (tokens) Revoke(context.Context, uuid.UUID, time.Time, *uuid.UUID) error { return nil }
func (tokens) RevokeFamily(context.Context, uuid.UUID, time.Time) error       { return nil }

type outbox struct{}

func (outbox) Write(context.Context, event.Envelope, string) error { return nil }

type hasher struct{}

func (hasher) Hash(pw string) (string, error)       { return "h:" + pw, nil }
func (hasher) Verify(pw, hash string) (bool, error) { return hash == "h:"+pw, nil }

type issuer struct{}

func (issuer) IssueAccess(domain.User, time.Time) (string, time.Duration, error) {
	return "access-token", 15 * time.Minute, nil
}

func newHandler(t *testing.T) (authv1.AuthServiceServer, *users) {
	t.Helper()
	us := &users{byID: map[uuid.UUID]domain.User{}}
	svc, err := app.NewService(app.Deps{
		Users: us, Tokens: tokens{}, Outbox: outbox{}, Tx: tx{}, Hasher: hasher{}, Issuer: issuer{},
		Now: time.Now, RefreshTTL: time.Hour, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	return authgrpc.NewHandler(svc), us
}

func TestGRPCHandler_GetMe_UsesClaims(t *testing.T) {
	h, us := newHandler(t)
	uid := uuid.New()
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	us.byID[uid] = domain.User{ID: uid, Email: "a@b.co", Role: authz.RoleRecruiter, CreatedAt: created}

	ctx := authz.WithClaims(context.Background(), authz.Claims{Subject: uid.String(), Role: authz.RoleRecruiter})
	got, err := h.GetMe(ctx, &authv1.GetMeRequest{})
	require.NoError(t, err)
	assert.Equal(t, uid.String(), got.GetId())
	assert.Equal(t, "a@b.co", got.GetEmail())
	assert.Equal(t, authv1.Role_ROLE_RECRUITER, got.GetRole())
	assert.True(t, created.Equal(got.GetCreatedAt().AsTime()))
}

func TestGRPCHandler_GetMe_Unauthenticated(t *testing.T) {
	h, _ := newHandler(t)
	_, err := h.GetMe(context.Background(), &authv1.GetMeRequest{})
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)

	ctx := authz.WithClaims(context.Background(), authz.Claims{Subject: "not-a-uuid"})
	_, err = h.GetMe(ctx, &authv1.GetMeRequest{})
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
}

func TestGRPCHandler_Login_MapsTokenPair(t *testing.T) {
	h, _ := newHandler(t)
	reg, err := h.Register(context.Background(), &authv1.RegisterRequest{
		Email: "a@b.co", Password: "Password123!", Role: authv1.Role_ROLE_CANDIDATE,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, reg.GetUserId())

	tp, err := h.Login(context.Background(), &authv1.LoginRequest{Email: "a@b.co", Password: "Password123!"})
	require.NoError(t, err)
	assert.Equal(t, int64(900), tp.GetExpiresIn())
	assert.Equal(t, "access-token", tp.GetAccessToken())
	assert.NotEmpty(t, tp.GetRefreshToken())
}

func TestGRPCHandler_Register_RejectsAdminAndUnspecifiedRole(t *testing.T) {
	h, _ := newHandler(t)
	for _, r := range []authv1.Role{authv1.Role_ROLE_ADMIN, authv1.Role_ROLE_UNSPECIFIED} {
		_, err := h.Register(context.Background(), &authv1.RegisterRequest{
			Email: "a@b.co", Password: "Password123!", Role: r,
		})
		assert.ErrorIs(t, err, apperr.ErrValidation, r.String())
	}
}

func TestGRPCHandler_RefreshAndLogout(t *testing.T) {
	h, _ := newHandler(t)
	_, err := h.Refresh(context.Background(), &authv1.RefreshRequest{RefreshToken: "unknown"})
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
	_, err = h.Logout(context.Background(), &authv1.LogoutRequest{RefreshToken: "unknown"})
	assert.NoError(t, err)
}

func TestPublicMethods(t *testing.T) {
	assert.ElementsMatch(t, []string{
		"/smarthire.auth.v1.AuthService/Register",
		"/smarthire.auth.v1.AuthService/Login",
		"/smarthire.auth.v1.AuthService/Refresh",
		"/smarthire.auth.v1.AuthService/Logout",
	}, authgrpc.PublicMethods)
}
