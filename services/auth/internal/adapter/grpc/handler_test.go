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

func (u *users) SetStatus(_ context.Context, id uuid.UUID, st domain.UserStatus, _ time.Time) error {
	x := u.byID[id]
	x.Status = st
	u.byID[id] = x
	return nil
}

func (u *users) HardDelete(_ context.Context, id uuid.UUID) error {
	delete(u.byID, id)
	return nil
}

type deletions struct {
	byID map[uuid.UUID]domain.DeletionRequest
}

func (d *deletions) Create(_ context.Context, r domain.DeletionRequest) error {
	d.byID[r.ID] = r
	return nil
}

func (d *deletions) GetActiveByUser(_ context.Context, user uuid.UUID) (domain.DeletionRequest, error) {
	for _, r := range d.byID {
		if r.UserID == user && r.Status != domain.DeletionCompleted {
			return r, nil
		}
	}
	return domain.DeletionRequest{}, domain.ErrDeletionNotFound
}

func (d *deletions) Get(_ context.Context, id uuid.UUID) (domain.DeletionRequest, error) {
	if r, ok := d.byID[id]; ok {
		return r, nil
	}
	return domain.DeletionRequest{}, domain.ErrDeletionNotFound
}

func (d *deletions) GetForUpdate(ctx context.Context, id uuid.UUID) (domain.DeletionRequest, error) {
	return d.Get(ctx, id)
}

func (d *deletions) Update(_ context.Context, r domain.DeletionRequest) error {
	d.byID[r.ID] = r
	return nil
}

func (d *deletions) ClaimSweep(context.Context, time.Time, time.Time, int) ([]domain.DeletionRequest, error) {
	return nil, nil
}

func (d *deletions) CountByStatus(context.Context) (map[domain.DeletionStatus]int64, error) {
	return nil, nil
}

// tokens stores only Create; other methods report "not found".
type tokens struct{}

func (tokens) Create(context.Context, domain.RefreshToken) error { return nil }
func (tokens) GetByHashForUpdate(context.Context, []byte) (domain.RefreshToken, error) {
	return domain.RefreshToken{}, apperr.New(apperr.ErrNotFound, "not found")
}
func (tokens) Revoke(context.Context, uuid.UUID, time.Time, *uuid.UUID) error { return nil }
func (tokens) RevokeFamily(context.Context, uuid.UUID, time.Time) error       { return nil }
func (tokens) RevokeAllForUser(context.Context, uuid.UUID, time.Time) error   { return nil }

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
		Deletions: &deletions{byID: map[uuid.UUID]domain.DeletionRequest{}},
		Now:       time.Now, RefreshTTL: time.Hour, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
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

func TestGRPCHandler_DeleteMeAndAdminView(t *testing.T) {
	h, us := newHandler(t)
	uid := uuid.New()
	us.byID[uid] = domain.User{ID: uid, Email: "a@b.co", PasswordHash: "h:pw", Role: authz.RoleCandidate, Status: domain.StatusActive}

	_, err := h.DeleteMe(context.Background(), &authv1.DeleteMeRequest{Password: "pw"})
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated, "needs claims")

	ctx := authz.WithClaims(context.Background(), authz.Claims{Subject: uid.String(), Role: authz.RoleCandidate})
	_, err = h.DeleteMe(ctx, &authv1.DeleteMeRequest{Password: "bad"})
	assert.ErrorIs(t, err, domain.ErrInvalidPassword)

	resp, err := h.DeleteMe(ctx, &authv1.DeleteMeRequest{Password: "pw"})
	require.NoError(t, err)
	assert.Equal(t, "PENDING", resp.GetStatus())
	assert.Equal(t, domain.StatusDeletionPending, us.byID[uid].Status)

	admin := authz.WithClaims(context.Background(), authz.Claims{Subject: uuid.NewString(), Role: authz.RoleAdmin})
	got, err := h.GetDeletionRequest(admin, &authv1.GetDeletionRequestRequest{Id: resp.GetDeletionRequestId()})
	require.NoError(t, err)
	assert.Equal(t, uid.String(), got.GetUserId())
	assert.Equal(t, "PENDING", got.GetStatus())
	assert.Len(t, got.GetExpectedServices(), 5)
	assert.Empty(t, got.GetPurgedServices())
	assert.NotNil(t, got.GetRequestedAt())
	assert.Nil(t, got.GetCompletedAt())

	_, err = h.GetDeletionRequest(admin, &authv1.GetDeletionRequestRequest{Id: "nope"})
	assert.ErrorIs(t, err, apperr.ErrValidation)
}
