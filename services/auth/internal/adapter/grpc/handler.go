// Package grpc adapts app.Service to the generated AuthService gRPC API.
package grpc

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	authv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/auth/v1"
	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/app"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

// PublicMethods are the RPCs callable without an access token.
var PublicMethods = []string{
	authv1.AuthService_Register_FullMethodName,
	authv1.AuthService_Login_FullMethodName,
	authv1.AuthService_Refresh_FullMethodName,
	authv1.AuthService_Logout_FullMethodName,
}

type handler struct {
	authv1.UnimplementedAuthServiceServer
	svc *app.Service
}

// NewHandler returns the AuthService implementation backed by svc.
func NewHandler(svc *app.Service) authv1.AuthServiceServer { return &handler{svc: svc} }

func (h *handler) Register(ctx context.Context, req *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
	id, err := h.svc.Register(ctx, req.GetEmail(), req.GetPassword(), roleFromProto(req.GetRole()))
	if err != nil {
		return nil, err
	}
	return &authv1.RegisterResponse{UserId: id.String()}, nil
}

func (h *handler) Login(ctx context.Context, req *authv1.LoginRequest) (*authv1.TokenPair, error) {
	pair, err := h.svc.Login(ctx, req.GetEmail(), req.GetPassword())
	if err != nil {
		return nil, err
	}
	return pairToProto(pair), nil
}

func (h *handler) Refresh(ctx context.Context, req *authv1.RefreshRequest) (*authv1.TokenPair, error) {
	pair, err := h.svc.Refresh(ctx, req.GetRefreshToken())
	if err != nil {
		return nil, err
	}
	return pairToProto(pair), nil
}

func (h *handler) Logout(ctx context.Context, req *authv1.LogoutRequest) (*emptypb.Empty, error) {
	if err := h.svc.Logout(ctx, req.GetRefreshToken()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// callerID returns the verified subject of the access token.
func callerID(ctx context.Context) (uuid.UUID, error) {
	claims, ok := authz.ClaimsFrom(ctx)
	if !ok {
		return uuid.Nil, apperr.New(apperr.ErrUnauthenticated, "authentication required")
	}
	id, err := uuid.Parse(claims.Subject)
	if err != nil {
		return uuid.Nil, apperr.New(apperr.ErrUnauthenticated, "invalid token")
	}
	return id, nil
}

func (h *handler) GetMe(ctx context.Context, _ *authv1.GetMeRequest) (*authv1.User, error) {
	id, err := callerID(ctx)
	if err != nil {
		return nil, err
	}
	u, err := h.svc.GetMe(ctx, id)
	if err != nil {
		return nil, err
	}
	return userToProto(u), nil
}

func (h *handler) DeleteMe(ctx context.Context, req *authv1.DeleteMeRequest) (*authv1.DeleteMeResponse, error) {
	id, err := callerID(ctx)
	if err != nil {
		return nil, err
	}
	r, err := h.svc.DeleteMe(ctx, id, req.GetPassword())
	if err != nil {
		return nil, err
	}
	return &authv1.DeleteMeResponse{DeletionRequestId: r.ID.String(), Status: string(r.Status)}, nil
}

func (h *handler) GetDeletionRequest(ctx context.Context, req *authv1.GetDeletionRequestRequest) (*authv1.DeletionRequest, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, apperr.New(apperr.ErrValidation, "invalid deletion request id")
	}
	r, err := h.svc.GetDeletionRequest(ctx, id)
	if err != nil {
		return nil, err
	}
	out := &authv1.DeletionRequest{
		Id: r.ID.String(), UserId: r.UserID.String(), Status: string(r.Status),
		ExpectedServices: r.Expected, PurgedServices: r.Purged,
		RequestedAt: timestamppb.New(r.RequestedAt),
	}
	if r.CompletedAt != nil {
		out.CompletedAt = timestamppb.New(*r.CompletedAt)
	}
	return out, nil
}

func pairToProto(p app.TokenPair) *authv1.TokenPair {
	return &authv1.TokenPair{
		AccessToken:  p.AccessToken,
		ExpiresIn:    int64(p.ExpiresIn.Seconds()),
		RefreshToken: p.RefreshToken,
	}
}

func userToProto(u domain.User) *authv1.User {
	return &authv1.User{
		Id:        u.ID.String(),
		Email:     u.Email,
		Role:      roleToProto(u.Role),
		CreatedAt: timestamppb.New(u.CreatedAt),
	}
}

// roleFromProto maps unknown/unspecified to "", which the service rejects.
func roleFromProto(r authv1.Role) authz.Role {
	switch r {
	case authv1.Role_ROLE_CANDIDATE:
		return authz.RoleCandidate
	case authv1.Role_ROLE_RECRUITER:
		return authz.RoleRecruiter
	case authv1.Role_ROLE_ADMIN:
		return authz.RoleAdmin
	default:
		return ""
	}
}

func roleToProto(r authz.Role) authv1.Role {
	switch r {
	case authz.RoleCandidate:
		return authv1.Role_ROLE_CANDIDATE
	case authz.RoleRecruiter:
		return authv1.Role_ROLE_RECRUITER
	case authz.RoleAdmin:
		return authv1.Role_ROLE_ADMIN
	default:
		return authv1.Role_ROLE_UNSPECIFIED
	}
}
