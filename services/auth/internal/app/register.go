package app

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

const (
	eventSource         = "auth-service"
	eventUserRegistered = "user.registered"
)

type userRegisteredPayload struct {
	UserID string     `json:"user_id"`
	Email  string     `json:"email"`
	Role   authz.Role `json:"role"`
}

// Register creates a self-service account. Only candidate and recruiter roles
// are allowed; privileged accounts go through CreateUser.
func (s *Service) Register(ctx context.Context, email, password string, role authz.Role) (uuid.UUID, error) {
	if role != authz.RoleCandidate && role != authz.RoleRecruiter {
		return uuid.Nil, apperr.New(apperr.ErrValidation, "role must be candidate or recruiter")
	}
	return s.CreateUser(ctx, email, password, role)
}

// CreateUser creates an account with any valid role (used by seeding) and
// writes a user.registered outbox event in the same transaction.
func (s *Service) CreateUser(ctx context.Context, email, password string, role authz.Role) (uuid.UUID, error) {
	switch role {
	case authz.RoleCandidate, authz.RoleRecruiter, authz.RoleAdmin:
	default:
		return uuid.Nil, apperr.New(apperr.ErrValidation, "unknown role")
	}
	email, err := domain.NewEmail(email)
	if err != nil {
		return uuid.Nil, err
	}
	if err := domain.ValidatePassword(password); err != nil {
		return uuid.Nil, err
	}
	hash, err := s.Hasher.Hash(password)
	if err != nil {
		return uuid.Nil, fmt.Errorf("hash password: %w", err)
	}
	now := s.Now()
	u := domain.User{
		ID: uuid.New(), Email: email, PasswordHash: hash, Role: role,
		Status: domain.StatusActive, CreatedAt: now, UpdatedAt: now,
	}
	err = s.Tx.WithTx(ctx, func(ctx context.Context) error {
		if err := s.Users.Create(ctx, u); err != nil {
			return err
		}
		env, err := event.New(ctx, eventSource, eventUserRegistered, 1,
			userRegisteredPayload{UserID: u.ID.String(), Email: u.Email, Role: u.Role}, now)
		if err != nil {
			return fmt.Errorf("build event: %w", err)
		}
		return s.Outbox.Write(ctx, env, u.ID.String())
	})
	if err != nil {
		return uuid.Nil, err
	}
	return u.ID, nil
}
