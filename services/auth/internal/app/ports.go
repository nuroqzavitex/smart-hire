// Package app implements the auth use cases against ports defined here.
package app

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

// TxManager runs fn inside a transaction; repos read the tx from ctx.
type TxManager interface {
	WithTx(ctx context.Context, fn func(ctx context.Context) error) error
}

// UserRepo persists users.
type UserRepo interface {
	// Create fails with apperr.ErrConflict on a duplicate email.
	Create(ctx context.Context, u domain.User) error
	// GetByEmail fails with apperr.ErrNotFound when absent.
	GetByEmail(ctx context.Context, email string) (domain.User, error)
	GetByID(ctx context.Context, id uuid.UUID) (domain.User, error)
}

// RefreshTokenRepo persists hashed refresh tokens.
type RefreshTokenRepo interface {
	Create(ctx context.Context, t domain.RefreshToken) error
	// GetByHashForUpdate fails with apperr.ErrNotFound when absent.
	GetByHashForUpdate(ctx context.Context, hash []byte) (domain.RefreshToken, error)
	Revoke(ctx context.Context, id uuid.UUID, at time.Time, replacedBy *uuid.UUID) error
	// RevokeFamily revokes only tokens of the family that are not yet revoked.
	RevokeFamily(ctx context.Context, familyID uuid.UUID, at time.Time) error
}

// OutboxWriter records an event in the same transaction as the state change.
type OutboxWriter interface {
	Write(ctx context.Context, e event.Envelope, aggregateID string) error
}

// PasswordHasher hashes and verifies passwords.
type PasswordHasher interface {
	Hash(pw string) (string, error)
	Verify(pw, hash string) (bool, error)
}

// TokenIssuer mints signed access tokens.
type TokenIssuer interface {
	IssueAccess(u domain.User, now time.Time) (token string, ttl time.Duration, err error)
}
