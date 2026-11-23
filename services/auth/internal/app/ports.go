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
	// SetStatus fails with apperr.ErrNotFound when absent.
	SetStatus(ctx context.Context, id uuid.UUID, status domain.UserStatus, at time.Time) error
	// HardDelete removes the user, its refresh tokens and its user.registered
	// outbox rows (they hold the email). Deleting a missing user succeeds.
	HardDelete(ctx context.Context, id uuid.UUID) error
}

// RefreshTokenRepo persists hashed refresh tokens.
type RefreshTokenRepo interface {
	Create(ctx context.Context, t domain.RefreshToken) error
	// GetByHashForUpdate fails with apperr.ErrNotFound when absent.
	GetByHashForUpdate(ctx context.Context, hash []byte) (domain.RefreshToken, error)
	Revoke(ctx context.Context, id uuid.UUID, at time.Time, replacedBy *uuid.UUID) error
	// RevokeFamily revokes only tokens of the family that are not yet revoked.
	RevokeFamily(ctx context.Context, familyID uuid.UUID, at time.Time) error
	// RevokeAllForUser revokes every live token of the user.
	RevokeAllForUser(ctx context.Context, userID uuid.UUID, at time.Time) error
}

// DeletionRepo persists account-deletion requests (ADR 0014).
type DeletionRepo interface {
	// Create fails with apperr.ErrConflict when the user already has a
	// request that is not COMPLETED.
	Create(ctx context.Context, r domain.DeletionRequest) error
	// GetActiveByUser, Get and GetForUpdate fail with
	// domain.ErrDeletionNotFound (apperr.ErrNotFound) when absent.
	GetActiveByUser(ctx context.Context, userID uuid.UUID) (domain.DeletionRequest, error)
	Get(ctx context.Context, id uuid.UUID) (domain.DeletionRequest, error)
	GetForUpdate(ctx context.Context, id uuid.UUID) (domain.DeletionRequest, error)
	// Update writes status, purged services, last_published_at and completed_at.
	Update(ctx context.Context, r domain.DeletionRequest) error
	// ClaimSweep locks (SKIP LOCKED) up to limit unfinished requests published
	// before republishBefore, or PENDING ones requested before stuckBefore.
	ClaimSweep(ctx context.Context, republishBefore, stuckBefore time.Time, limit int) ([]domain.DeletionRequest, error)
	CountByStatus(ctx context.Context) (map[domain.DeletionStatus]int64, error)
}

// Idempotency records processed event IDs inside the tx carried by ctx; it
// reports false when eventID was already processed.
type Idempotency interface {
	Once(ctx context.Context, eventID string) (bool, error)
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
