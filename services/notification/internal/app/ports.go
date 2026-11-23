// Package app holds the notification use cases and the ports they depend on.
package app

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/services/notification/internal/domain"
)

// TxManager runs fn in a transaction; ctx passed to fn carries the tx.
type TxManager interface {
	WithTx(ctx context.Context, fn func(ctx context.Context) error) error
}

// Idempotency records processed event IDs inside the tx carried by ctx; it
// reports false when eventID was already processed.
type Idempotency interface {
	Once(ctx context.Context, eventID string) (bool, error)
}

// Cursor is a keyset pagination position (created_at DESC, id DESC).
type Cursor struct {
	At time.Time `json:"p"`
	ID uuid.UUID `json:"id"`
}

// UserRepo is the replica of accounts.
type UserRepo interface {
	Upsert(ctx context.Context, u domain.User) error
	// Get returns domain.ErrUserNotFound.
	Get(ctx context.Context, id uuid.UUID) (domain.User, error)
}

// JobRepo is the replica of jobs.
type JobRepo interface {
	// Upsert stores j unless a row with the same or a newer version exists;
	// applied reports whether it was written.
	Upsert(ctx context.Context, j domain.Job) (applied bool, err error)
	// Get returns domain.ErrJobNotFound.
	Get(ctx context.Context, id uuid.UUID) (domain.Job, error)
}

// NotificationRepo persists notifications.
type NotificationRepo interface {
	Create(ctx context.Context, n domain.Notification) error
	// List orders by created_at DESC, id DESC.
	List(ctx context.Context, userID uuid.UUID, unreadOnly bool, after *Cursor, limit int) ([]domain.Notification, error)
	CountUnread(ctx context.Context, userID uuid.UUID) (int, error)
	// MarkRead sets read_at once (an earlier read_at is kept) and returns the
	// row; another user's or a missing id is domain.ErrNotificationNotFound.
	MarkRead(ctx context.Context, id, userID uuid.UUID, at time.Time) (domain.Notification, error)
}

// EmailRepo is the email outbox.
type EmailRepo interface {
	Enqueue(ctx context.Context, e domain.Email) error
	// ClaimDue leases up to limit PENDING emails due at now: their
	// next_attempt_at moves to now+lease so no other sender takes them.
	ClaimDue(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]domain.Email, error)
	MarkSent(ctx context.Context, id uuid.UUID, at time.Time) error
	MarkRetry(ctx context.Context, id uuid.UUID, attempts int, next time.Time, lastErr string) error
	MarkFailed(ctx context.Context, id uuid.UUID, attempts int, lastErr string) error
}

// Mailer sends one email.
type Mailer interface {
	Send(ctx context.Context, to, subject, html, text string) error
}

// Pusher delivers a notification to the user's open WebSocket connections
// (best effort, never blocks).
type Pusher interface {
	Push(userID uuid.UUID, n domain.Notification)
}

// ErrPoisonEvent marks a consumed event that can never be processed.
var ErrPoisonEvent = errors.New("poison event")
