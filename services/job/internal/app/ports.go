// Package app holds the job service's use cases and the ports they depend on.
package app

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

// TxManager runs fn in a transaction; ctx passed to fn carries the tx.
type TxManager interface {
	WithTx(ctx context.Context, fn func(ctx context.Context) error) error
}

// OutboxWriter stores an event in the transactional outbox.
type OutboxWriter interface {
	Write(ctx context.Context, e event.Envelope, aggregateID string) error
}

// Cursor is a keyset pagination position.
type Cursor struct {
	Rank *float32  `json:"r,omitempty"`
	At   time.Time `json:"p"`
	ID   uuid.UUID `json:"id"`
}

// SearchFilter narrows open-job listings.
type SearchFilter struct {
	Query     string
	Skills    []string
	Location  string
	Level     *domain.Level
	SalaryMin *int
}

// SearchHit is a job with its text-search rank.
type SearchHit struct {
	Job  domain.Job
	Rank float32
}

// JobRepo persists jobs.
type JobRepo interface {
	Create(ctx context.Context, j domain.Job) error
	// Get ignores deleted jobs; a missing job yields domain.ErrJobNotFound.
	Get(ctx context.Context, id uuid.UUID) (domain.Job, error)
	// GetForUpdate is Get with a row lock.
	GetForUpdate(ctx context.Context, id uuid.UUID) (domain.Job, error)
	// Update writes every mutable field.
	Update(ctx context.Context, j domain.Job) error
	// Search returns open, non-deleted jobs; Rank is 0 when Query is empty.
	Search(ctx context.Context, f SearchFilter, after *Cursor, limit int) ([]SearchHit, error)
	ListByRecruiter(ctx context.Context, recruiterID uuid.UUID, status *domain.Status, after *Cursor, limit int) ([]domain.Job, error)
}
