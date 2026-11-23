// Package app holds the application service's use cases, the submit saga
// orchestrator and the ports they depend on.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

// TxManager runs fn in a transaction; ctx passed to fn carries the tx.
type TxManager interface {
	WithTx(ctx context.Context, fn func(ctx context.Context) error) error
}

// OutboxWriter stores an event in the transactional outbox.
type OutboxWriter interface {
	Write(ctx context.Context, e event.Envelope, aggregateID string) error
}

// Idempotency records processed event IDs. Once must run inside the tx
// carried by ctx; it reports false when eventID was already processed.
type Idempotency interface {
	Once(ctx context.Context, eventID string) (bool, error)
}

// Cursor is a keyset pagination position (created_at DESC, id DESC).
type Cursor struct {
	At time.Time `json:"p"`
	ID uuid.UUID `json:"id"`
}

// ListFilter narrows ListApplications; nil fields do not filter.
type ListFilter struct {
	CandidateID *uuid.UUID
	RecruiterID *uuid.UUID
	JobID       *uuid.UUID
	Status      *domain.Status
}

// ApplicationRepo persists applications and their history.
type ApplicationRepo interface {
	// Create returns domain.ErrIdempotencyKeyConflict or
	// domain.ErrAlreadyApplied on the matching unique violation.
	Create(ctx context.Context, a domain.Application) error
	// Get, GetForUpdate and GetByKey return domain.ErrApplicationNotFound.
	Get(ctx context.Context, id uuid.UUID) (domain.Application, error)
	GetForUpdate(ctx context.Context, id uuid.UUID) (domain.Application, error)
	GetByKey(ctx context.Context, candidateID uuid.UUID, key string) (domain.Application, error)
	// Update writes status, reason, score, breakdown, model version,
	// version and updated_at.
	Update(ctx context.Context, a domain.Application) error
	List(ctx context.Context, f ListFilter, after *Cursor, limit int) ([]domain.Application, error)
	AddHistory(ctx context.Context, appID uuid.UUID, h domain.HistoryEntry) error
	History(ctx context.Context, appID uuid.UUID) ([]domain.HistoryEntry, error)
}

// SagaRepo persists submit sagas.
type SagaRepo interface {
	Create(ctx context.Context, s domain.Saga) error
	// Claim leases up to limit due RUNNING/COMPENSATING sagas: their
	// next_run_at becomes now+lease, attempts and version are incremented,
	// and the returned sagas carry the new version.
	Claim(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]domain.Saga, error)
	// Save writes s only if the stored version still equals s.Version (and
	// then increments it); false means another writer won.
	Save(ctx context.Context, s domain.Saga) (bool, error)
	// GetByApplication returns domain.ErrApplicationNotFound when absent.
	GetByApplication(ctx context.Context, appID uuid.UUID) (domain.Saga, error)
	// CancelRunningForJob moves every RUNNING saga of the job to
	// COMPENSATING / RELEASE_QUOTA with the reason, due now.
	CancelRunningForJob(ctx context.Context, jobID uuid.UUID, reason string, now time.Time) (int, error)
}

// ClosedJobs remembers jobs that were closed or deleted (terminal states),
// so a submit racing a job.closed event is still caught.
type ClosedJobs interface {
	// MarkClosed is idempotent; it must run inside the consumer's tx.
	MarkClosed(ctx context.Context, jobID uuid.UUID, at time.Time) error
	IsClosed(ctx context.Context, jobID uuid.UUID) (bool, error)
}

// JobInfo is what the submit check needs from the job service.
type JobInfo struct {
	ID          uuid.UUID
	RecruiterID uuid.UUID
	Open        bool
}

// Jobs reads a job as the signed-in caller (the caller's JWT is forwarded).
// It returns apperr NotFound or Unavailable errors.
type Jobs interface {
	GetJob(ctx context.Context, id uuid.UUID) (JobInfo, error)
}

// CVInfo is what the submit check needs from the candidate service.
type CVInfo struct {
	ID     uuid.UUID
	Parsed bool
}

// CVs reads one of the caller's CVs (owner-only, the caller's JWT is
// forwarded). It returns apperr NotFound or Unavailable errors.
type CVs interface {
	GetCV(ctx context.Context, id uuid.UUID) (CVInfo, error)
}

// Quota is the candidate service's internal quota API used by the saga.
type Quota interface {
	// Reserve returns ErrQuotaExhausted, ErrReservationReleased, or another
	// (transient) error.
	Reserve(ctx context.Context, appID, candidateID uuid.UUID) error
	Release(ctx context.Context, appID uuid.UUID) error
}

// Score is the AI service's score for one CV against one job.
type Score struct {
	Value        float64
	Breakdown    json.RawMessage
	ModelVersion string
}

// Scorer is the AI service's ScoreMatch. It returns ErrNotMatched when the
// AI has no replica of the CV or job yet and ErrScorerUnavailable for every
// other failure (timeout, AI down, breaker open).
type Scorer interface {
	Score(ctx context.Context, cvID, jobID uuid.UUID) (Score, error)
}

// Errors of the saga's outbound ports.
var (
	ErrQuotaExhausted      = errors.New("quota exhausted")
	ErrReservationReleased = errors.New("quota reservation already released")
	ErrNotMatched          = errors.New("cv or job not matched yet")
	ErrScorerUnavailable   = errors.New("scorer unavailable")
)

// ErrPoisonEvent marks a consumed event that can never be processed.
var ErrPoisonEvent = errors.New("poison event")
