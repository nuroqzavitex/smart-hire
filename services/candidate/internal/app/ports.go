// Package app holds the candidate service's use cases and the ports they
// depend on.
package app

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
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

// Cursor is a keyset pagination position.
type Cursor struct {
	At time.Time `json:"p"`
	ID uuid.UUID `json:"id"`
}

// CandidateRepo persists candidate profiles.
type CandidateRepo interface {
	// Get returns domain.ErrProfileNotFound when the profile does not exist.
	Get(ctx context.Context, userID uuid.UUID) (domain.Candidate, error)
	// Upsert keeps created_at of an existing row and returns the stored row.
	Upsert(ctx context.Context, c domain.Candidate) (domain.Candidate, error)
}

// CVRepo persists CVs.
type CVRepo interface {
	Create(ctx context.Context, cv domain.CV) error
	// Get returns domain.ErrCVNotFound when the CV does not exist.
	Get(ctx context.Context, id uuid.UUID) (domain.CV, error)
	// GetForUpdate is Get with a row lock.
	GetForUpdate(ctx context.Context, id uuid.UUID) (domain.CV, error)
	// Update writes every mutable field.
	Update(ctx context.Context, cv domain.CV) error
	// ListByCandidate orders by created_at DESC, id DESC.
	ListByCandidate(ctx context.Context, candidateID uuid.UUID, after *Cursor, limit int) ([]domain.CV, error)
}

// ObjectInfo describes a stored object.
type ObjectInfo struct {
	Size        int64
	ContentType string
}

// ObjectStorage is the CV blob store.
type ObjectStorage interface {
	PresignPut(ctx context.Context, key, contentType string, ttl time.Duration) (string, error)
	// Stat returns ErrObjectNotFound when the object does not exist.
	Stat(ctx context.Context, key string) (ObjectInfo, error)
	// ReadHead returns at most the first n bytes of the object.
	ReadHead(ctx context.Context, key string, n int64) ([]byte, error)
	Delete(ctx context.Context, key string) error
}

// ErrObjectNotFound reports a missing object in ObjectStorage.
var ErrObjectNotFound = errors.New("object not found")

// SchemaValidator validates documents against the contracts/events schemas.
// Its errors may echo document values, so callers must not log them or put
// them in consumer errors.
type SchemaValidator interface {
	ValidateEvent(eventType string, doc []byte) error
	ValidateParsedCV(doc []byte) error
}

// ErrPoisonEvent marks a consumed event that can never be processed; wrap it
// as fmt.Errorf("%w: ...", ErrPoisonEvent).
var ErrPoisonEvent = errors.New("poison event")
