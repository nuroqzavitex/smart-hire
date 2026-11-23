package app

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
)

// Saga defaults (spec §6.4, ADR 0011).
const (
	defaultSagaDeadline        = 10 * time.Minute
	defaultSagaLease           = 30 * time.Second
	defaultSagaBatch           = 10
	defaultScoreNotFoundWindow = 2 * time.Minute
)

// Deps are the collaborators of Service.
type Deps struct {
	Applications ApplicationRepo
	Sagas        SagaRepo
	Outbox       OutboxWriter
	Idem         Idempotency
	ClosedJobs   ClosedJobs
	Tx           TxManager
	Jobs         Jobs
	CVs          CVs
	Quota        Quota
	Scorer       Scorer
	// Purger pseudonymises a deleted user's applications (ADR 0014).
	Purger UserPurger
	Now    func() time.Time
	NewID  func() uuid.UUID
	Logger *slog.Logger
	// Wake nudges the saga worker after a saga became due; nil is a no-op.
	Wake func()

	SagaDeadline        time.Duration
	SagaLease           time.Duration
	SagaBatch           int
	ScoreNotFoundWindow time.Duration
}

// Service implements the application use cases and the submit saga.
type Service struct{ Deps }

// NewService wires a Service, defaulting Now, NewID, Logger, Wake and the
// saga settings.
func NewService(d Deps) *Service {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.NewID == nil {
		d.NewID = uuid.New
	}
	if d.Logger == nil {
		d.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if d.Wake == nil {
		d.Wake = func() {}
	}
	if d.SagaDeadline == 0 {
		d.SagaDeadline = defaultSagaDeadline
	}
	if d.SagaLease == 0 {
		d.SagaLease = defaultSagaLease
	}
	if d.SagaBatch == 0 {
		d.SagaBatch = defaultSagaBatch
	}
	if d.ScoreNotFoundWindow == 0 {
		d.ScoreNotFoundWindow = defaultScoreNotFoundWindow
	}
	return &Service{Deps: d}
}

// caller returns the authenticated caller's id and role.
func caller(ctx context.Context) (uuid.UUID, authz.Role, error) {
	c, ok := authz.ClaimsFrom(ctx)
	if !ok {
		return uuid.Nil, "", apperr.New(apperr.ErrUnauthenticated, "authentication required")
	}
	id, err := uuid.Parse(c.Subject)
	if err != nil {
		return uuid.Nil, "", apperr.New(apperr.ErrUnauthenticated, "invalid subject")
	}
	return id, c.Role, nil
}
