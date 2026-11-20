package app

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
)

const defaultUploadTTL = 10 * time.Minute

// Deps are the collaborators of Service.
type Deps struct {
	Candidates   CandidateRepo
	CVs          CVRepo
	Storage      ObjectStorage
	Outbox       OutboxWriter
	Idem         Idempotency
	Tx           TxManager
	Schema       SchemaValidator
	Now          func() time.Time
	NewID        func() uuid.UUID
	NewAttemptID func() string
	UploadTTL    time.Duration
	Logger       *slog.Logger
	// Recommender (AI service) and JobDirectory (job service) back
	// ListRecommendedJobs; nil means matching is unavailable (ADR 0010).
	Recommender  Recommender
	JobDirectory JobDirectory
}

// Service implements the candidate use cases.
type Service struct{ Deps }

// NewService wires a Service, defaulting Now, NewID, NewAttemptID, UploadTTL
// and Logger.
func NewService(d Deps) *Service {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.NewID == nil {
		d.NewID = uuid.New
	}
	if d.NewAttemptID == nil {
		d.NewAttemptID = func() string { return ulid.Make().String() }
	}
	if d.UploadTTL == 0 {
		d.UploadTTL = defaultUploadTTL
	}
	if d.Logger == nil {
		d.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Service{Deps: d}
}

// candidate requires the candidate role and returns the caller's user ID.
func candidate(ctx context.Context) (uuid.UUID, error) {
	if err := authz.RequireRole(ctx, authz.RoleCandidate); err != nil {
		return uuid.Nil, err
	}
	c, _ := authz.ClaimsFrom(ctx)
	id, err := uuid.Parse(c.Subject)
	if err != nil {
		return uuid.Nil, apperr.New(apperr.ErrUnauthenticated, "invalid subject")
	}
	return id, nil
}
