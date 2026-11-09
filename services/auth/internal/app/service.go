package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

// Deps are the collaborators of Service.
type Deps struct {
	Users      UserRepo
	Tokens     RefreshTokenRepo
	Outbox     OutboxWriter
	Tx         TxManager
	Hasher     PasswordHasher
	Issuer     TokenIssuer
	Now        func() time.Time
	RefreshTTL time.Duration
	Logger     *slog.Logger
}

// Service implements the auth use cases.
type Service struct {
	Deps
	dummyHash string
}

// TokenPair is the result of a successful authentication.
type TokenPair struct {
	AccessToken  string
	ExpiresIn    time.Duration
	RefreshToken string
}

// NewService builds a Service and precomputes the dummy hash used to equalise
// login timing for unknown emails.
func NewService(d Deps) (*Service, error) {
	dummy, err := d.Hasher.Hash("dummy-password-for-timing")
	if err != nil {
		return nil, fmt.Errorf("compute dummy hash: %w", err)
	}
	return &Service{Deps: d, dummyHash: dummy}, nil
}

// issuePair mints an access token and a new refresh token in the given family,
// stores the refresh token and returns its ID.
func (s *Service) issuePair(ctx context.Context, u domain.User, familyID uuid.UUID) (TokenPair, uuid.UUID, error) {
	now := s.Now()
	access, ttl, err := s.Issuer.IssueAccess(u, now)
	if err != nil {
		return TokenPair{}, uuid.Nil, fmt.Errorf("issue access token: %w", err)
	}
	raw, hash, err := newRefreshSecret()
	if err != nil {
		return TokenPair{}, uuid.Nil, err
	}
	rt := domain.RefreshToken{
		ID: uuid.New(), UserID: u.ID, FamilyID: familyID, Hash: hash,
		ExpiresAt: now.Add(s.RefreshTTL), CreatedAt: now,
	}
	if err := s.Tokens.Create(ctx, rt); err != nil {
		return TokenPair{}, uuid.Nil, fmt.Errorf("store refresh token: %w", err)
	}
	return TokenPair{AccessToken: access, ExpiresIn: ttl, RefreshToken: raw}, rt.ID, nil
}
