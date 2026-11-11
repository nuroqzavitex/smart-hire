package app

import (
	"context"
	"fmt"

	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

// GetProfile returns the caller's profile (domain.ErrProfileNotFound until
// the first UpsertProfile).
func (s *Service) GetProfile(ctx context.Context) (domain.Candidate, error) {
	uid, err := candidate(ctx)
	if err != nil {
		return domain.Candidate{}, err
	}
	return s.Candidates.Get(ctx, uid)
}

// UpsertProfile creates the caller's profile on first use and replaces it
// afterwards; created_at is kept.
func (s *Service) UpsertProfile(ctx context.Context, in domain.ProfileInput) (domain.Candidate, error) {
	uid, err := candidate(ctx)
	if err != nil {
		return domain.Candidate{}, err
	}
	c, err := domain.NewProfile(uid, in, s.Now())
	if err != nil {
		return domain.Candidate{}, err
	}
	out, err := s.Candidates.Upsert(ctx, c)
	if err != nil {
		return domain.Candidate{}, fmt.Errorf("upsert profile: %w", err)
	}
	return out, nil
}
