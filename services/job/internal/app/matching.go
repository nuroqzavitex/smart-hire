package app

import (
	"context"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

// MaxBatchGet bounds BatchGetJobs.
const MaxBatchGet = 50

// ListMatchedCandidates returns the AI ranking of candidates for a job. Only
// the owning recruiter or an admin may see it; another recruiter gets
// not-found so job ids are not probed (ADR 0010).
func (s *Service) ListMatchedCandidates(ctx context.Context, jobID uuid.UUID, topK int) (MatchList, error) {
	if err := authz.RequireRole(ctx, authz.RoleRecruiter, authz.RoleAdmin); err != nil {
		return MatchList{}, err
	}
	c, _ := authz.ClaimsFrom(ctx)
	j, err := s.Jobs.Get(ctx, jobID)
	if err != nil {
		return MatchList{}, err
	}
	if j.DeletedAt != nil || (c.Role != authz.RoleAdmin && !j.OwnedBy(c.Subject)) {
		return MatchList{}, domain.ErrJobNotFound
	}
	if s.Matcher == nil {
		return MatchList{}, apperr.New(apperr.ErrUnavailable, "matching temporarily unavailable")
	}
	return s.Matcher.FindCandidates(ctx, jobID, topK)
}

// BatchGetJobs returns the open jobs among ids in request order (duplicates
// and unknown or non-open ids dropped). Any signed-in caller may use it: open
// jobs are public.
func (s *Service) BatchGetJobs(ctx context.Context, ids []uuid.UUID) ([]domain.Job, error) {
	if err := authz.RequireRole(ctx); err != nil {
		return nil, err
	}
	if len(ids) > MaxBatchGet {
		return nil, apperr.New(apperr.ErrValidation, "at most 50 ids")
	}
	if len(ids) == 0 {
		return nil, nil
	}
	jobs, err := s.Jobs.GetOpenByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	byID := make(map[uuid.UUID]domain.Job, len(jobs))
	for _, j := range jobs {
		byID[j.ID] = j
	}
	out := make([]domain.Job, 0, len(jobs))
	for _, id := range ids {
		if j, ok := byID[id]; ok {
			out = append(out, j)
			delete(byID, id)
		}
	}
	return out, nil
}
