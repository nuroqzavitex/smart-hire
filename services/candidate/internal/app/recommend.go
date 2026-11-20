package app

import (
	"context"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

// MatchBreakdown explains a match score (ADR 0010).
type MatchBreakdown struct {
	SemanticSimilarity    float64
	RequiredSkillCoverage float64
	ExperienceFit         float64
	LocationFit           float64
	MatchedSkills         []string
	MissingSkills         []string
}

// RankedJob is one job the AI service ranked for a CV.
type RankedJob struct {
	JobID     uuid.UUID
	Score     float64
	Breakdown MatchBreakdown
}

// Recommendations is the AI service's ranking of open jobs for a CV.
type Recommendations struct {
	Jobs         []RankedJob
	ModelVersion string
}

// Recommender asks the AI service for jobs matching a CV; it fails with
// apperr.ErrUnavailable when the service is down or its breaker is open.
type Recommender interface {
	RecommendJobs(ctx context.Context, cvID uuid.UUID, topK int) (Recommendations, error)
}

// JobSummary is what a recommendation shows of a job.
type JobSummary struct {
	ID        uuid.UUID
	Title     string
	Level     string
	Location  string
	SalaryMin *int
	SalaryMax *int
}

// JobDirectory returns the open jobs among ids (job service), in any order.
type JobDirectory interface {
	OpenJobs(ctx context.Context, ids []uuid.UUID) ([]JobSummary, error)
}

// RecommendedJob is a ranked job with its details.
type RecommendedJob struct {
	Job       JobSummary
	Score     float64
	Breakdown MatchBreakdown
}

// RecommendedJobs is the response of ListRecommendedJobs.
type RecommendedJobs struct {
	Jobs         []RecommendedJob
	ModelVersion string
}

// ListRecommendedJobs ranks open jobs for one of the caller's parsed CVs: the
// AI service orders them, the job service supplies their details, and jobs
// that closed in between are dropped.
func (s *Service) ListRecommendedJobs(ctx context.Context, cvID uuid.UUID, topK int) (RecommendedJobs, error) {
	uid, err := candidate(ctx)
	if err != nil {
		return RecommendedJobs{}, err
	}
	cv, err := s.ownedCV(ctx, s.CVs.Get, cvID, uid)
	if err != nil {
		return RecommendedJobs{}, err
	}
	if cv.Status != domain.StatusParsed {
		return RecommendedJobs{}, apperr.New(apperr.ErrValidation, "cv is not parsed")
	}
	if s.Recommender == nil || s.JobDirectory == nil {
		return RecommendedJobs{}, apperr.New(apperr.ErrUnavailable, "matching temporarily unavailable")
	}
	recs, err := s.Recommender.RecommendJobs(ctx, cvID, topK)
	if err != nil {
		return RecommendedJobs{}, err
	}
	out := RecommendedJobs{ModelVersion: recs.ModelVersion, Jobs: []RecommendedJob{}}
	if len(recs.Jobs) == 0 {
		return out, nil
	}
	ids := make([]uuid.UUID, len(recs.Jobs))
	for i, r := range recs.Jobs {
		ids[i] = r.JobID
	}
	jobs, err := s.JobDirectory.OpenJobs(ctx, ids)
	if err != nil {
		return RecommendedJobs{}, err
	}
	byID := make(map[uuid.UUID]JobSummary, len(jobs))
	for _, j := range jobs {
		byID[j.ID] = j
	}
	for _, r := range recs.Jobs {
		if j, ok := byID[r.JobID]; ok {
			out.Jobs = append(out.Jobs, RecommendedJob{Job: j, Score: r.Score, Breakdown: r.Breakdown})
		}
	}
	return out, nil
}
