package app

import (
	"context"
	"strings"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

const (
	defaultPageSize = 20
	maxPageSize     = 100
)

// PageRequest selects a page; Size 0 means the default (20).
type PageRequest struct {
	Size  int
	Token string
}

// Page is one page of jobs; NextToken is empty on the last page.
type Page struct {
	Jobs      []domain.Job
	NextToken string
}

func (pr PageRequest) size() (int, error) {
	switch {
	case pr.Size == 0:
		return defaultPageSize, nil
	case pr.Size < 0 || pr.Size > maxPageSize:
		return 0, apperr.New(apperr.ErrValidation, "page_size must be between 1 and 100")
	}
	return pr.Size, nil
}

// Get returns a job: open jobs are public; the owner sees every state.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	j, err := s.Jobs.Get(ctx, id)
	if err != nil {
		return domain.Job{}, err
	}
	if j.DeletedAt != nil {
		return domain.Job{}, domain.ErrJobNotFound
	}
	if j.Status == domain.StatusOpen {
		return j, nil
	}
	if c, ok := authz.ClaimsFrom(ctx); ok && j.OwnedBy(c.Subject) {
		return j, nil
	}
	return domain.Job{}, domain.ErrJobNotFound
}

// ListOpen pages through open jobs matching the filter.
func (s *Service) ListOpen(ctx context.Context, f SearchFilter, pr PageRequest) (Page, error) {
	size, err := pr.size()
	if err != nil {
		return Page{}, err
	}
	f.Query = strings.TrimSpace(f.Query)
	f.Skills = lowerSkills(f.Skills)
	var after *Cursor
	if pr.Token != "" {
		c, err := DecodeCursor(pr.Token)
		if err != nil {
			return Page{}, err
		}
		// A cursor carries a rank only for text queries; reject mismatches.
		if (c.Rank != nil) != (f.Query != "") {
			return Page{}, apperr.New(apperr.ErrValidation, "invalid page token")
		}
		after = &c
	}
	hits, err := s.Jobs.Search(ctx, f, after, size+1)
	if err != nil {
		return Page{}, err
	}
	more := len(hits) > size
	if more {
		hits = hits[:size]
	}
	page := Page{Jobs: make([]domain.Job, len(hits))}
	for i, h := range hits {
		page.Jobs[i] = h.Job
	}
	if more {
		last := hits[len(hits)-1]
		c := Cursor{ID: last.Job.ID}
		if last.Job.PublishedAt != nil {
			c.At = *last.Job.PublishedAt
		}
		if f.Query != "" {
			r := last.Rank
			c.Rank = &r
		}
		page.NextToken = EncodeCursor(c)
	}
	return page, nil
}

// ListMine pages through the calling recruiter's jobs.
func (s *Service) ListMine(ctx context.Context, status *domain.Status, pr PageRequest) (Page, error) {
	claims, err := recruiterClaims(ctx)
	if err != nil {
		return Page{}, err
	}
	rid, err := uuid.Parse(claims.Subject)
	if err != nil {
		return Page{}, apperr.New(apperr.ErrUnauthenticated, "invalid subject")
	}
	size, err := pr.size()
	if err != nil {
		return Page{}, err
	}
	var after *Cursor
	if pr.Token != "" {
		c, err := DecodeCursor(pr.Token)
		if err != nil {
			return Page{}, err
		}
		if c.Rank != nil {
			return Page{}, apperr.New(apperr.ErrValidation, "invalid page token")
		}
		after = &c
	}
	jobs, err := s.Jobs.ListByRecruiter(ctx, rid, status, after, size+1)
	if err != nil {
		return Page{}, err
	}
	more := len(jobs) > size
	if more {
		jobs = jobs[:size]
	}
	page := Page{Jobs: jobs}
	if more {
		last := jobs[len(jobs)-1]
		page.NextToken = EncodeCursor(Cursor{At: last.CreatedAt, ID: last.ID})
	}
	return page, nil
}

func lowerSkills(in []string) []string {
	var out []string
	for _, s := range in {
		if s = strings.ToLower(strings.TrimSpace(s)); s != "" {
			out = append(out, s)
		}
	}
	return out
}
