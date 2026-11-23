package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// Page is one page of jobs; NextToken is empty on the last page. Facets
// are set only by the read model; Degraded means the Postgres fallback served
// a read-model query.
type Page struct {
	Jobs      []domain.Job
	NextToken string
	Facets    Facets
	Degraded  bool
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

// ListOpen pages through open jobs matching the filter. It reads the
// OpenSearch read model when one is configured and falls back to Postgres
// (Degraded) when that fails; a page token from the other backend restarts
// at page 1 (ADR 0009).
func (s *Service) ListOpen(ctx context.Context, f SearchFilter, pr PageRequest) (Page, error) {
	size, err := pr.size()
	if err != nil {
		return Page{}, err
	}
	f.Query = strings.TrimSpace(f.Query)
	f.Skills = lowerSkills(f.Skills)
	var tok *searchToken
	if pr.Token != "" {
		t, err := decodeSearchToken(pr.Token)
		if err != nil {
			return Page{}, err
		}
		tok = &t
	}
	if s.Search != nil {
		page, err := s.listFromReadModel(ctx, f, size, tok)
		if err == nil || errors.Is(err, apperr.ErrValidation) {
			return page, err
		}
		// The client went away: there is nobody to serve a fallback to.
		if ctx.Err() != nil {
			return Page{}, ctx.Err()
		}
		s.Logger.WarnContext(ctx, "job search read model failed, using postgres", "error_type", fmt.Sprintf("%T", err))
		searchFallback(ctx)
		page, err = s.listFromPostgres(ctx, f, size, tok)
		page.Degraded = true
		return page, err
	}
	return s.listFromPostgres(ctx, f, size, tok)
}

func (s *Service) listFromReadModel(ctx context.Context, f SearchFilter, size int, tok *searchToken) (Page, error) {
	textQuery := f.Query != ""
	var after json.RawMessage
	if tok != nil && tok.Backend == backendOpenSearch {
		// Sort keys differ with and without a text query.
		if tok.Text != textQuery {
			return Page{}, errBadToken
		}
		after = tok.After
	}
	res, err := s.Search.Search(ctx, SearchQuery{Filter: f, After: after, Size: size})
	if err != nil {
		return Page{}, err
	}
	page := Page{Jobs: res.Jobs, Facets: res.Facets}
	if res.Next != nil {
		page.NextToken = encodeSearchToken(searchToken{Backend: backendOpenSearch, Text: textQuery, After: res.Next})
	}
	return page, nil
}

func (s *Service) listFromPostgres(ctx context.Context, f SearchFilter, size int, tok *searchToken) (Page, error) {
	var after *Cursor
	if tok != nil && tok.Backend == backendPostgres {
		// A cursor carries a rank only for text queries; reject mismatches.
		if (tok.Cursor.Rank != nil) != (f.Query != "") {
			return Page{}, errBadToken
		}
		after = tok.Cursor
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
		page.NextToken = encodeSearchToken(searchToken{Backend: backendPostgres, Cursor: &c})
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
