package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

const (
	defaultPageSize = 20
	maxPageSize     = 100
)

// ListInput filters and pages ListApplications.
type ListInput struct {
	JobID     *uuid.UUID
	Status    *domain.Status
	PageSize  int
	PageToken string
}

// visible reports whether the caller may see the application: its
// candidate, the recruiter who owns the job, or an admin.
func visible(a domain.Application, id uuid.UUID, role authz.Role) bool {
	switch role {
	case authz.RoleAdmin:
		return true
	case authz.RoleCandidate:
		return a.CandidateID == id
	case authz.RoleRecruiter:
		return a.RecruiterID == id
	}
	return false
}

// Get returns an application and its history; anyone who may not see it
// gets ErrNotFound.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (domain.Application, []domain.HistoryEntry, error) {
	me, role, err := caller(ctx)
	if err != nil {
		return domain.Application{}, nil, err
	}
	a, err := s.Applications.Get(ctx, id)
	if err != nil {
		return domain.Application{}, nil, err
	}
	if !visible(a, me, role) {
		return domain.Application{}, nil, domain.ErrApplicationNotFound
	}
	h, err := s.Applications.History(ctx, id)
	if err != nil {
		return domain.Application{}, nil, err
	}
	return a, h, nil
}

// List pages the caller's applications, newest first: a candidate's own, a
// recruiter's on their jobs, or all for an admin.
func (s *Service) List(ctx context.Context, in ListInput) ([]domain.Application, string, error) {
	me, role, err := caller(ctx)
	if err != nil {
		return nil, "", err
	}
	size := in.PageSize
	switch {
	case size == 0:
		size = defaultPageSize
	case size < 0 || size > maxPageSize:
		return nil, "", apperr.New(apperr.ErrValidation, "page_size must be 1-100")
	}
	var after *Cursor
	if in.PageToken != "" {
		c, err := DecodeCursor(in.PageToken)
		if err != nil {
			return nil, "", err
		}
		after = &c
	}
	f := ListFilter{JobID: in.JobID, Status: in.Status}
	switch role {
	case authz.RoleCandidate:
		f.CandidateID = &me
	case authz.RoleRecruiter:
		f.RecruiterID = &me
	case authz.RoleAdmin:
	default:
		return nil, "", errors.New("unknown role")
	}
	as, err := s.Applications.List(ctx, f, after, size+1)
	if err != nil {
		return nil, "", fmt.Errorf("list applications: %w", err)
	}
	next := ""
	if len(as) > size {
		as = as[:size]
		last := as[len(as)-1]
		next = EncodeCursor(Cursor{At: last.CreatedAt, ID: last.ID})
	}
	return as, next, nil
}
