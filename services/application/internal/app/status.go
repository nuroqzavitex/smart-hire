package app

import (
	"context"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

// UpdateStatus moves an application along the recruiter part of the state
// machine (spec §5.5). Only the recruiter who owns the job, or an admin, may
// do it; other recruiters get ErrNotFound.
func (s *Service) UpdateStatus(ctx context.Context, id uuid.UUID, to domain.Status) (domain.Application, error) {
	if err := authz.RequireRole(ctx, authz.RoleRecruiter, authz.RoleAdmin); err != nil {
		return domain.Application{}, err
	}
	me, role, err := caller(ctx)
	if err != nil {
		return domain.Application{}, err
	}
	var out domain.Application
	err = s.Tx.WithTx(ctx, func(ctx context.Context) error {
		a, err := s.Applications.GetForUpdate(ctx, id)
		if err != nil {
			return err
		}
		if !visible(a, me, role) {
			return domain.ErrApplicationNotFound
		}
		if !domain.CanRecruiterMove(a.Status, to) {
			return domain.ErrInvalidTransition
		}
		out, err = s.transition(ctx, a, to, me.String(), func(a *domain.Application) { a.Reason = "" })
		return err
	})
	return out, err
}

// transition applies one status change with its history entry and event;
// mutate may adjust other fields. It must run inside a transaction.
func (s *Service) transition(ctx context.Context, a domain.Application, to domain.Status, actor string,
	mutate func(*domain.Application),
) (domain.Application, error) {
	now := s.Now().UTC()
	from := a.Status
	a.Status = to
	a.Version++
	a.UpdatedAt = now
	if mutate != nil {
		mutate(&a)
	}
	if err := s.Applications.Update(ctx, a); err != nil {
		return domain.Application{}, err
	}
	if err := s.Applications.AddHistory(ctx, a.ID, domain.HistoryEntry{
		From: &from, To: to, Actor: actor, Reason: a.Reason, At: now,
	}); err != nil {
		return domain.Application{}, err
	}
	if err := s.emitStatusChanged(ctx, a, &from, actor, now); err != nil {
		return domain.Application{}, err
	}
	return a, nil
}
