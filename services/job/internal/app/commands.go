package app

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

// Create makes a new draft job for the calling recruiter and emits job.created.
func (s *Service) Create(ctx context.Context, d domain.Draft) (domain.Job, error) {
	claims, err := recruiterClaims(ctx)
	if err != nil {
		return domain.Job{}, err
	}
	rid, err := uuid.Parse(claims.Subject)
	if err != nil {
		return domain.Job{}, apperr.New(apperr.ErrUnauthenticated, "invalid subject")
	}
	var out domain.Job
	err = s.Tx.WithTx(ctx, func(ctx context.Context) error {
		now := s.Now()
		j, err := domain.NewJob(s.NewID(), rid, d, now)
		if err != nil {
			return err
		}
		if err := s.Jobs.Create(ctx, j); err != nil {
			return fmt.Errorf("create job: %w", err)
		}
		if err := s.emit(ctx, j, evCreated, payloadOf(j), now); err != nil {
			return err
		}
		out = j
		return nil
	})
	if err != nil {
		return domain.Job{}, err
	}
	return out, nil
}

// Update applies a patch to an owned job. An empty patch is a no-op.
func (s *Service) Update(ctx context.Context, id uuid.UUID, p domain.Patch) (domain.Job, error) {
	return s.mutate(ctx, id, func(j *domain.Job, now time.Time) (string, func(domain.Job) any, error) {
		if p.Empty() {
			return "", nil, nil
		}
		return evUpdated, func(j domain.Job) any { return payloadOf(j) }, j.Apply(p, now)
	})
}

// Publish opens an owned draft job.
func (s *Service) Publish(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	return s.mutate(ctx, id, func(j *domain.Job, now time.Time) (string, func(domain.Job) any, error) {
		return evUpdated, func(j domain.Job) any { return payloadOf(j) }, j.Publish(now)
	})
}

// Close closes an owned open job.
func (s *Service) Close(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	return s.mutate(ctx, id, func(j *domain.Job, now time.Time) (string, func(domain.Job) any, error) {
		return evClosed, refPayload, j.Close(now)
	})
}

// Delete soft-deletes an owned draft or closed job.
func (s *Service) Delete(ctx context.Context, id uuid.UUID) error {
	_, err := s.mutate(ctx, id, func(j *domain.Job, now time.Time) (string, func(domain.Job) any, error) {
		return evDeleted, refPayload, j.Delete(now)
	})
	return err
}

func refPayload(j domain.Job) any {
	return jobRefPayload{JobID: j.ID.String(), Version: j.Version}
}

// mutate is the common owner-command flow. In one transaction it locks the
// row, hides deleted/foreign jobs as NotFound, then calls fn, which applies
// the domain change and names the event to emit (empty type = no change, no
// event). The clock is read only after the lock so same-aggregate events
// cannot invert in the outbox.
func (s *Service) mutate(ctx context.Context, id uuid.UUID,
	fn func(j *domain.Job, now time.Time) (eventType string, payload func(domain.Job) any, err error),
) (domain.Job, error) {
	claims, err := recruiterClaims(ctx)
	if err != nil {
		return domain.Job{}, err
	}
	var out domain.Job
	err = s.Tx.WithTx(ctx, func(ctx context.Context) error {
		j, err := s.Jobs.GetForUpdate(ctx, id)
		if err != nil {
			return err
		}
		if j.DeletedAt != nil || !j.OwnedBy(claims.Subject) {
			return domain.ErrJobNotFound
		}
		now := s.Now()
		evType, payload, err := fn(&j, now)
		if err != nil {
			return err
		}
		if evType == "" {
			out = j
			return nil
		}
		if err := s.Jobs.Update(ctx, j); err != nil {
			return fmt.Errorf("update job: %w", err)
		}
		if err := s.emit(ctx, j, evType, payload(j), now); err != nil {
			return err
		}
		out = j
		return nil
	})
	if err != nil {
		return domain.Job{}, err
	}
	return out, nil
}

func recruiterClaims(ctx context.Context) (authz.Claims, error) {
	if err := authz.RequireRole(ctx, authz.RoleRecruiter); err != nil {
		return authz.Claims{}, err
	}
	c, _ := authz.ClaimsFrom(ctx)
	return c, nil
}
