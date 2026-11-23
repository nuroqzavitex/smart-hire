package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

// ErrPoisonEvent marks a consumed event that can never be processed; the
// consumer sends it straight to the DLQ.
var ErrPoisonEvent = errors.New("poison event")

// sweepBatch bounds the requests handled per sweep.
const sweepBatch = 100

// DeleteMe starts the account-deletion saga (spec §6.5, ADR 0014). It
// re-checks the password, then in one transaction marks the account
// DELETION_PENDING, revokes every refresh token, records the request and
// writes user.deleted to the outbox. A repeat call returns the open request.
func (s *Service) DeleteMe(ctx context.Context, userID uuid.UUID, password string) (domain.DeletionRequest, error) {
	u, err := s.Users.GetByID(ctx, userID)
	if errors.Is(err, apperr.ErrNotFound) {
		return domain.DeletionRequest{}, apperr.New(apperr.ErrUnauthenticated, "invalid token")
	}
	if err != nil {
		return domain.DeletionRequest{}, err
	}
	ok, err := s.Hasher.Verify(password, u.PasswordHash)
	if err != nil {
		return domain.DeletionRequest{}, fmt.Errorf("verify password: %w", err)
	}
	if !ok {
		return domain.DeletionRequest{}, domain.ErrInvalidPassword
	}
	if u.Role == authz.RoleAdmin {
		return domain.DeletionRequest{}, domain.ErrAdminDeletion
	}

	var out domain.DeletionRequest
	err = s.Tx.WithTx(ctx, func(ctx context.Context) error {
		existing, err := s.Deletions.GetActiveByUser(ctx, u.ID)
		if err == nil {
			out = existing
			return nil
		}
		if !errors.Is(err, apperr.ErrNotFound) {
			return fmt.Errorf("load deletion request: %w", err)
		}
		now := s.Now()
		r := domain.NewDeletionRequest(u.ID, u.Role, userdeletion.Services, now)
		if err := s.Deletions.Create(ctx, r); err != nil {
			return err
		}
		if err := s.Users.SetStatus(ctx, u.ID, domain.StatusDeletionPending, now); err != nil {
			return fmt.Errorf("mark user pending deletion: %w", err)
		}
		if err := s.Tokens.RevokeAllForUser(ctx, u.ID, now); err != nil {
			return fmt.Errorf("revoke refresh tokens: %w", err)
		}
		if err := s.publishDeleted(ctx, r, now); err != nil {
			return err
		}
		out = r
		return nil
	})
	if errors.Is(err, apperr.ErrConflict) {
		// A concurrent call created the request first: answer with it.
		return s.Deletions.GetActiveByUser(ctx, u.ID)
	}
	if err != nil {
		return domain.DeletionRequest{}, err
	}
	s.Logger.InfoContext(ctx, "account deletion requested",
		"user_id", u.ID.String(), "deletion_request_id", out.ID.String())
	return out, nil
}

func (s *Service) publishDeleted(ctx context.Context, r domain.DeletionRequest, now time.Time) error {
	env, err := userdeletion.NewDeletedEvent(ctx, userdeletion.Deleted{
		UserID: r.UserID, Role: string(r.Role), RequestID: r.ID, RequestedAt: r.RequestedAt,
	}, now)
	if err != nil {
		return fmt.Errorf("build event: %w", err)
	}
	return s.Outbox.Write(ctx, env, r.UserID.String())
}

// GetDeletionRequest returns a request to an admin (proof of deletion).
func (s *Service) GetDeletionRequest(ctx context.Context, id uuid.UUID) (domain.DeletionRequest, error) {
	claims, ok := authz.ClaimsFrom(ctx)
	if !ok {
		return domain.DeletionRequest{}, domain.ErrDeletionRequiresJWT
	}
	if claims.Role != authz.RoleAdmin {
		return domain.DeletionRequest{}, domain.ErrDeletionAdminOnly
	}
	return s.Deletions.Get(ctx, id)
}

// HandleDataPurged records one service's user.data_purged. When every
// expected service confirmed, the user is hard-deleted and the request
// COMPLETED in the same transaction. Duplicates, confirmations of unknown
// requests and late confirmations are ignored; malformed or mismatching
// events wrap ErrPoisonEvent. Errors never carry payload values.
func (s *Service) HandleDataPurged(ctx context.Context, env event.Envelope) error {
	p, err := userdeletion.ParsePurged(env)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPoisonEvent, err)
	}
	return s.Tx.WithTx(ctx, func(ctx context.Context) error {
		first, err := s.Idem.Once(ctx, env.EventID)
		if err != nil {
			return fmt.Errorf("record processed event: %w", err)
		}
		if !first {
			return nil
		}
		r, err := s.Deletions.GetForUpdate(ctx, p.RequestID)
		if errors.Is(err, apperr.ErrNotFound) {
			s.Logger.InfoContext(ctx, "purge confirmation for unknown deletion request ignored",
				"deletion_request_id", p.RequestID.String(), "event_id", env.EventID)
			return nil
		}
		if err != nil {
			return fmt.Errorf("load deletion request: %w", err)
		}
		if r.UserID != p.UserID {
			return fmt.Errorf("%w: user_id does not match deletion request %s", ErrPoisonEvent, r.ID)
		}
		now := s.Now()
		done, err := r.MarkPurged(p.Service, now)
		if errors.Is(err, domain.ErrUnknownService) {
			return fmt.Errorf("%w: service not expected by deletion request %s", ErrPoisonEvent, r.ID)
		}
		if err != nil {
			return err
		}
		if err := s.Deletions.Update(ctx, r); err != nil {
			return fmt.Errorf("update deletion request: %w", err)
		}
		if !done {
			return nil
		}
		if err := s.Users.HardDelete(ctx, r.UserID); err != nil {
			return fmt.Errorf("hard delete user: %w", err)
		}
		s.Logger.InfoContext(ctx, "account deletion completed",
			"user_id", r.UserID.String(), "deletion_request_id", r.ID.String(),
			"took_seconds", now.Sub(r.RequestedAt).Seconds())
		return nil
	})
}

// SweepDeletions re-publishes user.deleted for unfinished requests whose last
// publish is older than DeletionRepublishAfter (a new event ID, so every
// service purges and confirms again) and flags PENDING requests older than
// DeletionStuckAfter as STUCK.
func (s *Service) SweepDeletions(ctx context.Context) (republished, stuck int, err error) {
	err = s.Tx.WithTx(ctx, func(ctx context.Context) error {
		republished, stuck = 0, 0
		now := s.Now()
		rs, err := s.Deletions.ClaimSweep(ctx, now.Add(-s.DeletionRepublishAfter), now.Add(-s.DeletionStuckAfter), sweepBatch)
		if err != nil {
			return fmt.Errorf("claim deletion requests: %w", err)
		}
		for _, r := range rs {
			if r.Status == domain.DeletionPending && r.RequestedAt.Before(now.Add(-s.DeletionStuckAfter)) {
				r.Status = domain.DeletionStuck
				stuck++
				s.Logger.WarnContext(ctx, "account deletion stuck",
					"user_id", r.UserID.String(), "deletion_request_id", r.ID.String(),
					"purged", r.Purged, "expected", r.Expected)
			}
			if r.LastPublishedAt.Before(now.Add(-s.DeletionRepublishAfter)) {
				if err := s.publishDeleted(ctx, r, now); err != nil {
					return err
				}
				r.LastPublishedAt = now
				republished++
			}
			if err := s.Deletions.Update(ctx, r); err != nil {
				return fmt.Errorf("update deletion request: %w", err)
			}
		}
		return nil
	})
	return republished, stuck, err
}

// DeletionCounts counts requests per status (account.deletions gauge).
func (s *Service) DeletionCounts(ctx context.Context) (map[domain.DeletionStatus]int64, error) {
	return s.Deletions.CountByStatus(ctx)
}
