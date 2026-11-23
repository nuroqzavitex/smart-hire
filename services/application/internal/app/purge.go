package app

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
)

// PurgeStats counts what PurgeUser changed.
type PurgeStats struct {
	// Cancelled live applications of the candidate (their sagas end
	// COMPENSATED with a bumped version, so a leased worker cannot write).
	Cancelled int
	// Pseudonymised applications of the candidate (new random candidate_id
	// per application, no CV, idempotency key or score breakdown).
	Pseudonymised int
	// Recruiter applications whose recruiter_id became uuid.Nil.
	Recruiter int
}

// UserPurger pseudonymises a deleted user's applications in the tx carried
// by ctx ("keep the statistics, drop the personal link", spec §6.5).
type UserPurger interface {
	PurgeUser(ctx context.Context, userID uuid.UUID, now time.Time) (PurgeStats, error)
}

// HandleUserDeleted purges a deleted account (ADR 0014) and answers
// user.data_purged in the same transaction. The cancellations are not
// announced with application.status_changed: nobody is left to notify. A
// new user.deleted (a republish) runs the purge again, which then changes
// nothing, and confirms again.
func (s *Service) HandleUserDeleted(ctx context.Context, env event.Envelope) error {
	d, err := userdeletion.ParseDeleted(env)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPoisonEvent, err)
	}
	var stats PurgeStats
	err = s.Tx.WithTx(ctx, func(ctx context.Context) error {
		first, err := s.Idem.Once(ctx, env.EventID)
		if err != nil || !first {
			return err
		}
		now := s.Now().UTC()
		if stats, err = s.Purger.PurgeUser(ctx, d.UserID, now); err != nil {
			return fmt.Errorf("purge applications: %w", err)
		}
		purged, err := userdeletion.NewPurgedEvent(ctx, d, userdeletion.ServiceApplication, now)
		if err != nil {
			return fmt.Errorf("build event: %w", err)
		}
		return s.Outbox.Write(ctx, purged, d.UserID.String())
	})
	if err != nil {
		return err
	}
	s.Logger.InfoContext(ctx, "application data purged", "user_id", d.UserID.String(),
		"deletion_request_id", d.RequestID.String(), "cancelled", stats.Cancelled,
		"pseudonymised", stats.Pseudonymised, "recruiter_applications", stats.Recruiter)
	return nil
}
