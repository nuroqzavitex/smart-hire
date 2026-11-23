package app

import (
	"context"
	"fmt"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
)

// HandleUserDeleted purges a deleted account (spec §6.5, ADR 0014). In one
// transaction it removes each CV file from storage, turns the CV into a
// DELETED tombstone (emitting cv.updated so the matcher drops it), deletes
// the profile and the quota rows, and answers user.data_purged. A storage
// error rolls everything back and the event is retried. Every new
// user.deleted (a republish) is confirmed again; tombstones are not touched
// twice. Errors never carry payload values.
func (s *Service) HandleUserDeleted(ctx context.Context, env event.Envelope) error {
	d, err := userdeletion.ParseDeleted(env)
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
		cvs, err := s.CVs.ListAllByCandidateForUpdate(ctx, d.UserID)
		if err != nil {
			return fmt.Errorf("list cvs: %w", err)
		}
		now := s.Now()
		removed := 0
		for _, cv := range cvs {
			if !cv.Tombstone(now) {
				continue
			}
			if err := s.Storage.Delete(ctx, cv.ObjectKey); err != nil {
				return fmt.Errorf("delete cv file: %w", err)
			}
			if err := s.CVs.Update(ctx, cv); err != nil {
				return fmt.Errorf("tombstone cv: %w", err)
			}
			if err := s.Outbox.DeleteCVUpdates(ctx, cv.ID.String()); err != nil {
				return fmt.Errorf("drop cv.updated outbox rows: %w", err)
			}
			if err := s.emitUpdated(ctx, cv, now); err != nil {
				return err
			}
			removed++
		}
		if err := s.Candidates.Delete(ctx, d.UserID); err != nil {
			return fmt.Errorf("delete profile: %w", err)
		}
		if err := s.Quota.DeleteCandidate(ctx, d.UserID); err != nil {
			return fmt.Errorf("delete quota: %w", err)
		}
		purged, err := userdeletion.NewPurgedEvent(ctx, d, userdeletion.ServiceCandidate, now)
		if err != nil {
			return fmt.Errorf("build event: %w", err)
		}
		if err := s.Outbox.Write(ctx, purged, d.UserID.String()); err != nil {
			return fmt.Errorf("write outbox: %w", err)
		}
		s.Logger.InfoContext(ctx, "candidate data purged", "user_id", d.UserID.String(),
			"deletion_request_id", d.RequestID.String(), "cvs_removed", removed, "event_id", env.EventID)
		return nil
	})
}
