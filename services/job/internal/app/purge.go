package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
)

// ErrPoisonEvent marks a consumed event that can never be processed.
var ErrPoisonEvent = errors.New("poison event")

// HandleUserDeleted purges a deleted recruiter (spec §6.5, ADR 0014): in one
// transaction every job of the user is closed, soft-deleted and given the
// nil owner, each emitting job.deleted (the projector, AI and application
// services already react to it), and user.data_purged answers Auth. A
// candidate owns no jobs and is confirmed straight away. A new user.deleted
// (a republish) confirms again without touching purged jobs.
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
		jobs, err := s.Jobs.ListByRecruiterForUpdate(ctx, d.UserID)
		if err != nil {
			return fmt.Errorf("list recruiter jobs: %w", err)
		}
		now := s.Now()
		removed := 0
		for _, j := range jobs {
			if !j.Purge(now) {
				continue
			}
			if err := s.Jobs.Update(ctx, j); err != nil {
				return fmt.Errorf("purge job: %w", err)
			}
			if err := s.emit(ctx, j, evDeleted, refPayload(j), now); err != nil {
				return err
			}
			removed++
		}
		purged, err := userdeletion.NewPurgedEvent(ctx, d, userdeletion.ServiceJob, now)
		if err != nil {
			return fmt.Errorf("build event: %w", err)
		}
		if err := s.Outbox.Write(ctx, purged, d.UserID.String()); err != nil {
			return fmt.Errorf("write outbox: %w", err)
		}
		s.Logger.InfoContext(ctx, "job data purged", "user_id", d.UserID.String(),
			"deletion_request_id", d.RequestID.String(), "jobs_removed", removed, "event_id", env.EventID)
		return nil
	})
}
