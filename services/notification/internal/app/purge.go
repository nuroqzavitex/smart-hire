package app

import (
	"context"
	"fmt"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
)

// handleUserDeleted purges a deleted account (spec §6.5, ADR 0014): in one
// transaction it deletes the user's notifications and queued emails, turns
// the user replica into a tombstone (no address; later events notify nobody),
// forgets the user as owner of replicated jobs and answers user.data_purged.
// After the commit the user's open WebSockets are closed. A new user.deleted
// (a republish) confirms again.
func (s *Service) handleUserDeleted(ctx context.Context, env event.Envelope) error {
	d, err := userdeletion.ParseDeleted(env)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPoisonEvent, err)
	}
	removed, first := 0, false
	err = s.Tx.WithTx(ctx, func(ctx context.Context) error {
		var err error
		if first, err = s.Idem.Once(ctx, env.EventID); err != nil || !first {
			return err
		}
		if removed, err = s.Notifications.DeleteByUser(ctx, d.UserID); err != nil {
			return fmt.Errorf("delete notifications: %w", err)
		}
		if err := s.Users.Tombstone(ctx, d.UserID); err != nil {
			return fmt.Errorf("tombstone user: %w", err)
		}
		if err := s.Jobs.ClearRecruiter(ctx, d.UserID); err != nil {
			return fmt.Errorf("clear job owner: %w", err)
		}
		purged, err := userdeletion.NewPurgedEvent(ctx, d, userdeletion.ServiceNotification, s.Now())
		if err != nil {
			return fmt.Errorf("build event: %w", err)
		}
		return s.Outbox.Write(ctx, purged, d.UserID.String())
	})
	if err != nil || !first {
		return err
	}
	s.Pusher.Disconnect(d.UserID)
	s.Logger.InfoContext(ctx, "notification data purged", "user_id", d.UserID.String(),
		"deletion_request_id", d.RequestID.String(), "notifications_removed", removed)
	return nil
}
