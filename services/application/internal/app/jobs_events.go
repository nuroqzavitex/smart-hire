package app

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

type jobRefPayload struct {
	JobID string `json:"job_id"`
}

// HandleJobEvent consumes job.closed and job.deleted: it records the job as
// closed (both are terminal) and every saga of that job that is still
// RUNNING switches to compensation (release the quota, cancel
// the application). Applications past the saga are left to the recruiter.
// Other event types are ignored; duplicates are skipped by event id.
func (s *Service) HandleJobEvent(ctx context.Context, raw []byte) error {
	var env event.Envelope
	if err := json.Unmarshal(raw, &env); err != nil || env.EventID == "" {
		return fmt.Errorf("%w: malformed envelope", ErrPoisonEvent)
	}
	if env.EventType != "job.closed" && env.EventType != "job.deleted" {
		return nil
	}
	var p jobRefPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return fmt.Errorf("%w: malformed payload", ErrPoisonEvent)
	}
	jobID, err := uuid.Parse(p.JobID)
	if err != nil {
		return fmt.Errorf("%w: invalid job_id", ErrPoisonEvent)
	}
	cancelled := 0
	err = s.Tx.WithTx(ctx, func(ctx context.Context) error {
		first, err := s.Idem.Once(ctx, env.EventID)
		if err != nil || !first {
			return err
		}
		now := s.Now().UTC()
		if err := s.ClosedJobs.MarkClosed(ctx, jobID, now); err != nil {
			return err
		}
		cancelled, err = s.Sagas.CancelRunningForJob(ctx, jobID, domain.ReasonJobClosed, now)
		return err
	})
	if err != nil {
		return err
	}
	if cancelled > 0 {
		s.Logger.Info("job closed, compensating running sagas", "job_id", jobID, "sagas", cancelled)
		s.Wake()
	}
	return nil
}
