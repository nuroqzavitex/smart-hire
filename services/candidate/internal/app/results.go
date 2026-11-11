package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

// HandleParseResult applies a cv.parsed or cv.parse_failed envelope (raw
// JSON) from the AI service. Duplicate events (by event_id) and results for a
// stale attempt are ignored; events that can never apply return an error
// wrapping ErrPoisonEvent. Errors never carry payload values.
func (s *Service) HandleParseResult(ctx context.Context, raw []byte) error {
	var env event.Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("%w: malformed envelope", ErrPoisonEvent)
	}
	if env.EventType != evParsed && env.EventType != evParseFailed {
		return fmt.Errorf("%w: unexpected event type", ErrPoisonEvent)
	}
	if err := s.Schema.ValidateEvent(env.EventType, raw); err != nil {
		// The schema error echoes offending values (E-R6); keep it out.
		return fmt.Errorf("%w: %s payload failed schema validation", ErrPoisonEvent, env.EventType)
	}
	var p cvResultPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return fmt.Errorf("%w: malformed %s payload", ErrPoisonEvent, env.EventType)
	}
	cvID, err := uuid.Parse(p.CVID)
	if err != nil {
		return fmt.Errorf("%w: invalid cv_id", ErrPoisonEvent)
	}
	return s.Tx.WithTx(ctx, func(ctx context.Context) error {
		first, err := s.Idem.Once(ctx, env.EventID)
		if err != nil {
			return fmt.Errorf("record processed event: %w", err)
		}
		if !first {
			return nil
		}
		cv, err := s.CVs.GetForUpdate(ctx, cvID)
		if errors.Is(err, domain.ErrCVNotFound) {
			return fmt.Errorf("%w: cv %s not found", ErrPoisonEvent, cvID)
		}
		if err != nil {
			return fmt.Errorf("load cv: %w", err)
		}
		if cv.CandidateID.String() != p.CandidateID {
			return fmt.Errorf("%w: candidate_id does not own cv %s", ErrPoisonEvent, cvID)
		}
		now := s.Now()
		if env.EventType == evParsed {
			err = cv.ApplyParsed(p.AttemptID, p.Parsed, now)
		} else {
			err = cv.ApplyFailed(p.AttemptID, p.Reason, now)
		}
		if errors.Is(err, domain.ErrStaleResult) {
			s.Logger.InfoContext(ctx, "stale cv parse result ignored",
				"cv_id", cvID.String(), "attempt_id", p.AttemptID, "event_id", env.EventID)
			return nil // commit: the event stays recorded as processed
		}
		if err != nil {
			return err
		}
		if err := s.CVs.Update(ctx, cv); err != nil {
			return fmt.Errorf("update cv: %w", err)
		}
		return nil
	})
}
