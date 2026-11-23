package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

func tracer() trace.Tracer {
	return otel.Tracer("github.com/nuroqzavitex/smarthire/services/application/internal/app")
}

const (
	backoffBase    = time.Second
	backoffCeiling = 30 * time.Second
	maxLastError   = 200
)

// outcome is what one saga step decided: the saga's next state and, when
// the application changes too, its new status.
type outcome struct {
	saga domain.Saga
	// to is the application's new status; "" leaves it unchanged.
	to     domain.Status
	reason string
	score  *Score
}

// ProcessDue claims due sagas and runs one step of each. It returns how
// many were claimed; step failures are logged and retried later, so only a
// failed claim is returned as an error.
func (s *Service) ProcessDue(ctx context.Context) (int, error) {
	sagas, err := s.Sagas.Claim(ctx, s.Now().UTC(), s.SagaLease, s.SagaBatch)
	if err != nil {
		return 0, err
	}
	for _, sg := range sagas {
		// The whole batch shares one lease; a slow step earlier in the batch
		// can outlast it, and then another worker may already own this saga.
		if !s.Now().UTC().Before(sg.NextRunAt) {
			s.Logger.Info("saga lease expired before its step ran, skipped", "application_id", sg.ApplicationID)
			continue
		}
		if err := s.runStep(ctx, sg); err != nil {
			if ctx.Err() != nil {
				return len(sagas), nil //nolint:nilerr // shutting down; the lease expires and another run resumes
			}
			// The lease expires and the step runs again (steps are
			// idempotent). Error text holds no PII: ids and codes only.
			s.Logger.Error("saga step failed", "application_id", sg.ApplicationID,
				"step", string(sg.Step), "err", err)
		}
	}
	return len(sagas), nil
}

// runStep runs the saga's current step and records its outcome, in one span
// whose trace the outcome's events carry.
func (s *Service) runStep(ctx context.Context, sg domain.Saga) (err error) {
	step := string(sg.Step)
	ctx, span := tracer().Start(ctx, "saga "+step, trace.WithAttributes(
		attribute.String("application.id", sg.ApplicationID.String()),
		attribute.String("saga.step", step),
		attribute.String("saga.state", string(sg.State)),
		attribute.Int("saga.attempts", sg.Attempts),
	))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "saga step failed")
		}
		span.End()
	}()
	now := s.Now().UTC()
	var o outcome
	switch {
	case sg.State == domain.SagaCompensating:
		o = s.releaseQuota(ctx, sg, now)
	case sg.Step == domain.StepReserveQuota:
		o = s.reserveQuota(ctx, sg, now)
	case sg.Step == domain.StepScore:
		o = s.score(ctx, sg, now)
	default:
		return fmt.Errorf("saga in unexpected step %s / state %s", sg.Step, sg.State)
	}
	if ctx.Err() != nil {
		return ctx.Err() // the call was cut short by shutdown: not an outcome
	}
	return s.finish(ctx, sg, o, now)
}

func (s *Service) reserveQuota(ctx context.Context, sg domain.Saga, now time.Time) outcome {
	// job.closed may have been consumed before this saga's row was
	// committed, finding nothing to cancel; it left the closed job behind.
	if closed, err := s.ClosedJobs.IsClosed(ctx, sg.JobID); err != nil {
		return retry(sg, err, now)
	} else if closed {
		return compensate(sg, domain.ReasonJobClosed, now)
	}
	if !now.Before(sg.DeadlineAt) {
		return compensate(sg, domain.ReasonTimeout, now)
	}
	app, err := s.Applications.Get(ctx, sg.ApplicationID)
	if err != nil {
		return retry(sg, err, now)
	}
	switch err := s.Quota.Reserve(ctx, sg.ApplicationID, app.CandidateID); {
	case err == nil:
		next := sg
		next.Step, next.StepStartedAt, next.NextRunAt, next.Attempts, next.LastError = domain.StepScore, now, now, 0, ""
		return outcome{saga: next, to: domain.StatusScreening}
	case errors.Is(err, ErrQuotaExhausted):
		return outcome{saga: complete(sg), to: domain.StatusCancelled, reason: domain.ReasonQuotaExceeded}
	case errors.Is(err, ErrReservationReleased):
		reason := sg.CancelReason
		if reason == "" {
			reason = domain.ReasonTimeout
		}
		return compensate(sg, reason, now)
	default:
		return retry(sg, err, now)
	}
}

// score never cancels: an AI that is down, slow or has no data yet sends the
// application to a recruiter for manual review (spec §6.4, §14.2).
func (s *Service) score(ctx context.Context, sg domain.Saga, now time.Time) outcome {
	app, err := s.Applications.Get(ctx, sg.ApplicationID)
	if err != nil {
		return retry(sg, err, now)
	}
	sc, err := s.Scorer.Score(ctx, app.CVID, app.JobID)
	switch {
	case err == nil:
		return outcome{saga: complete(sg), to: domain.StatusScreened, score: &sc}
	case errors.Is(err, ErrNotMatched) && now.Before(sg.StepStartedAt.Add(s.ScoreNotFoundWindow)):
		return retry(sg, err, now) // the AI's replica may still be catching up
	case errors.Is(err, ErrNotMatched):
		return outcome{saga: complete(sg), to: domain.StatusNeedsManualReview, reason: domain.ReasonNotMatched}
	default:
		return outcome{saga: complete(sg), to: domain.StatusNeedsManualReview, reason: domain.ReasonAIUnavailable}
	}
}

// releaseQuota is the compensation; it retries until it succeeds (no
// deadline), then cancels the application with the saga's reason.
func (s *Service) releaseQuota(ctx context.Context, sg domain.Saga, now time.Time) outcome {
	if err := s.Quota.Release(ctx, sg.ApplicationID); err != nil {
		return retry(sg, err, now)
	}
	next := sg
	next.State, next.LastError = domain.SagaCompensated, ""
	return outcome{saga: next, to: domain.StatusCancelled, reason: sg.CancelReason}
}

func complete(sg domain.Saga) domain.Saga {
	sg.State, sg.LastError = domain.SagaCompleted, ""
	return sg
}

func compensate(sg domain.Saga, reason string, now time.Time) outcome {
	sg.State, sg.Step, sg.CancelReason = domain.SagaCompensating, domain.StepReleaseQuota, reason
	sg.NextRunAt, sg.Attempts, sg.LastError = now, 0, ""
	return outcome{saga: sg}
}

// retry schedules the same step again with exponential backoff:
// 1s·2^(attempt-1), capped at 30s.
func retry(sg domain.Saga, err error, now time.Time) outcome {
	sg.NextRunAt = now.Add(Backoff(sg.Attempts))
	msg := err.Error()
	if len(msg) > maxLastError {
		msg = msg[:maxLastError]
	}
	sg.LastError = msg
	return outcome{saga: sg}
}

// Backoff is the delay before retrying a step after its attempt-th failed
// attempt (attempt ≥ 1).
func Backoff(attempt int) time.Duration {
	d := backoffBase
	for i := 1; i < attempt && d < backoffCeiling; i++ {
		d *= 2
	}
	return min(d, backoffCeiling)
}

// finish writes the outcome in one transaction: the saga (only if nobody
// changed it since the claim), then the application, its history and the
// status_changed event.
func (s *Service) finish(ctx context.Context, claimed domain.Saga, o outcome, now time.Time) error {
	o.saga.UpdatedAt = now
	o.saga.Version = claimed.Version
	return s.Tx.WithTx(ctx, func(ctx context.Context) error {
		ok, err := s.Sagas.Save(ctx, o.saga)
		if err != nil {
			return err
		}
		if !ok {
			s.Logger.Info("saga changed during step, result dropped", "application_id", claimed.ApplicationID,
				"step", string(claimed.Step))
			return nil
		}
		s.Logger.Info("saga step finished", "application_id", claimed.ApplicationID, "step", string(claimed.Step),
			"saga_state", string(o.saga.State), "next_step", string(o.saga.Step), "status", string(o.to),
			"reason", o.reason, "last_error", o.saga.LastError)
		if o.to == "" {
			return nil
		}
		a, err := s.Applications.GetForUpdate(ctx, claimed.ApplicationID)
		if err != nil {
			return err
		}
		_, err = s.transition(ctx, a, o.to, domain.ActorSystem, func(a *domain.Application) {
			a.Reason = o.reason
			if o.score != nil {
				v := o.score.Value
				a.Score, a.Breakdown, a.ModelVersion = &v, o.score.Breakdown, o.score.ModelVersion
			}
		})
		return err
	})
}
