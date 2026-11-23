package app

import (
	"context"
	"fmt"
	"time"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

const source = "application-service"

const (
	evSubmitted     = "application.submitted"
	evStatusChanged = "application.status_changed"
)

type submittedPayload struct {
	ApplicationID string `json:"application_id"`
	JobID         string `json:"job_id"`
	CandidateID   string `json:"candidate_id"`
	RecruiterID   string `json:"recruiter_id"`
	CVID          string `json:"cv_id"`
}

type statusChangedPayload struct {
	ApplicationID string   `json:"application_id"`
	JobID         string   `json:"job_id"`
	CandidateID   string   `json:"candidate_id"`
	RecruiterID   string   `json:"recruiter_id"`
	From          *string  `json:"from"`
	To            string   `json:"to"`
	Reason        *string  `json:"reason"`
	Score         *float64 `json:"score"`
	Version       int64    `json:"version"`
	Actor         string   `json:"actor"`
}

func (s *Service) emitSubmitted(ctx context.Context, a domain.Application, at time.Time) error {
	return s.emit(ctx, evSubmitted, a, submittedPayload{
		ApplicationID: a.ID.String(), JobID: a.JobID.String(), CandidateID: a.CandidateID.String(),
		RecruiterID: a.RecruiterID.String(), CVID: a.CVID.String(),
	}, at)
}

// emitStatusChanged writes application.status_changed for a's current
// state; from is nil for the creation event. It must run inside the
// transaction that saved a.
func (s *Service) emitStatusChanged(ctx context.Context, a domain.Application, from *domain.Status, actor string, at time.Time) error {
	p := statusChangedPayload{
		ApplicationID: a.ID.String(), JobID: a.JobID.String(), CandidateID: a.CandidateID.String(),
		RecruiterID: a.RecruiterID.String(), To: string(a.Status), Score: a.Score, Version: a.Version, Actor: actor,
	}
	if from != nil {
		f := string(*from)
		p.From = &f
	}
	if a.Reason != "" {
		r := a.Reason
		p.Reason = &r
	}
	return s.emit(ctx, evStatusChanged, a, p, at)
}

func (s *Service) emit(ctx context.Context, typ string, a domain.Application, payload any, at time.Time) error {
	env, err := event.New(ctx, source, typ, 1, payload, at)
	if err != nil {
		return fmt.Errorf("build event: %w", err)
	}
	if err := s.Outbox.Write(ctx, env, a.ID.String()); err != nil {
		return fmt.Errorf("write outbox: %w", err)
	}
	return nil
}
