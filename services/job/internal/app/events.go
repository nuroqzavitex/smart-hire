package app

import (
	"context"
	"fmt"
	"time"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

const source = "job-service"

const (
	evCreated = "job.created"
	evUpdated = "job.updated"
	evClosed  = "job.closed"
	evDeleted = "job.deleted"
)

type jobPayload struct {
	JobID            string   `json:"job_id"`
	RecruiterID      string   `json:"recruiter_id"`
	Title            string   `json:"title"`
	Description      string   `json:"description"`
	RequiredSkills   []string `json:"required_skills"`
	NiceToHaveSkills []string `json:"nice_to_have_skills"`
	Level            string   `json:"level"`
	Location         string   `json:"location"`
	SalaryMin        *int     `json:"salary_min"`
	SalaryMax        *int     `json:"salary_max"`
	Status           string   `json:"status"`
	Version          int64    `json:"version"`
}

type jobRefPayload struct {
	JobID   string `json:"job_id"`
	Version int64  `json:"version"`
}

func payloadOf(j domain.Job) jobPayload {
	return jobPayload{
		JobID: j.ID.String(), RecruiterID: j.RecruiterID.String(),
		Title: j.Title, Description: j.Description,
		RequiredSkills: nonNil(j.RequiredSkills), NiceToHaveSkills: nonNil(j.NiceToHaveSkills),
		Level: string(j.Level), Location: j.Location,
		SalaryMin: j.SalaryMin, SalaryMax: j.SalaryMax,
		Status: string(j.Status), Version: j.Version,
	}
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// emit writes a job event to the outbox with the given occurrence time; it
// must run inside the command's transaction.
func (s *Service) emit(ctx context.Context, j domain.Job, eventType string, payload any, at time.Time) error {
	env, err := event.New(ctx, source, eventType, 1, payload, at)
	if err != nil {
		return fmt.Errorf("build event: %w", err)
	}
	if err := s.Outbox.Write(ctx, env, j.ID.String()); err != nil {
		return fmt.Errorf("write outbox: %w", err)
	}
	return nil
}
