package app

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

const source = "candidate-service"

const (
	evUploaded    = "cv.uploaded"
	evParsed      = "cv.parsed"
	evParseFailed = "cv.parse_failed"
	evUpdated     = "cv.updated"
)

// cvUpdatedPayload carries a CV's parse state after every change (ADR 0010).
// Parsed is JSON null unless the CV is PARSED.
type cvUpdatedPayload struct {
	CVID        string          `json:"cv_id"`
	CandidateID string          `json:"candidate_id"`
	Version     int64           `json:"version"`
	Status      string          `json:"status"`
	Parsed      json.RawMessage `json:"parsed"`
	ParsedAt    *time.Time      `json:"parsed_at"`
}

type cvUploadedPayload struct {
	CVID        string `json:"cv_id"`
	CandidateID string `json:"candidate_id"`
	ObjectKey   string `json:"object_key"`
	AttemptID   string `json:"attempt_id"`
}

// cvResultPayload decodes the payload of cv.parsed and cv.parse_failed.
type cvResultPayload struct {
	CVID         string          `json:"cv_id"`
	CandidateID  string          `json:"candidate_id"`
	AttemptID    string          `json:"attempt_id"`
	Parsed       json.RawMessage `json:"parsed"`
	ModelVersion string          `json:"model_version"`
	Reason       string          `json:"reason"`
	Detail       string          `json:"detail"`
}

// emitUploaded writes cv.uploaded for the CV's current attempt; it must run
// inside the command's transaction.
func (s *Service) emitUploaded(ctx context.Context, cv domain.CV, at time.Time) error {
	env, err := event.New(ctx, source, evUploaded, 1, cvUploadedPayload{
		CVID:        cv.ID.String(),
		CandidateID: cv.CandidateID.String(),
		ObjectKey:   cv.ObjectKey,
		AttemptID:   cv.AttemptID,
	}, at)
	if err != nil {
		return fmt.Errorf("build event: %w", err)
	}
	if err := s.Outbox.Write(ctx, env, cv.ID.String()); err != nil {
		return fmt.Errorf("write outbox: %w", err)
	}
	return nil
}

// emitUpdated writes cv.updated with the CV's state after the change; it must
// run inside the command's transaction, after the CV is saved.
func (s *Service) emitUpdated(ctx context.Context, cv domain.CV, at time.Time) error {
	parsed := json.RawMessage("null")
	var parsedAt *time.Time
	if cv.Status == domain.StatusParsed && len(cv.ParsedData) > 0 {
		parsed = cv.ParsedData
		if cv.ParsedAt != nil {
			t := cv.ParsedAt.UTC()
			parsedAt = &t
		}
	}
	env, err := event.New(ctx, source, evUpdated, 1, cvUpdatedPayload{
		CVID:        cv.ID.String(),
		CandidateID: cv.CandidateID.String(),
		Version:     cv.Version,
		Status:      string(cv.Status),
		Parsed:      parsed,
		ParsedAt:    parsedAt,
	}, at)
	if err != nil {
		return fmt.Errorf("build event: %w", err)
	}
	if err := s.Outbox.Write(ctx, env, cv.ID.String()); err != nil {
		return fmt.Errorf("write outbox: %w", err)
	}
	return nil
}
