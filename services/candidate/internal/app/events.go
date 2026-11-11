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
)

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
