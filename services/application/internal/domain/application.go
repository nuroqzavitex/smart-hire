package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Reasons recorded when the saga ends somewhere other than SCREENED.
const (
	ReasonQuotaExceeded = "quota_exceeded"
	ReasonJobClosed     = "job_closed"
	ReasonTimeout       = "timeout"
	ReasonAIUnavailable = "ai_unavailable"
	ReasonNotMatched    = "not_matched"
)

// ActorSystem is the history actor of saga transitions.
const ActorSystem = "system"

// Application is one candidate's application to one job with one CV.
// RecruiterID is copied from the job at submit time so recruiters can list
// their applications without asking the job service.
type Application struct {
	ID             uuid.UUID
	JobID          uuid.UUID
	CandidateID    uuid.UUID
	RecruiterID    uuid.UUID
	CVID           uuid.UUID
	Status         Status
	Reason         string
	Score          *float64
	Breakdown      json.RawMessage
	ModelVersion   string
	IdempotencyKey string
	RequestHash    string
	Version        int64
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// HistoryEntry is one status change; From is nil for the creation entry.
type HistoryEntry struct {
	From   *Status
	To     Status
	Actor  string
	Reason string
	At     time.Time
}
