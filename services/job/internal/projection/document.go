// Package projection keeps the OpenSearch job read model in step with the
// job write model: it maps job.* events (and, for rebuilds, Postgres rows) to
// search documents written with external versions (ADR 0009).
package projection

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

// Statuses only the read model knows: a deleted job stays as a tombstone so a
// late, older event cannot resurrect it.
const (
	StatusOpen    = string(domain.StatusOpen)
	StatusClosed  = string(domain.StatusClosed)
	StatusDeleted = "deleted"
)

// Document is a job in the read model. Tombstones carry only JobID, Version
// and Status.
type Document struct {
	JobID            string     `json:"job_id"`
	Version          int64      `json:"version"`
	Status           string     `json:"status"`
	RecruiterID      string     `json:"recruiter_id,omitempty"`
	Title            string     `json:"title,omitempty"`
	Description      string     `json:"description,omitempty"`
	RequiredSkills   []string   `json:"required_skills,omitempty"`
	NiceToHaveSkills []string   `json:"nice_to_have_skills,omitempty"`
	Level            string     `json:"level,omitempty"`
	Location         string     `json:"location,omitempty"`
	SalaryMin        *int       `json:"salary_min,omitempty"`
	SalaryMax        *int       `json:"salary_max,omitempty"`
	PublishedAt      *time.Time `json:"published_at,omitempty"`
}

var errBadPayload = kafka.Permanent(errors.New("job event payload invalid"))

type fullPayload struct {
	JobID            string     `json:"job_id"`
	RecruiterID      string     `json:"recruiter_id"`
	Title            string     `json:"title"`
	Description      string     `json:"description"`
	RequiredSkills   []string   `json:"required_skills"`
	NiceToHaveSkills []string   `json:"nice_to_have_skills"`
	Level            string     `json:"level"`
	Location         string     `json:"location"`
	SalaryMin        *int       `json:"salary_min"`
	SalaryMax        *int       `json:"salary_max"`
	Status           string     `json:"status"`
	Version          int64      `json:"version"`
	PublishedAt      *time.Time `json:"published_at"`
}

type refPayload struct {
	JobID   string `json:"job_id"`
	Version int64  `json:"version"`
}

// FromEnvelope maps a job.* event to the document it writes. Unknown types
// and unparsable payloads are permanent errors (straight to the DLQ).
func FromEnvelope(env event.Envelope) (Document, error) {
	switch env.EventType {
	case "job.created", "job.updated":
		var p fullPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil || p.JobID == "" || p.Version < 1 {
			return Document{}, errBadPayload
		}
		published := p.PublishedAt
		// Events written before published_at existed (M1a) lack it.
		if published == nil && p.Status == StatusOpen {
			at := env.OccurredAt.UTC()
			published = &at
		}
		return Document{
			JobID: p.JobID, Version: p.Version, Status: p.Status, RecruiterID: p.RecruiterID,
			Title: p.Title, Description: p.Description,
			RequiredSkills: p.RequiredSkills, NiceToHaveSkills: p.NiceToHaveSkills,
			Level: p.Level, Location: p.Location, SalaryMin: p.SalaryMin, SalaryMax: p.SalaryMax,
			PublishedAt: published,
		}, nil
	case "job.closed", "job.deleted":
		var p refPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil || p.JobID == "" || p.Version < 1 {
			return Document{}, errBadPayload
		}
		status := StatusClosed
		if env.EventType == "job.deleted" {
			status = StatusDeleted
		}
		return Document{JobID: p.JobID, Version: p.Version, Status: status}, nil
	}
	return Document{}, kafka.Permanent(errors.New("unexpected event type"))
}

// FromJob maps a Postgres row to its document (used by rebuild): a
// soft-deleted job becomes a tombstone.
func FromJob(j domain.Job) Document {
	if j.DeletedAt != nil {
		return Document{JobID: j.ID.String(), Version: j.Version, Status: StatusDeleted}
	}
	var published *time.Time
	if j.PublishedAt != nil {
		at := j.PublishedAt.UTC()
		published = &at
	}
	return Document{
		JobID: j.ID.String(), Version: j.Version, Status: string(j.Status),
		RecruiterID: j.RecruiterID.String(), Title: j.Title, Description: j.Description,
		RequiredSkills: j.RequiredSkills, NiceToHaveSkills: j.NiceToHaveSkills,
		Level: string(j.Level), Location: j.Location, SalaryMin: j.SalaryMin, SalaryMax: j.SalaryMax,
		PublishedAt: published,
	}
}
