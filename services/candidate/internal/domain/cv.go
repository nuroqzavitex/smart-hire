package domain

import (
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
)

// Upload limits.
const (
	MaxCVBytes     int64 = 5 << 20
	PDFContentType       = "application/pdf"
)

const maxFileNameRunes = 255

// StaleUploadAfter is how long a CV may wait in UPLOADED for the current
// attempt's result before the candidate may reparse it. It exceeds the AI
// worker's max.poll.interval.ms (900s), so an attempt still being processed
// is not restarted; past it the cv.uploaded or result event was most likely
// dead-lettered and the CV would otherwise stay UPLOADED forever.
const StaleUploadAfter = 15 * time.Minute

// Status is the lifecycle state of a CV.
type Status string

// CV statuses.
const (
	StatusUploading Status = "UPLOADING"
	StatusUploaded  Status = "UPLOADED"
	StatusParsed    Status = "PARSED"
	StatusFailed    Status = "FAILED"
)

// CV is an uploaded resume and its parse state. AttemptID identifies the
// current parse attempt; results for any other attempt are stale.
type CV struct {
	ID          uuid.UUID
	CandidateID uuid.UUID
	ObjectKey   string
	FileName    string
	Status      Status
	AttemptID   string
	ParsedData  json.RawMessage
	ParseError  *string
	SizeBytes   *int64
	ConsentAt   time.Time
	ParsedAt    *time.Time
	Version     int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// ObjectKey returns the storage key of a CV's PDF.
func ObjectKey(candidateID, cvID uuid.UUID) string {
	return "cvs/" + candidateID.String() + "/" + cvID.String() + ".pdf"
}

// NewCV creates a CV awaiting upload; creating it records the candidate's
// consent at now. fileName is display-only.
func NewCV(id, candidateID uuid.UUID, fileName string, now time.Time) (CV, error) {
	name := strings.TrimSpace(fileName)
	if n := utf8.RuneCountInString(name); n < 1 || n > maxFileNameRunes {
		return CV{}, apperr.New(apperr.ErrValidation, "file_name must be 1-255 characters")
	}
	return CV{
		ID:          id,
		CandidateID: candidateID,
		ObjectKey:   ObjectKey(candidateID, id),
		FileName:    name,
		Status:      StatusUploading,
		Version:     1,
		ConsentAt:   now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}

// Complete marks the upload as done and starts parse attempt attemptID.
func (c *CV) Complete(sizeBytes int64, attemptID string, now time.Time) error {
	if c.Status != StatusUploading {
		return ErrInvalidTransition
	}
	c.Status = StatusUploaded
	c.SizeBytes = &sizeBytes
	c.AttemptID = attemptID
	c.touch(now)
	return nil
}

// ApplyParsed stores a successful parse result for the current attempt.
func (c *CV) ApplyParsed(attemptID string, parsed json.RawMessage, now time.Time) error {
	if !c.awaitsResult(attemptID) {
		return ErrStaleResult
	}
	c.Status = StatusParsed
	c.ParsedData = parsed
	c.ParseError = nil
	c.ParsedAt = &now
	c.touch(now)
	return nil
}

// ApplyFailed records a failed parse for the current attempt.
func (c *CV) ApplyFailed(attemptID, reason string, now time.Time) error {
	if !c.awaitsResult(attemptID) {
		return ErrStaleResult
	}
	c.Status = StatusFailed
	c.ParseError = &reason
	c.ParsedAt = &now
	c.touch(now)
	return nil
}

// UpdateParsed replaces the parsed data with the candidate's edits.
func (c *CV) UpdateParsed(parsed json.RawMessage, now time.Time) error {
	if c.Status != StatusParsed {
		return ErrInvalidTransition
	}
	c.ParsedData = parsed
	c.touch(now)
	return nil
}

// Reparse discards the previous result and starts parse attempt attemptID.
// It is allowed from PARSED and FAILED, and from UPLOADED once the current
// attempt (started at UpdatedAt) is at least StaleUploadAfter old.
func (c *CV) Reparse(attemptID string, now time.Time) error {
	switch c.Status {
	case StatusParsed, StatusFailed:
	case StatusUploaded:
		if now.Sub(c.UpdatedAt) < StaleUploadAfter {
			return ErrInvalidTransition
		}
	default:
		return ErrInvalidTransition
	}
	c.Status = StatusUploaded
	c.AttemptID = attemptID
	c.ParsedData = nil
	c.ParseError = nil
	c.ParsedAt = nil
	c.touch(now)
	return nil
}

// OwnedBy reports whether subject is the candidate who owns the CV.
func (c CV) OwnedBy(subject string) bool {
	return subject != "" && c.CandidateID.String() == subject
}

func (c *CV) awaitsResult(attemptID string) bool {
	return c.Status == StatusUploaded && attemptID == c.AttemptID
}

func (c *CV) touch(now time.Time) {
	c.Version++
	c.UpdatedAt = now
}
