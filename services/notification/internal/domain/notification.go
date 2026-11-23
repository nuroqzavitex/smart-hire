// Package domain holds notifications, queued emails and the local replicas
// of users and jobs. It has no framework, database or proto imports.
package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
)

// Type is the kind of a notification (spec §4 of the M4a design).
type Type string

// Notification types.
const (
	TypeWelcome             Type = "welcome"
	TypeCVParsed            Type = "cv_parsed"
	TypeCVParseFailed       Type = "cv_parse_failed"
	TypeApplicationReceived Type = "application_received"
	TypeApplicationStatus   Type = "application_status"
	TypeCandidatesMatched   Type = "candidates_matched"
)

// Notification is one message for one user.
type Notification struct {
	ID        uuid.UUID
	UserID    uuid.UUID
	Type      Type
	Title     string
	Body      string
	Data      map[string]string
	ReadAt    *time.Time
	CreatedAt time.Time
}

// EmailStatus is the delivery state of a queued email.
type EmailStatus string

// Email statuses.
const (
	EmailPending EmailStatus = "PENDING"
	EmailSent    EmailStatus = "SENT"
	EmailFailed  EmailStatus = "FAILED"
)

// Email is a queued email (email outbox, ADR 0012).
type Email struct {
	ID             uuid.UUID
	NotificationID uuid.UUID
	To             string
	Subject        string
	HTML           string
	Text           string
	Status         EmailStatus
	Attempts       int
	NextAttemptAt  time.Time
	LastError      string
	CreatedAt      time.Time
}

// User is the local replica of an account, from user.registered. Deleted
// marks the tombstone of a deleted account (ADR 0014): no email, and no new
// notifications for it.
type User struct {
	ID      uuid.UUID
	Email   string
	Role    string
	Deleted bool
}

// Job is the local replica of a job, from job.*; Deleted marks a tombstone.
type Job struct {
	ID          uuid.UUID
	RecruiterID uuid.UUID
	Title       string
	Version     int64
	Deleted     bool
}

// Errors.
var (
	ErrNotificationNotFound = apperr.New(apperr.ErrNotFound, "notification not found")
	// ErrUserNotFound and ErrJobNotFound report a missing replica row.
	ErrUserNotFound = errors.New("user not in replica")
	ErrJobNotFound  = errors.New("job not in replica")
)
