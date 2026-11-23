// Package userdeletion holds the events of the account-deletion saga
// (choreography, spec §6.5, ADR 0014): Auth publishes user.deleted, every
// data-owning service purges the user and answers with user.data_purged.
// Errors never echo payload values.
package userdeletion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/event"
)

// Topics (= event types).
const (
	TopicDeleted = "user.deleted"
	TopicPurged  = "user.data_purged"
)

// Services that must confirm a purge.
const (
	ServiceCandidate    = "candidate"
	ServiceJob          = "job"
	ServiceApplication  = "application"
	ServiceAI           = "ai"
	ServiceNotification = "notification"
)

// Services lists every service Auth waits for, in a stable order.
var Services = []string{ServiceCandidate, ServiceJob, ServiceApplication, ServiceAI, ServiceNotification}

// DeletedUserID replaces a deleted user's ID where a row must keep a
// non-null owner (e.g. a recruiter's closed jobs).
var DeletedUserID = uuid.Nil

// ErrInvalid reports an event that can never be processed.
var ErrInvalid = errors.New("invalid user deletion event")

const authSource = "auth-service"

// Deleted is the payload of user.deleted.
type Deleted struct {
	UserID      uuid.UUID
	Role        string
	RequestID   uuid.UUID
	RequestedAt time.Time
}

// Purged is the payload of user.data_purged.
type Purged struct {
	UserID    uuid.UUID
	Service   string
	RequestID uuid.UUID
	PurgedAt  time.Time
}

type deletedJSON struct {
	UserID      string    `json:"user_id"`
	Role        string    `json:"role"`
	RequestID   string    `json:"deletion_request_id"`
	RequestedAt time.Time `json:"requested_at"`
}

type purgedJSON struct {
	UserID    string    `json:"user_id"`
	Service   string    `json:"service"`
	RequestID string    `json:"deletion_request_id"`
	PurgedAt  time.Time `json:"purged_at"`
}

// IsService reports whether s is one of Services.
func IsService(s string) bool { return slices.Contains(Services, s) }

// NewDeletedEvent builds a user.deleted envelope (source auth-service).
func NewDeletedEvent(ctx context.Context, d Deleted, now time.Time) (event.Envelope, error) {
	return event.New(ctx, authSource, TopicDeleted, 1, deletedJSON{
		UserID: d.UserID.String(), Role: d.Role, RequestID: d.RequestID.String(), RequestedAt: d.RequestedAt.UTC(),
	}, now)
}

// NewPurgedEvent builds the user.data_purged answer of service to d; the
// source is "<service>-service".
func NewPurgedEvent(ctx context.Context, d Deleted, service string, now time.Time) (event.Envelope, error) {
	if !IsService(service) {
		return event.Envelope{}, fmt.Errorf("%w: unknown service", ErrInvalid)
	}
	return event.New(ctx, service+"-service", TopicPurged, 1, purgedJSON{
		UserID: d.UserID.String(), Service: service, RequestID: d.RequestID.String(), PurgedAt: now.UTC(),
	}, now)
}

// ParseDeleted decodes a user.deleted envelope.
func ParseDeleted(env event.Envelope) (Deleted, error) {
	if env.EventType != TopicDeleted || env.EventVersion != 1 {
		return Deleted{}, fmt.Errorf("%w: unexpected event type or version", ErrInvalid)
	}
	var p deletedJSON
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return Deleted{}, fmt.Errorf("%w: malformed payload", ErrInvalid)
	}
	user, err1 := uuid.Parse(p.UserID)
	req, err2 := uuid.Parse(p.RequestID)
	if err1 != nil || err2 != nil {
		return Deleted{}, fmt.Errorf("%w: invalid id", ErrInvalid)
	}
	return Deleted{UserID: user, Role: p.Role, RequestID: req, RequestedAt: p.RequestedAt}, nil
}

// ParsePurged decodes a user.data_purged envelope.
func ParsePurged(env event.Envelope) (Purged, error) {
	if env.EventType != TopicPurged || env.EventVersion != 1 {
		return Purged{}, fmt.Errorf("%w: unexpected event type or version", ErrInvalid)
	}
	var p purgedJSON
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return Purged{}, fmt.Errorf("%w: malformed payload", ErrInvalid)
	}
	user, err1 := uuid.Parse(p.UserID)
	req, err2 := uuid.Parse(p.RequestID)
	if err1 != nil || err2 != nil {
		return Purged{}, fmt.Errorf("%w: invalid id", ErrInvalid)
	}
	if !IsService(p.Service) {
		return Purged{}, fmt.Errorf("%w: unknown service", ErrInvalid)
	}
	return Purged{UserID: user, Service: p.Service, RequestID: req, PurgedAt: p.PurgedAt}, nil
}
