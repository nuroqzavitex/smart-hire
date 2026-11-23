package domain

import (
	"errors"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
)

// DeletionStatus is the state of an account-deletion request (ADR 0014).
type DeletionStatus string

// Deletion request states. STUCK only flags a request for the alert; it can
// still complete when the missing confirmations arrive.
const (
	DeletionPending   DeletionStatus = "PENDING"
	DeletionCompleted DeletionStatus = "COMPLETED"
	DeletionStuck     DeletionStatus = "STUCK"
)

// DeletionRequest tracks which services confirmed that they purged a user.
// Auth does not command the services; it only collects user.data_purged.
type DeletionRequest struct {
	ID              uuid.UUID
	UserID          uuid.UUID
	Role            authz.Role
	Status          DeletionStatus
	Expected        []string
	Purged          []string
	RequestedAt     time.Time
	LastPublishedAt time.Time
	CompletedAt     *time.Time
}

// Errors of the deletion flow.
var (
	ErrUnknownService      = errors.New("service is not expected to confirm this deletion")
	ErrInvalidPassword     = apperr.New(apperr.ErrUnauthenticated, "invalid password")
	ErrAdminDeletion       = apperr.New(apperr.ErrForbidden, "admin accounts cannot be deleted")
	ErrDeletionNotFound    = apperr.New(apperr.ErrNotFound, "deletion request not found")
	ErrDeletionAdminOnly   = apperr.New(apperr.ErrForbidden, "admin role required")
	ErrDeletionRequiresJWT = apperr.New(apperr.ErrUnauthenticated, "authentication required")
)

// NewDeletionRequest starts a PENDING request that waits for every expected
// service; it counts as published at now.
func NewDeletionRequest(userID uuid.UUID, role authz.Role, expected []string, now time.Time) DeletionRequest {
	return DeletionRequest{
		ID: uuid.New(), UserID: userID, Role: role, Status: DeletionPending,
		Expected: slices.Clone(expected), Purged: []string{},
		RequestedAt: now, LastPublishedAt: now,
	}
}

// MarkPurged records service's confirmation (as a set) and reports whether it
// completed the request. A completed request ignores late confirmations.
func (r *DeletionRequest) MarkPurged(service string, now time.Time) (bool, error) {
	if !slices.Contains(r.Expected, service) {
		return false, ErrUnknownService
	}
	if r.Status == DeletionCompleted {
		return false, nil
	}
	if !slices.Contains(r.Purged, service) {
		r.Purged = append(r.Purged, service)
	}
	for _, s := range r.Expected {
		if !slices.Contains(r.Purged, s) {
			return false, nil
		}
	}
	r.Status = DeletionCompleted
	r.CompletedAt = &now
	return true, nil
}
