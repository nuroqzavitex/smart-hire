// Package domain holds the application aggregate, its state machine and the
// submit saga's state. It has no framework, database or proto imports.
package domain

import "fmt"

// Status is an application's state (spec §5.5).
type Status string

// Application statuses. The saga owns PENDING and SCREENING and the moves to
// SCREENED, NEEDS_MANUAL_REVIEW and CANCELLED; recruiters own the rest.
const (
	StatusPending           Status = "PENDING"
	StatusScreening         Status = "SCREENING"
	StatusScreened          Status = "SCREENED"
	StatusNeedsManualReview Status = "NEEDS_MANUAL_REVIEW"
	StatusInterview         Status = "INTERVIEW"
	StatusOffered           Status = "OFFERED"
	StatusHired             Status = "HIRED"
	StatusRejected          Status = "REJECTED"
	StatusCancelled         Status = "CANCELLED"
)

// AllStatuses lists every status in pipeline order.
var AllStatuses = []Status{
	StatusPending, StatusScreening, StatusScreened, StatusNeedsManualReview,
	StatusInterview, StatusOffered, StatusHired, StatusRejected, StatusCancelled,
}

// ParseStatus accepts the exact upper-case status names.
func ParseStatus(s string) (Status, error) {
	for _, st := range AllStatuses {
		if string(st) == s {
			return st, nil
		}
	}
	return "", fmt.Errorf("unknown application status %q", s)
}

var recruiterMoves = map[Status][]Status{
	StatusScreened:          {StatusInterview, StatusRejected},
	StatusNeedsManualReview: {StatusInterview, StatusRejected},
	StatusInterview:         {StatusOffered, StatusRejected},
	StatusOffered:           {StatusHired, StatusRejected},
}

// CanRecruiterMove reports whether a recruiter may move an application from
// one status to another.
func CanRecruiterMove(from, to Status) bool {
	for _, s := range recruiterMoves[from] {
		if s == to {
			return true
		}
	}
	return false
}
