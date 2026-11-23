package domain

import (
	"time"

	"github.com/google/uuid"
)

// SagaStep is the next step the submit saga runs.
type SagaStep string

// Saga steps (spec §6.4): reserve quota at Candidate, score with AI, and the
// compensation that gives the quota back.
const (
	StepReserveQuota SagaStep = "RESERVE_QUOTA"
	StepScore        SagaStep = "SCORE"
	StepReleaseQuota SagaStep = "RELEASE_QUOTA"
)

// SagaState is the saga's lifecycle state; RUNNING and COMPENSATING sagas
// are picked up by the worker.
type SagaState string

// Saga states.
const (
	SagaRunning      SagaState = "RUNNING"
	SagaCompensating SagaState = "COMPENSATING"
	SagaCompleted    SagaState = "COMPLETED"
	SagaCompensated  SagaState = "COMPENSATED"
)

// Saga is the persisted state of one application's submit saga. NextRunAt
// doubles as the worker's lease; Version guards every write so a worker
// whose lease expired cannot overwrite newer state.
type Saga struct {
	ID            uuid.UUID
	ApplicationID uuid.UUID
	JobID         uuid.UUID
	Step          SagaStep
	State         SagaState
	Attempts      int
	NextRunAt     time.Time
	DeadlineAt    time.Time
	StepStartedAt time.Time
	CancelReason  string
	LastError     string
	Version       int64
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Active reports whether the worker still has work for the saga.
func (s Saga) Active() bool { return s.State == SagaRunning || s.State == SagaCompensating }
