package app

import (
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// Deps are the collaborators of Service.
type Deps struct {
	Jobs   JobRepo
	Outbox OutboxWriter
	Tx     TxManager
	Now    func() time.Time
	NewID  func() uuid.UUID
	Logger *slog.Logger
}

// Service implements the job use cases.
type Service struct{ Deps }

// NewService wires a Service, defaulting Now, NewID and Logger.
func NewService(d Deps) *Service {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.NewID == nil {
		d.NewID = uuid.New
	}
	if d.Logger == nil {
		d.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Service{Deps: d}
}
