package app

import (
	"context"
	"io"
	"log/slog"
	"time"
)

// Worker drives the submit sagas: it runs ProcessDue until nothing is due,
// then waits for the poll interval or a Wake. Crash recovery needs nothing
// extra: a saga abandoned mid-step keeps a past next_run_at once its lease
// ends and is claimed again (ADR 0011).
type Worker struct {
	svc  *Service
	poll time.Duration
	wake chan struct{}
	log  *slog.Logger
}

// NewWorker returns a worker polling every poll; log may be nil.
func NewWorker(svc *Service, poll time.Duration, log *slog.Logger) *Worker {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Worker{svc: svc, poll: poll, wake: make(chan struct{}, 1), log: log}
}

// Wake asks the worker to look for due sagas now; it never blocks.
func (w *Worker) Wake() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Run loops until ctx is cancelled; it returns nil on shutdown.
func (w *Worker) Run(ctx context.Context) error {
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		for ctx.Err() == nil {
			n, err := w.svc.ProcessDue(ctx)
			if err != nil {
				if ctx.Err() == nil {
					w.log.Error("saga claim failed", "err", err)
				}
				break
			}
			if n == 0 {
				break
			}
		}
		t.Reset(w.poll)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		case <-w.wake:
		}
	}
}
