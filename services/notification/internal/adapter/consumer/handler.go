// Package consumer adapts the event use case to a pkg/kafka handler.
package consumer

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/app"
)

// Group is the consumer group of the notification service.
const Group = "notification"

// Topics are the events that update replicas or notify someone (ADR 0012).
var Topics = []string{
	"user.registered", "job.created", "job.updated", "job.deleted", "cv.parsed", "cv.parse_failed",
	"application.submitted", "application.status_changed", "match.completed",
}

// EventHandler is satisfied by *app.Service.
type EventHandler interface {
	HandleEvent(ctx context.Context, raw []byte) error
}

// NewHandler passes each record's raw value to uc; poison events go
// straight to the DLQ, other errors are retried.
func NewHandler(uc EventHandler) kafka.Handler {
	return func(ctx context.Context, _ event.Envelope, rec *kgo.Record) error {
		err := uc.HandleEvent(ctx, rec.Value)
		if errors.Is(err, app.ErrPoisonEvent) {
			return kafka.Permanent(err)
		}
		return err
	}
}
