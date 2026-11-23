// Package consumer adapts the job-event and account-purge use cases to a
// pkg/kafka handler.
package consumer

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
)

// Group is the consumer group of the job-event consumer.
const Group = "application"

// Topics are the job events that cancel running sagas (spec §6.4) and
// user.deleted (ADR 0014).
var Topics = []string{"job.closed", "job.deleted", userdeletion.TopicDeleted}

// JobEventHandler is satisfied by *app.Service.
type JobEventHandler interface {
	HandleJobEvent(ctx context.Context, raw []byte) error
	HandleUserDeleted(ctx context.Context, env event.Envelope) error
}

// NewHandler passes each record's raw value to uc; poison events go
// straight to the DLQ, other errors are retried.
func NewHandler(uc JobEventHandler) kafka.Handler {
	return func(ctx context.Context, env event.Envelope, rec *kgo.Record) error {
		var err error
		if env.EventType == userdeletion.TopicDeleted {
			err = uc.HandleUserDeleted(ctx, env)
		} else {
			err = uc.HandleJobEvent(ctx, rec.Value)
		}
		if errors.Is(err, app.ErrPoisonEvent) {
			return kafka.Permanent(err)
		}
		return err
	}
}
