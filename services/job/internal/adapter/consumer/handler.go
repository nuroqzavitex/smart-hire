// Package consumer adapts the account-purge use case to a pkg/kafka handler.
package consumer

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
)

// Group is the consumer group of the job server's user.deleted consumer.
const Group = "job-user-deleted"

// Topics are the topics it subscribes to.
var Topics = []string{userdeletion.TopicDeleted}

// PurgeHandler is satisfied by *app.Service.
type PurgeHandler interface {
	HandleUserDeleted(ctx context.Context, env event.Envelope) error
}

// NewHandler passes each envelope to uc; poison events go straight to the DLQ.
func NewHandler(uc PurgeHandler) kafka.Handler {
	return func(ctx context.Context, env event.Envelope, _ *kgo.Record) error {
		err := uc.HandleUserDeleted(ctx, env)
		if errors.Is(err, app.ErrPoisonEvent) {
			return kafka.Permanent(err)
		}
		return err
	}
}
