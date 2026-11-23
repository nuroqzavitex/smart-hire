// Package consumer adapts the deletion tracker to a pkg/kafka handler.
package consumer

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/app"
)

// Group is the consumer group of the deletion tracker (ADR 0014).
const Group = "auth-deletion-tracker"

// Topics are the topics the tracker subscribes to.
var Topics = []string{userdeletion.TopicPurged}

// PurgeHandler is satisfied by *app.Service.
type PurgeHandler interface {
	HandleDataPurged(ctx context.Context, env event.Envelope) error
}

// NewHandler passes each envelope to uc; poison events go straight to the DLQ.
func NewHandler(uc PurgeHandler) kafka.Handler {
	return func(ctx context.Context, env event.Envelope, _ *kgo.Record) error {
		err := uc.HandleDataPurged(ctx, env)
		if errors.Is(err, app.ErrPoisonEvent) {
			return kafka.Permanent(err)
		}
		return err
	}
}
