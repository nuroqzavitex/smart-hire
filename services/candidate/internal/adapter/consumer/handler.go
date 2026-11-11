// Package consumer adapts the parse-result use case to a pkg/kafka handler.
package consumer

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
)

// Group is the consumer group of the CV parse-result consumer.
const Group = "candidate-cv-results"

// Topics are the topics the parse-result consumer subscribes to.
var Topics = []string{"cv.parsed", "cv.parse_failed"}

// ResultHandler is satisfied by *app.Service.
type ResultHandler interface {
	HandleParseResult(ctx context.Context, raw []byte) error
}

// NewHandler passes each record's raw value to uc. Poison events (wrapping
// app.ErrPoisonEvent) are marked permanent so they go straight to the DLQ;
// any other error is retried. The use case keeps payload values out of its
// errors, so they are safe for logs and the dlq-error header.
func NewHandler(uc ResultHandler) kafka.Handler {
	return func(ctx context.Context, _ event.Envelope, rec *kgo.Record) error {
		err := uc.HandleParseResult(ctx, rec.Value)
		if errors.Is(err, app.ErrPoisonEvent) {
			return kafka.Permanent(err)
		}
		return err
	}
}
