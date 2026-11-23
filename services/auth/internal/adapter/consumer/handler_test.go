package consumer_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/consumer"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/app"
)

type stub struct{ err error }

func (s stub) HandleDataPurged(context.Context, event.Envelope) error { return s.err }

func TestHandler_PoisonIsPermanent(t *testing.T) {
	h := consumer.NewHandler(stub{err: fmt.Errorf("%w: bad", app.ErrPoisonEvent)})
	assert.True(t, kafka.IsPermanent(h(context.Background(), event.Envelope{}, nil)))

	transient := errors.New("db down")
	err := consumer.NewHandler(stub{err: transient})(context.Background(), event.Envelope{}, nil)
	assert.ErrorIs(t, err, transient)
	assert.False(t, kafka.IsPermanent(err))
	assert.NoError(t, consumer.NewHandler(stub{})(context.Background(), event.Envelope{}, nil))
}
