package consumer_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/consumer"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
)

type stub struct{ err error }

func (s stub) HandleUserDeleted(context.Context, event.Envelope) error { return s.err }

func TestHandler(t *testing.T) {
	assert.Equal(t, []string{"user.deleted"}, consumer.Topics)
	assert.NoError(t, consumer.NewHandler(stub{})(context.Background(), event.Envelope{}, nil))
	err := consumer.NewHandler(stub{err: fmt.Errorf("%w: x", app.ErrPoisonEvent)})(context.Background(), event.Envelope{}, nil)
	assert.True(t, kafka.IsPermanent(err))
	cause := errors.New("db down")
	err = consumer.NewHandler(stub{err: cause})(context.Background(), event.Envelope{}, nil)
	assert.ErrorIs(t, err, cause)
	assert.False(t, kafka.IsPermanent(err))
}
