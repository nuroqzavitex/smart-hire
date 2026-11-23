package consumer_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/consumer"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
)

type stub struct {
	jobs, deleted int
	err           error
}

func (s *stub) HandleJobEvent(context.Context, []byte) error { s.jobs++; return s.err }
func (s *stub) HandleUserDeleted(context.Context, event.Envelope) error {
	s.deleted++
	return s.err
}

func TestHandler_RoutesByEventType(t *testing.T) {
	assert.Equal(t, []string{"job.closed", "job.deleted", "user.deleted"}, consumer.Topics)
	s := &stub{}
	h := consumer.NewHandler(s)
	assert.NoError(t, h(context.Background(), event.Envelope{EventType: "job.closed"}, &kgo.Record{}))
	assert.NoError(t, h(context.Background(), event.Envelope{EventType: "user.deleted"}, &kgo.Record{}))
	assert.Equal(t, 1, s.jobs)
	assert.Equal(t, 1, s.deleted)

	s.err = fmt.Errorf("%w: x", app.ErrPoisonEvent)
	assert.True(t, kafka.IsPermanent(h(context.Background(), event.Envelope{EventType: "user.deleted"}, &kgo.Record{})))
}
