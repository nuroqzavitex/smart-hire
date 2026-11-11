package consumer_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/consumer"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
)

type fakeUC struct {
	raw   []byte
	calls int
	err   error
}

func (f *fakeUC) HandleParseResult(_ context.Context, raw []byte) error {
	f.calls++
	f.raw = raw
	return f.err
}

func call(t *testing.T, uc *fakeUC) error {
	t.Helper()
	h := consumer.NewHandler(uc)
	rec := &kgo.Record{Topic: "cv.parsed", Value: []byte(`{"event_id":"x"}`)}
	err := h(context.Background(), event.Envelope{EventID: "x"}, rec)
	require.Equal(t, 1, uc.calls)
	assert.Equal(t, rec.Value, uc.raw, "the raw record value is passed through")
	return err
}

func TestHandler_Success(t *testing.T) {
	require.NoError(t, call(t, &fakeUC{}))
}

func TestHandler_PoisonIsPermanent(t *testing.T) {
	err := call(t, &fakeUC{err: fmt.Errorf("%w: x", app.ErrPoisonEvent)})
	require.Error(t, err)
	assert.True(t, kafka.IsPermanent(err))
	assert.ErrorIs(t, err, app.ErrPoisonEvent)
}

func TestHandler_OtherErrorsRetry(t *testing.T) {
	cause := errors.New("db down")
	err := call(t, &fakeUC{err: cause})
	require.Error(t, err)
	assert.False(t, kafka.IsPermanent(err))
	assert.ErrorIs(t, err, cause)
}

func TestTopicsAndGroup(t *testing.T) {
	assert.Equal(t, "candidate-cv-results", consumer.Group)
	assert.Equal(t, []string{"cv.parsed", "cv.parse_failed"}, consumer.Topics)
}
