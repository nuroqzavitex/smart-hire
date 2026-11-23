package app_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/otel/oteltest"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/app"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/domain"
)

func (e *env) queue() uuid.UUID {
	id := uuid.New()
	e.emails.m[id] = domain.Email{ID: id, NotificationID: uuid.New(), To: "a@x.local", Subject: "s", HTML: "<html></html>",
		Text: "t", Status: domain.EmailPending, NextAttemptAt: e.now}
	return id
}

func TestSendDue_Sends(t *testing.T) {
	m := oteltest.Install(t)
	e := newEnv(t)
	id := e.queue()
	n, err := e.svc.SendDue(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, domain.EmailSent, e.emails.m[id].Status)
	assert.Equal(t, []string{"a@x.local"}, e.mailer.sent)
	v, _ := m.Int64(t, "emails.sent")
	assert.Equal(t, int64(1), v)
}

func TestSendDue_BackoffThenFailed(t *testing.T) {
	m := oteltest.Install(t)
	e := newEnv(t)
	e.mailer.err = errBoom
	id := e.queue()
	want := []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second,
		80 * time.Second, 160 * time.Second, 320 * time.Second}
	for i, d := range want {
		_, err := e.svc.SendDue(context.Background())
		require.NoError(t, err)
		got := e.emails.m[id]
		assert.Equal(t, domain.EmailPending, got.Status, "attempt %d", i+1)
		assert.Equal(t, i+1, got.Attempts)
		assert.Equal(t, e.now.Add(d), got.NextAttemptAt, "attempt %d", i+1)
		assert.Equal(t, "smtp down", got.LastError)
		e.now = got.NextAttemptAt
	}
	_, err := e.svc.SendDue(context.Background())
	require.NoError(t, err)
	got := e.emails.m[id]
	assert.Equal(t, domain.EmailFailed, got.Status, "8th failure gives up")
	assert.Equal(t, 8, got.Attempts)
	v, _ := m.Int64(t, "emails.failed")
	assert.Equal(t, int64(1), v)
}

func TestEmailBackoff_Capped(t *testing.T) {
	assert.Equal(t, 5*time.Second, app.EmailBackoff(1))
	assert.Equal(t, 10*time.Minute, app.EmailBackoff(20))
}

// Final review: email counters exist at 0 before the first send.
func TestEmailCounters_ExistAtZero(t *testing.T) {
	m := oteltest.Install(t)
	newEnv(t)
	for _, name := range []string{"emails.sent", "emails.failed"} {
		v, ok := m.Int64(t, name)
		assert.True(t, ok, name)
		assert.Zero(t, v, name)
	}
}
