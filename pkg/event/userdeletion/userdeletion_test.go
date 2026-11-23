package userdeletion_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/eventschema"
	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
)

var at = time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)

func sample() userdeletion.Deleted {
	return userdeletion.Deleted{UserID: uuid.New(), Role: "candidate", RequestID: uuid.New(), RequestedAt: at}
}

func marshal(t *testing.T, env event.Envelope) []byte {
	t.Helper()
	b, err := json.Marshal(env)
	require.NoError(t, err)
	return b
}

func TestDeleted_RoundTripMatchesSchema(t *testing.T) {
	d := sample()
	env, err := userdeletion.NewDeletedEvent(context.Background(), d, at)
	require.NoError(t, err)
	assert.Equal(t, userdeletion.TopicDeleted, env.EventType)
	assert.Equal(t, "auth-service", env.Source)
	require.NoError(t, eventschema.Validate(userdeletion.TopicDeleted, marshal(t, env)))

	got, err := userdeletion.ParseDeleted(env)
	require.NoError(t, err)
	assert.Equal(t, d, got)
}

func TestPurged_RoundTripMatchesSchemaForEveryService(t *testing.T) {
	d := sample()
	for _, svc := range userdeletion.Services {
		env, err := userdeletion.NewPurgedEvent(context.Background(), d, svc, at)
		require.NoError(t, err, svc)
		assert.Equal(t, svc+"-service", env.Source)
		require.NoError(t, eventschema.Validate(userdeletion.TopicPurged, marshal(t, env)), svc)
		got, err := userdeletion.ParsePurged(env)
		require.NoError(t, err)
		assert.Equal(t, userdeletion.Purged{UserID: d.UserID, Service: svc, RequestID: d.RequestID, PurgedAt: at}, got)
	}
}

func TestNewPurgedEvent_UnknownService(t *testing.T) {
	_, err := userdeletion.NewPurgedEvent(context.Background(), sample(), "auth", at)
	assert.ErrorIs(t, err, userdeletion.ErrInvalid)
}

func TestParse_RejectsBadInput(t *testing.T) {
	good, err := userdeletion.NewDeletedEvent(context.Background(), sample(), at)
	require.NoError(t, err)
	cases := map[string]func(e *event.Envelope){
		"wrong type":    func(e *event.Envelope) { e.EventType = "user.registered" },
		"wrong version": func(e *event.Envelope) { e.EventVersion = 2 },
		"bad json":      func(e *event.Envelope) { e.Payload = json.RawMessage(`{`) },
		"bad user id": func(e *event.Envelope) {
			e.Payload = json.RawMessage(`{"user_id":"x","role":"candidate","deletion_request_id":"` + uuid.NewString() + `","requested_at":"2026-11-24T10:00:00Z"}`)
		},
		"bad request id": func(e *event.Envelope) {
			e.Payload = json.RawMessage(`{"user_id":"` + uuid.NewString() + `","role":"candidate","deletion_request_id":"","requested_at":"2026-11-24T10:00:00Z"}`)
		},
	}
	for name, mutate := range cases {
		e := good
		mutate(&e)
		_, err := userdeletion.ParseDeleted(e)
		assert.ErrorIs(t, err, userdeletion.ErrInvalid, name)
		assert.NotContains(t, err.Error(), `"x"`, name)
	}

	p, err := userdeletion.NewPurgedEvent(context.Background(), sample(), userdeletion.ServiceJob, at)
	require.NoError(t, err)
	p.Payload = json.RawMessage(`{"user_id":"` + uuid.NewString() + `","service":"auth","deletion_request_id":"` + uuid.NewString() + `","purged_at":"2026-11-24T10:00:00Z"}`)
	_, err = userdeletion.ParsePurged(p)
	assert.ErrorIs(t, err, userdeletion.ErrInvalid)
	_, err = userdeletion.ParsePurged(good)
	assert.ErrorIs(t, err, userdeletion.ErrInvalid, "a user.deleted is not a user.data_purged")
}
