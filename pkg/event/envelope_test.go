package event_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/logger"
)

func TestEnvelope_New(t *testing.T) {
	now := time.Date(2026, 11, 8, 16, 0, 0, 0, time.FixedZone("ICT", 7*3600))
	ctx := logger.WithRequestID(context.Background(), "req-9")
	e, err := event.New(ctx, "auth-service", "user.registered", 1, map[string]string{"user_id": "u"}, now)
	require.NoError(t, err)
	b, _ := json.Marshal(e)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	assert.Len(t, m["event_id"], 26)
	assert.Equal(t, "user.registered", m["event_type"])
	assert.EqualValues(t, 1, m["event_version"])
	assert.Equal(t, "auth-service", m["source"])
	assert.Equal(t, "2026-11-08T09:00:00Z", m["occurred_at"])
	assert.Equal(t, "req-9", m["correlation_id"])
	assert.Equal(t, map[string]any{"user_id": "u"}, m["payload"])
}

func TestEnvelope_New_BadPayload(t *testing.T) {
	_, err := event.New(context.Background(), "s", "t", 1, make(chan int), time.Now())
	require.Error(t, err)
}
