package app

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
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

func TestPayloadOf_PublishedAt(t *testing.T) {
	base := domain.Job{
		ID: uuid.New(), RecruiterID: uuid.New(), Title: "Go developer",
		Description: "Build distributed services in Go.", Level: domain.LevelSenior,
		Location: "Hà Nội", Status: domain.StatusDraft, Version: 1,
	}

	draft, err := json.Marshal(payloadOf(base))
	require.NoError(t, err)
	assert.Contains(t, string(draft), `"published_at":null`)

	open := base
	at := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	open.Status, open.PublishedAt, open.Version = domain.StatusOpen, &at, 2
	b, err := json.Marshal(payloadOf(open))
	require.NoError(t, err)
	assert.Contains(t, string(b), `"published_at":"2026-11-20T10:00:00Z"`)

	for _, typ := range []string{evCreated, evUpdated} {
		env, err := event.New(context.Background(), source, typ, 1, payloadOf(open), at)
		require.NoError(t, err)
		doc, err := json.Marshal(env)
		require.NoError(t, err)
		assert.NoError(t, eventschema.Validate(typ, doc), typ)
	}
}
