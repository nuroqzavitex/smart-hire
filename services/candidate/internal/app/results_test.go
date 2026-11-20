package app_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/eventschema"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

// fixture reads a shared ParsedCV fixture from contracts/events/testdata.
func fixture(t *testing.T, name string) json.RawMessage {
	t.Helper()
	dir, err := eventschema.Dir()
	require.NoError(t, err)
	raw, err := os.ReadFile(filepath.Join(dir, "testdata", "parsed_cv", name)) //nolint:gosec // test fixture
	require.NoError(t, err)
	return raw
}

// resultEnvelope builds a serialized ai-service result event.
func resultEnvelope(t *testing.T, eventType string, payload map[string]any) []byte {
	t.Helper()
	env, err := event.New(context.Background(), "ai-service", eventType, 1, payload, time.Now())
	require.NoError(t, err)
	raw, err := json.Marshal(env)
	require.NoError(t, err)
	return raw
}

func parsedEvent(t *testing.T, cv domain.CV, attemptID string, parsed json.RawMessage) []byte {
	t.Helper()
	return resultEnvelope(t, "cv.parsed", map[string]any{
		"cv_id": cv.ID.String(), "candidate_id": cv.CandidateID.String(), "attempt_id": attemptID,
		"parsed": parsed, "model_version": "fake:rules-v1",
	})
}

func failedEvent(t *testing.T, cv domain.CV, attemptID, reason string) []byte {
	t.Helper()
	return resultEnvelope(t, "cv.parse_failed", map[string]any{
		"cv_id": cv.ID.String(), "candidate_id": cv.CandidateID.String(), "attempt_id": attemptID,
		"reason": reason, "detail": "",
	})
}

func eventID(t *testing.T, raw []byte) string {
	t.Helper()
	var env event.Envelope
	require.NoError(t, json.Unmarshal(raw, &env))
	return env.EventID
}

// parsed drives a CV of owner to PARSED with the full.json fixture.
func (e *env) parsed(t *testing.T, owner uuid.UUID) domain.CV {
	t.Helper()
	cv := e.uploaded(t, owner)
	require.NoError(t, e.svc.HandleParseResult(context.Background(), parsedEvent(t, cv, cv.AttemptID, fixture(t, "valid/full.json"))))
	out := e.cvs.m[cv.ID]
	require.Equal(t, domain.StatusParsed, out.Status)
	return out
}

func TestHandleParseResult_AppliesParsed(t *testing.T) {
	e := newEnv(t)
	cv := e.uploaded(t, u1)
	full := fixture(t, "valid/full.json")
	raw := parsedEvent(t, cv, cv.AttemptID, full)

	require.NoError(t, e.svc.HandleParseResult(context.Background(), raw))
	got := e.cvs.m[cv.ID]
	assert.Equal(t, domain.StatusParsed, got.Status)
	assert.JSONEq(t, string(full), string(got.ParsedData))
	assert.Nil(t, got.ParseError)
	assert.NotNil(t, got.ParsedAt)
	assert.Equal(t, cv.Version+1, got.Version)
	assert.True(t, e.idem.seen[eventID(t, raw)])
	assert.Equal(t, 2, e.ob.count(), "cv.uploaded, then cv.updated for the applied result")
}

func TestHandleParseResult_AppliesFailed(t *testing.T) {
	e := newEnv(t)
	cv := e.uploaded(t, u1)
	require.NoError(t, e.svc.HandleParseResult(context.Background(), failedEvent(t, cv, cv.AttemptID, "no_text")))
	got := e.cvs.m[cv.ID]
	assert.Equal(t, domain.StatusFailed, got.Status)
	require.NotNil(t, got.ParseError)
	assert.Equal(t, "no_text", *got.ParseError)
	assert.Nil(t, got.ParsedData)
	assert.Equal(t, cv.Version+1, got.Version)
}

func TestHandleParseResult_DuplicateIgnored(t *testing.T) {
	e := newEnv(t)
	cv := e.uploaded(t, u1)
	raw := parsedEvent(t, cv, cv.AttemptID, fixture(t, "valid/full.json"))
	require.NoError(t, e.svc.HandleParseResult(context.Background(), raw))
	require.NoError(t, e.svc.HandleParseResult(context.Background(), raw))
	assert.Equal(t, cv.Version+1, e.cvs.m[cv.ID].Version)
}

func TestHandleParseResult_StaleAttemptIgnored(t *testing.T) {
	e := newEnv(t)
	cv := e.parsed(t, u1) // attempt A applied
	attemptA := cv.AttemptID

	re, err := e.svc.ReparseCV(asCandidate(u1), cv.ID) // attempt B
	require.NoError(t, err)
	require.NotEqual(t, attemptA, re.AttemptID)

	stale := parsedEvent(t, cv, attemptA, fixture(t, "valid/minimal.json"))
	require.NoError(t, e.svc.HandleParseResult(context.Background(), stale))
	got := e.cvs.m[cv.ID]
	assert.Equal(t, domain.StatusUploaded, got.Status)
	assert.Equal(t, re.AttemptID, got.AttemptID)
	assert.Nil(t, got.ParsedData)
	assert.Equal(t, re.Version, got.Version)
	assert.True(t, e.idem.seen[eventID(t, stale)], "stale event is still recorded as processed")
	assert.Contains(t, e.logs.String(), "stale cv parse result ignored")
	assert.Contains(t, e.logs.String(), attemptA)

	// A stale failure for a CV that is no longer UPLOADED is ignored too.
	cur := parsedEvent(t, got, got.AttemptID, fixture(t, "valid/minimal.json"))
	require.NoError(t, e.svc.HandleParseResult(context.Background(), cur))
	require.NoError(t, e.svc.HandleParseResult(context.Background(), failedEvent(t, got, got.AttemptID, "no_text")))
	assert.Equal(t, domain.StatusParsed, e.cvs.m[cv.ID].Status)
}

func TestHandleParseResult_Poison(t *testing.T) {
	e := newEnv(t)
	cv := e.uploaded(t, u1)
	full := fixture(t, "valid/full.json")
	foreign := cv
	foreign.CandidateID = u2
	missing := cv
	missing.ID = uuid.New()

	noAttempt := resultEnvelope(t, "cv.parsed", map[string]any{
		"cv_id": cv.ID.String(), "candidate_id": cv.CandidateID.String(),
		"parsed": full, "model_version": "fake:rules-v1",
	})
	jobEvent, err := event.New(context.Background(), "job-service", "job.created", 1, map[string]any{"job_id": "x"}, time.Now())
	require.NoError(t, err)
	jobRaw, err := json.Marshal(jobEvent)
	require.NoError(t, err)

	cases := map[string][]byte{
		"not json":          []byte("not json"),
		"schema mismatch":   noAttempt,
		"unexpected type":   jobRaw,
		"cv does not exist": parsedEvent(t, missing, cv.AttemptID, full),
		"foreign candidate": parsedEvent(t, foreign, cv.AttemptID, full),
	}
	before := e.cvs.m[cv.ID]
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			err := e.svc.HandleParseResult(context.Background(), raw)
			require.ErrorIs(t, err, app.ErrPoisonEvent)
			assert.NotContains(t, err.Error(), "Nguyễn", "no payload values in poison errors")
			assert.Equal(t, before, e.cvs.m[cv.ID])
			assert.Empty(t, e.idem.seen, "poison events are not recorded")
		})
	}
	assert.Equal(t, 1, e.ob.count())
}

// TestHandleParseResult_SchemaErrorHidesValues checks E-R6: the schema error
// (which echoes offending values) never reaches the returned error.
func TestHandleParseResult_SchemaErrorHidesValues(t *testing.T) {
	e := newEnv(t)
	cv := e.uploaded(t, u1)
	bad := resultEnvelope(t, "cv.parse_failed", map[string]any{
		"cv_id": cv.ID.String(), "candidate_id": cv.CandidateID.String(), "attempt_id": cv.AttemptID,
		"reason": "secret-value-xyz", "detail": "",
	})
	err := e.svc.HandleParseResult(context.Background(), bad)
	require.ErrorIs(t, err, app.ErrPoisonEvent)
	assert.NotContains(t, err.Error(), "secret-value-xyz")
	assert.NotContains(t, e.logs.String(), "secret-value-xyz")
}
