package app_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/eventschema"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

// updatedEvents returns the cv.updated envelopes written after index from.
func (e *env) updatedEvents(from int) []event.Envelope {
	var out []event.Envelope
	for _, ev := range e.ob.evs[from:] {
		if ev.EventType == "cv.updated" {
			out = append(out, ev)
		}
	}
	return out
}

type updatedPayload struct {
	CVID        string          `json:"cv_id"`
	CandidateID string          `json:"candidate_id"`
	Version     int64           `json:"version"`
	Status      string          `json:"status"`
	Parsed      json.RawMessage `json:"parsed"`
	ParsedAt    *string         `json:"parsed_at"`
}

// onlyUpdated asserts exactly one schema-valid cv.updated keyed by the CV id
// and returns its payload.
func onlyUpdated(t *testing.T, evs []event.Envelope, cv domain.CV) updatedPayload {
	t.Helper()
	require.Len(t, evs, 1)
	raw, err := json.Marshal(evs[0])
	require.NoError(t, err)
	require.NoError(t, eventschema.Validate("cv.updated", raw))
	var p updatedPayload
	require.NoError(t, json.Unmarshal(evs[0].Payload, &p))
	assert.Equal(t, cv.ID.String(), p.CVID)
	assert.Equal(t, cv.CandidateID.String(), p.CandidateID)
	assert.Equal(t, cv.Version, p.Version)
	return p
}

func TestHandleParseResult_EmitsCvUpdated(t *testing.T) {
	e := newEnv(t)
	cv := e.uploaded(t, u1)
	n := e.ob.count()
	full := fixture(t, "valid/full.json")
	require.NoError(t, e.svc.HandleParseResult(context.Background(), parsedEvent(t, cv, cv.AttemptID, full)))

	got := e.cvs.m[cv.ID]
	p := onlyUpdated(t, e.updatedEvents(n), got)
	assert.Equal(t, "PARSED", p.Status)
	assert.JSONEq(t, string(got.ParsedData), string(p.Parsed))
	require.NotNil(t, p.ParsedAt)
	assert.Equal(t, cv.ID.String(), e.ob.lastKey())
}

func TestHandleParseResult_FailedEmitsCvUpdated(t *testing.T) {
	e := newEnv(t)
	cv := e.uploaded(t, u1)
	n := e.ob.count()
	require.NoError(t, e.svc.HandleParseResult(context.Background(), failedEvent(t, cv, cv.AttemptID, "no_text")))

	p := onlyUpdated(t, e.updatedEvents(n), e.cvs.m[cv.ID])
	assert.Equal(t, "FAILED", p.Status)
	assert.Equal(t, "null", string(p.Parsed))
	assert.Nil(t, p.ParsedAt)
}

func TestHandleParseResult_StaleEmitsNothing(t *testing.T) {
	e := newEnv(t)
	cv := e.parsed(t, u1)
	attemptA := cv.AttemptID
	_, err := e.svc.ReparseCV(asCandidate(u1), cv.ID)
	require.NoError(t, err)
	n := e.ob.count()

	require.NoError(t, e.svc.HandleParseResult(context.Background(), parsedEvent(t, cv, attemptA, fixture(t, "valid/minimal.json"))))
	assert.Equal(t, n, e.ob.count())
}

func TestUpdateParsedData_EmitsCvUpdated(t *testing.T) {
	e := newEnv(t)
	cv := e.parsed(t, u1)
	n := e.ob.count()
	edit := domain.ParsedCV{Skills: []string{"Go", "Rust"}}
	out, err := e.svc.UpdateParsedData(asCandidate(u1), cv.ID, edit)
	require.NoError(t, err)

	p := onlyUpdated(t, e.updatedEvents(n), out)
	assert.Equal(t, "PARSED", p.Status)
	assert.JSONEq(t, string(out.ParsedData), string(p.Parsed))
	assert.Equal(t, n+1, e.ob.count(), "only cv.updated")
}

func TestReparseCV_EmitsUploadedAndUpdated(t *testing.T) {
	e := newEnv(t)
	cv := e.parsed(t, u1)
	n := e.ob.count()
	out, err := e.svc.ReparseCV(asCandidate(u1), cv.ID)
	require.NoError(t, err)

	p := onlyUpdated(t, e.updatedEvents(n), out)
	assert.Equal(t, "UPLOADED", p.Status)
	assert.Equal(t, "null", string(p.Parsed))
	assert.Nil(t, p.ParsedAt)
	require.Equal(t, n+2, e.ob.count())
	assert.Equal(t, "cv.uploaded", e.ob.last().EventType)
}
