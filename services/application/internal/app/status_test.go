package app_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

// screened returns an application the saga already moved to SCREENED.
func (e *env) screened(t *testing.T) (uuid.UUID, domain.Application) {
	t.Helper()
	cand, a := e.submit(t)
	a.Status, a.Version = domain.StatusScreened, 3
	e.apps.m[a.ID] = a
	return cand, a
}

func TestUpdateStatus_RecruiterMoves(t *testing.T) {
	e := newEnv(t)
	_, a := e.screened(t)
	e.clock.Advance(time.Minute)
	events := len(e.ob.evs)

	got, err := e.svc.UpdateStatus(asRecruiter(e.recruiter), a.ID, domain.StatusInterview)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusInterview, got.Status)
	assert.Equal(t, int64(4), got.Version)
	assert.Equal(t, t0.Add(time.Minute), got.UpdatedAt)
	assert.Equal(t, got, e.apps.m[a.ID])

	h := e.apps.history[a.ID]
	last := h[len(h)-1]
	require.NotNil(t, last.From)
	assert.Equal(t, domain.StatusScreened, *last.From)
	assert.Equal(t, domain.StatusInterview, last.To)
	assert.Equal(t, e.recruiter.String(), last.Actor)

	require.Len(t, e.ob.evs, events+1)
	changed := e.eventsOf(t, "application.status_changed")
	ev := changed[len(changed)-1]
	assert.Equal(t, "SCREENED", ev["from"])
	assert.Equal(t, "INTERVIEW", ev["to"])
	assert.Equal(t, e.recruiter.String(), ev["actor"])
	assert.InDelta(t, 4, ev["version"], 0)

	_, err = e.svc.UpdateStatus(asAdmin(), a.ID, domain.StatusOffered)
	require.NoError(t, err, "admin may move too")
}

func TestUpdateStatus_InvalidTransition(t *testing.T) {
	e := newEnv(t)
	_, a := e.submit(t) // PENDING belongs to the saga
	_, err := e.svc.UpdateStatus(asRecruiter(e.recruiter), a.ID, domain.StatusInterview)
	assert.ErrorIs(t, err, apperr.ErrConflict)
	assert.Equal(t, domain.StatusPending, e.apps.m[a.ID].Status)
}

func TestUpdateStatus_OtherRecruiterNotFound(t *testing.T) {
	e := newEnv(t)
	_, a := e.screened(t)
	_, err := e.svc.UpdateStatus(asRecruiter(uuid.New()), a.ID, domain.StatusInterview)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	_, err = e.svc.UpdateStatus(asRecruiter(e.recruiter), uuid.New(), domain.StatusInterview)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
}

func TestUpdateStatus_CandidateForbidden(t *testing.T) {
	e := newEnv(t)
	cand, a := e.screened(t)
	_, err := e.svc.UpdateStatus(asCandidate(cand), a.ID, domain.StatusInterview)
	assert.ErrorIs(t, err, apperr.ErrForbidden)
}
