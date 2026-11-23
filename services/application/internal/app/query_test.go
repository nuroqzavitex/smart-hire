package app_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

func TestGet_Visibility(t *testing.T) {
	e := newEnv(t)
	cand, a := e.submit(t)

	got, hist, err := e.svc.Get(asCandidate(cand), a.ID)
	require.NoError(t, err)
	assert.Equal(t, a.ID, got.ID)
	assert.Len(t, hist, 1)
	_, _, err = e.svc.Get(asRecruiter(e.recruiter), a.ID)
	require.NoError(t, err)
	_, _, err = e.svc.Get(asAdmin(), a.ID)
	require.NoError(t, err)

	_, _, err = e.svc.Get(asCandidate(uuid.New()), a.ID)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	_, _, err = e.svc.Get(asRecruiter(uuid.New()), a.ID)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	_, _, err = e.svc.Get(asCandidate(cand), uuid.New())
	assert.ErrorIs(t, err, apperr.ErrNotFound)
}

func TestList_ScopesByRole(t *testing.T) {
	e := newEnv(t)
	cand, mine := e.submit(t)
	e.clock.Advance(time.Second)
	_, other := e.submit(t)
	otherJob := uuid.New()
	otherRecruiter := uuid.New()
	e.jobs.m[otherJob] = app.JobInfo{ID: otherJob, RecruiterID: otherRecruiter, Open: true}
	e.clock.Advance(time.Second)
	second, err := e.svc.Submit(asCandidate(cand), app.SubmitInput{JobID: otherJob, CVID: e.cv, IdempotencyKey: "k-other-job"})
	require.NoError(t, err)

	got, _, err := e.svc.List(asCandidate(cand), app.ListInput{})
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{second.ID, mine.ID}, ids(got), "candidate sees own, newest first")

	got, _, err = e.svc.List(asCandidate(cand), app.ListInput{JobID: &e.job})
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{mine.ID}, ids(got))

	got, _, err = e.svc.List(asRecruiter(e.recruiter), app.ListInput{})
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{other.ID, mine.ID}, ids(got), "recruiter sees applications on own jobs")

	got, _, err = e.svc.List(asAdmin(), app.ListInput{})
	require.NoError(t, err)
	assert.Len(t, got, 3)

	st := domain.StatusScreened
	got, _, err = e.svc.List(asAdmin(), app.ListInput{Status: &st})
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestList_Pages(t *testing.T) {
	e := newEnv(t)
	for range 3 {
		e.submit(t)
		e.clock.Advance(time.Second)
	}
	page1, tok, err := e.svc.List(asRecruiter(e.recruiter), app.ListInput{PageSize: 2})
	require.NoError(t, err)
	require.Len(t, page1, 2)
	require.NotEmpty(t, tok)
	page2, tok2, err := e.svc.List(asRecruiter(e.recruiter), app.ListInput{PageSize: 2, PageToken: tok})
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.Empty(t, tok2)
	assert.NotContains(t, ids(page1), page2[0].ID)

	_, _, err = e.svc.List(asRecruiter(e.recruiter), app.ListInput{PageToken: "garbage"})
	assert.ErrorIs(t, err, apperr.ErrValidation)
	_, _, err = e.svc.List(asRecruiter(e.recruiter), app.ListInput{PageSize: 101})
	assert.ErrorIs(t, err, apperr.ErrValidation)
}

func TestList_RequiresLogin(t *testing.T) {
	e := newEnv(t)
	_, _, err := e.svc.List(t.Context(), app.ListInput{})
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
}

func ids(as []domain.Application) []uuid.UUID {
	out := make([]uuid.UUID, len(as))
	for i, a := range as {
		out[i] = a.ID
	}
	return out
}
