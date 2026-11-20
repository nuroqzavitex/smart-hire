package app_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

type fakeMatcher struct {
	calls []uuid.UUID
	topK  int
	out   app.MatchList
	err   error
}

func (f *fakeMatcher) FindCandidates(_ context.Context, jobID uuid.UUID, topK int) (app.MatchList, error) {
	f.calls = append(f.calls, jobID)
	f.topK = topK
	return f.out, f.err
}

func matchEnv(t *testing.T) (env, *fakeMatcher) {
	t.Helper()
	m := &fakeMatcher{out: app.MatchList{ModelVersion: "fake-hash@768", Candidates: []app.MatchedCandidate{
		{CandidateID: uuid.New(), CVID: uuid.New(), Score: 87.5, Breakdown: app.MatchBreakdown{MatchedSkills: []string{"Go"}}},
	}}}
	return newEnv(t, func(d *app.Deps, _ *fakeJobs) { d.Matcher = m }), m
}

func asAdmin() context.Context {
	return authz.WithClaims(context.Background(), authz.Claims{Subject: uuid.NewString(), Role: authz.RoleAdmin})
}

func TestListMatchedCandidates_Owner(t *testing.T) {
	e, m := matchEnv(t)
	j := e.create(t, r1)
	got, err := e.svc.ListMatchedCandidates(asRecruiter(r1), j.ID, 5)
	require.NoError(t, err)
	assert.Equal(t, m.out, got)
	assert.Equal(t, []uuid.UUID{j.ID}, m.calls)
	assert.Equal(t, 5, m.topK)
}

func TestListMatchedCandidates_OtherRecruiterNotFound(t *testing.T) {
	e, m := matchEnv(t)
	j := e.create(t, r1)
	_, err := e.svc.ListMatchedCandidates(asRecruiter(r2), j.ID, 0)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	assert.Empty(t, m.calls, "AI is not asked about a job the caller cannot see")
}

func TestListMatchedCandidates_CandidateForbidden(t *testing.T) {
	e, m := matchEnv(t)
	j := e.create(t, r1)
	_, err := e.svc.ListMatchedCandidates(asCandidate(), j.ID, 0)
	assert.ErrorIs(t, err, apperr.ErrForbidden)
	_, err = e.svc.ListMatchedCandidates(context.Background(), j.ID, 0)
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
	assert.Empty(t, m.calls)
}

func TestListMatchedCandidates_AdminAllowed(t *testing.T) {
	e, _ := matchEnv(t)
	j := e.create(t, r1)
	_, err := e.svc.ListMatchedCandidates(asAdmin(), j.ID, 0)
	require.NoError(t, err)
}

func TestListMatchedCandidates_DeletedOrMissingNotFound(t *testing.T) {
	e, _ := matchEnv(t)
	_, err := e.svc.ListMatchedCandidates(asRecruiter(r1), uuid.New(), 0)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	j := e.create(t, r1)
	require.NoError(t, e.svc.Delete(asRecruiter(r1), j.ID))
	_, err = e.svc.ListMatchedCandidates(asRecruiter(r1), j.ID, 0)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
}

func TestListMatchedCandidates_AIUnavailable(t *testing.T) {
	e, m := matchEnv(t)
	m.err = apperr.New(apperr.ErrUnavailable, "matching temporarily unavailable")
	j := e.create(t, r1)
	_, err := e.svc.ListMatchedCandidates(asRecruiter(r1), j.ID, 0)
	assert.ErrorIs(t, err, apperr.ErrUnavailable)

	noAI := newEnv(t)
	j = noAI.create(t, r1)
	_, err = noAI.svc.ListMatchedCandidates(asRecruiter(r1), j.ID, 0)
	assert.ErrorIs(t, err, apperr.ErrUnavailable, "no AI configured")
}

func TestBatchGetJobs_OnlyOpenInOrder(t *testing.T) {
	e := newEnv(t)
	a, b := e.create(t, r1), e.create(t, r1)
	draft := e.create(t, r2)
	for _, j := range []domain.Job{a, b} {
		_, err := e.svc.Publish(asRecruiter(r1), j.ID)
		require.NoError(t, err)
	}
	got, err := e.svc.BatchGetJobs(asCandidate(), []uuid.UUID{b.ID, uuid.New(), draft.ID, a.ID, b.ID})
	require.NoError(t, err)
	ids := make([]uuid.UUID, len(got))
	for i, j := range got {
		ids[i] = j.ID
	}
	assert.Equal(t, []uuid.UUID{b.ID, a.ID}, ids)
}

func TestBatchGetJobs_TooManyIDs(t *testing.T) {
	e := newEnv(t)
	ids := make([]uuid.UUID, 51)
	for i := range ids {
		ids[i] = uuid.New()
	}
	_, err := e.svc.BatchGetJobs(asCandidate(), ids)
	assert.ErrorIs(t, err, apperr.ErrValidation)
	got, err := e.svc.BatchGetJobs(asCandidate(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestBatchGetJobs_RequiresLogin(t *testing.T) {
	e := newEnv(t)
	_, err := e.svc.BatchGetJobs(context.Background(), []uuid.UUID{uuid.New()})
	assert.True(t, errors.Is(err, apperr.ErrUnauthenticated))
}
