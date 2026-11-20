package app_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
)

type fakeRecommender struct {
	out   app.Recommendations
	err   error
	calls []uuid.UUID
	topK  int
}

func (f *fakeRecommender) RecommendJobs(_ context.Context, cvID uuid.UUID, topK int) (app.Recommendations, error) {
	f.calls = append(f.calls, cvID)
	f.topK = topK
	return f.out, f.err
}

type fakeDirectory struct {
	jobs  map[uuid.UUID]app.JobSummary
	asked [][]uuid.UUID
	err   error
}

func (f *fakeDirectory) OpenJobs(_ context.Context, ids []uuid.UUID) ([]app.JobSummary, error) {
	f.asked = append(f.asked, ids)
	var out []app.JobSummary
	for _, id := range ids {
		if j, ok := f.jobs[id]; ok {
			out = append(out, j)
		}
	}
	return out, f.err
}

func recommendEnv(t *testing.T) (*env, *fakeRecommender, *fakeDirectory) {
	t.Helper()
	e := newEnv(t)
	rec := &fakeRecommender{}
	dir := &fakeDirectory{jobs: map[uuid.UUID]app.JobSummary{}}
	e.svc.Recommender = rec
	e.svc.JobDirectory = dir
	return e, rec, dir
}

func TestListRecommendedJobs_MergesInAIOrder(t *testing.T) {
	e, rec, dir := recommendEnv(t)
	cv := e.parsed(t, u1)
	a, b := uuid.New(), uuid.New()
	salaryMax := 3000
	dir.jobs[a] = app.JobSummary{ID: a, Title: "A", Level: "senior", Location: "Hà Nội", SalaryMax: &salaryMax}
	dir.jobs[b] = app.JobSummary{ID: b, Title: "B", Level: "junior"}
	rec.out = app.Recommendations{ModelVersion: "m@768", Jobs: []app.RankedJob{
		{JobID: b, Score: 90, Breakdown: app.MatchBreakdown{MatchedSkills: []string{"Go"}}},
		{JobID: a, Score: 70},
	}}
	got, err := e.svc.ListRecommendedJobs(asCandidate(u1), cv.ID, 5)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{cv.ID}, rec.calls)
	assert.Equal(t, 5, rec.topK)
	assert.Equal(t, [][]uuid.UUID{{b, a}}, dir.asked)
	assert.Equal(t, "m@768", got.ModelVersion)
	require.Len(t, got.Jobs, 2)
	assert.Equal(t, "B", got.Jobs[0].Job.Title)
	assert.InDelta(t, 90.0, got.Jobs[0].Score, 1e-9)
	assert.Equal(t, []string{"Go"}, got.Jobs[0].Breakdown.MatchedSkills)
	assert.Equal(t, &salaryMax, got.Jobs[1].Job.SalaryMax)
}

func TestListRecommendedJobs_DropsClosedJobs(t *testing.T) {
	e, rec, dir := recommendEnv(t)
	cv := e.parsed(t, u1)
	open, closed := uuid.New(), uuid.New()
	dir.jobs[open] = app.JobSummary{ID: open, Title: "Open"}
	rec.out = app.Recommendations{Jobs: []app.RankedJob{{JobID: closed, Score: 99}, {JobID: open, Score: 50}}}
	got, err := e.svc.ListRecommendedJobs(asCandidate(u1), cv.ID, 0)
	require.NoError(t, err)
	require.Len(t, got.Jobs, 1)
	assert.Equal(t, open, got.Jobs[0].Job.ID)
}

func TestListRecommendedJobs_NoResultsSkipsJobLookup(t *testing.T) {
	e, _, dir := recommendEnv(t)
	cv := e.parsed(t, u1)
	got, err := e.svc.ListRecommendedJobs(asCandidate(u1), cv.ID, 0)
	require.NoError(t, err)
	assert.Empty(t, got.Jobs)
	assert.Empty(t, dir.asked)
}

func TestListRecommendedJobs_NotOwner(t *testing.T) {
	e, rec, _ := recommendEnv(t)
	cv := e.parsed(t, u1)
	_, err := e.svc.ListRecommendedJobs(asCandidate(u2), cv.ID, 0)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	_, err = e.svc.ListRecommendedJobs(asCandidate(u1), uuid.New(), 0)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	assert.Empty(t, rec.calls, "AI is never asked about another candidate's CV")
}

func TestListRecommendedJobs_NotParsed(t *testing.T) {
	e, rec, _ := recommendEnv(t)
	cv := e.uploaded(t, u1)
	_, err := e.svc.ListRecommendedJobs(asCandidate(u1), cv.ID, 0)
	assert.ErrorIs(t, err, apperr.ErrValidation)
	assert.Empty(t, rec.calls)
}

func TestListRecommendedJobs_AIUnavailable(t *testing.T) {
	e, rec, _ := recommendEnv(t)
	cv := e.parsed(t, u1)
	rec.err = apperr.New(apperr.ErrUnavailable, "matching temporarily unavailable")
	_, err := e.svc.ListRecommendedJobs(asCandidate(u1), cv.ID, 0)
	assert.ErrorIs(t, err, apperr.ErrUnavailable)

	bare := newEnv(t)
	cv = bare.parsed(t, u1)
	_, err = bare.svc.ListRecommendedJobs(asCandidate(u1), cv.ID, 0)
	assert.ErrorIs(t, err, apperr.ErrUnavailable, "nothing wired")
}

func TestListRecommendedJobs_JobServiceError(t *testing.T) {
	e, rec, dir := recommendEnv(t)
	cv := e.parsed(t, u1)
	rec.out = app.Recommendations{Jobs: []app.RankedJob{{JobID: uuid.New(), Score: 1}}}
	dir.err = apperr.New(apperr.ErrUnavailable, "jobs temporarily unavailable")
	_, err := e.svc.ListRecommendedJobs(asCandidate(u1), cv.ID, 0)
	assert.ErrorIs(t, err, apperr.ErrUnavailable)
}
