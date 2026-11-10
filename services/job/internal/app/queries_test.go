package app_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

func TestGet_Visibility(t *testing.T) {
	e := newEnv(t)
	own, other, anon := asRecruiter(r1), asRecruiter(r2), context.Background()

	j := e.create(t, r1)
	_, err := e.svc.Get(anon, j.ID)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	_, err = e.svc.Get(other, j.ID)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	_, err = e.svc.Get(own, j.ID)
	assert.NoError(t, err)
	_, err = e.svc.Get(own, uuid.New())
	assert.ErrorIs(t, err, apperr.ErrNotFound)

	_, err = e.svc.Publish(own, j.ID)
	require.NoError(t, err)
	_, err = e.svc.Get(anon, j.ID)
	assert.NoError(t, err)

	_, err = e.svc.Close(own, j.ID)
	require.NoError(t, err)
	_, err = e.svc.Get(own, j.ID)
	assert.NoError(t, err)
	_, err = e.svc.Get(anon, j.ID)
	assert.ErrorIs(t, err, apperr.ErrNotFound)

	require.NoError(t, e.svc.Delete(own, j.ID))
	_, err = e.svc.Get(own, j.ID)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
}

func TestGet_DeletedOpenJobHiddenEvenIfRepoReturnsIt(t *testing.T) {
	e := newEnv(t)
	j := e.create(t, r1)
	_, err := e.svc.Publish(asRecruiter(r1), j.ID)
	require.NoError(t, err)
	d := e.jobs.m[j.ID]
	now := *d.PublishedAt
	d.DeletedAt = &now
	e.jobs.m[j.ID] = d
	_, err = e.svc.Get(context.Background(), j.ID)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
}

func (e env) seedOpen(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		j := e.create(t, r1)
		_, err := e.svc.Publish(asRecruiter(r1), j.ID)
		require.NoError(t, err)
	}
}

func pagingEnv(t *testing.T) env {
	return newEnv(t, func(d *app.Deps, jobs *fakeJobs) { d.Jobs = pagingJobs{jobs} })
}

func TestListOpen_PagesAndToken(t *testing.T) {
	e := pagingEnv(t)
	e.seedOpen(t, 45)
	seen := map[uuid.UUID]bool{}
	var sizes []int
	token := ""
	for {
		p, err := e.svc.ListOpen(context.Background(), app.SearchFilter{}, app.PageRequest{Size: 20, Token: token})
		require.NoError(t, err)
		sizes = append(sizes, len(p.Jobs))
		for _, j := range p.Jobs {
			assert.False(t, seen[j.ID], "duplicate across pages")
			seen[j.ID] = true
		}
		if p.NextToken == "" {
			break
		}
		token = p.NextToken
	}
	assert.Equal(t, []int{20, 20, 5}, sizes)
	assert.Len(t, seen, 45)
}

func TestListOpen_ExactMultipleHasNoExtraPage(t *testing.T) {
	e := pagingEnv(t)
	e.seedOpen(t, 20)
	p, err := e.svc.ListOpen(context.Background(), app.SearchFilter{}, app.PageRequest{Size: 20})
	require.NoError(t, err)
	assert.Len(t, p.Jobs, 20)
	assert.Empty(t, p.NextToken)
}

func TestListOpen_DefaultAndMaxSize(t *testing.T) {
	e := pagingEnv(t)
	e.seedOpen(t, 25)
	p, err := e.svc.ListOpen(context.Background(), app.SearchFilter{}, app.PageRequest{})
	require.NoError(t, err)
	assert.Len(t, p.Jobs, 20)
	assert.NotEmpty(t, p.NextToken)
	_, err = e.svc.ListOpen(context.Background(), app.SearchFilter{}, app.PageRequest{Size: 101})
	assert.ErrorIs(t, err, apperr.ErrValidation)
	_, err = e.svc.ListOpen(context.Background(), app.SearchFilter{}, app.PageRequest{Size: -1})
	assert.ErrorIs(t, err, apperr.ErrValidation)
	_, err = e.svc.ListOpen(context.Background(), app.SearchFilter{}, app.PageRequest{Size: 100})
	assert.NoError(t, err)
}

func TestListOpen_BadTokenRejected(t *testing.T) {
	e := pagingEnv(t)
	_, err := e.svc.ListOpen(context.Background(), app.SearchFilter{}, app.PageRequest{Token: "!!!"})
	assert.ErrorIs(t, err, apperr.ErrValidation)
}

func TestListJobs_CursorFromOtherQueryRejected(t *testing.T) {
	e := pagingEnv(t)
	e.seedOpen(t, 5)
	ctx := context.Background()
	withQ, err := e.svc.ListOpen(ctx, app.SearchFilter{Query: " go "}, app.PageRequest{Size: 2})
	require.NoError(t, err)
	require.NotEmpty(t, withQ.NextToken)
	_, err = e.svc.ListOpen(ctx, app.SearchFilter{}, app.PageRequest{Size: 2, Token: withQ.NextToken})
	assert.ErrorIs(t, err, apperr.ErrValidation)

	noQ, err := e.svc.ListOpen(ctx, app.SearchFilter{}, app.PageRequest{Size: 2})
	require.NoError(t, err)
	require.NotEmpty(t, noQ.NextToken)
	_, err = e.svc.ListOpen(ctx, app.SearchFilter{Query: "go"}, app.PageRequest{Size: 2, Token: noQ.NextToken})
	assert.ErrorIs(t, err, apperr.ErrValidation)
	// the same query continues fine
	_, err = e.svc.ListOpen(ctx, app.SearchFilter{Query: "go"}, app.PageRequest{Size: 2, Token: withQ.NextToken})
	assert.NoError(t, err)
}

type captureJobs struct {
	pagingJobs
	got app.SearchFilter
}

func (c *captureJobs) Search(ctx context.Context, f app.SearchFilter, a *app.Cursor, l int) ([]app.SearchHit, error) {
	c.got = f
	return c.pagingJobs.Search(ctx, f, a, l)
}

func TestListOpen_NormalizesFilter(t *testing.T) {
	cj := &captureJobs{}
	e := newEnv(t, func(d *app.Deps, jobs *fakeJobs) {
		cj.pagingJobs = pagingJobs{jobs}
		d.Jobs = cj
	})
	_, err := e.svc.ListOpen(context.Background(),
		app.SearchFilter{Query: "  golang  ", Skills: []string{" Go ", "", "KAFKA"}}, app.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, "golang", cj.got.Query)
	assert.Equal(t, []string{"go", "kafka"}, cj.got.Skills)
}

func TestListMine_FiltersByOwnerAndStatus(t *testing.T) {
	e := newEnv(t)
	e.create(t, r1)
	e.create(t, r1)
	open := e.create(t, r1)
	_, err := e.svc.Publish(asRecruiter(r1), open.ID)
	require.NoError(t, err)
	e.create(t, r2)

	p, err := e.svc.ListMine(asRecruiter(r1), nil, app.PageRequest{})
	require.NoError(t, err)
	assert.Len(t, p.Jobs, 3)
	for _, j := range p.Jobs {
		assert.Equal(t, r1, j.RecruiterID)
	}
	draft := domain.StatusDraft
	p, err = e.svc.ListMine(asRecruiter(r1), &draft, app.PageRequest{})
	require.NoError(t, err)
	assert.Len(t, p.Jobs, 2)
	for _, j := range p.Jobs {
		assert.Equal(t, domain.StatusDraft, j.Status)
	}
}

func TestListMine_AuthSizeAndToken(t *testing.T) {
	e := newEnv(t)
	_, err := e.svc.ListMine(context.Background(), nil, app.PageRequest{})
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
	_, err = e.svc.ListMine(asCandidate(), nil, app.PageRequest{})
	assert.ErrorIs(t, err, apperr.ErrForbidden)
	_, err = e.svc.ListMine(asRecruiter(r1), nil, app.PageRequest{Size: 101})
	assert.ErrorIs(t, err, apperr.ErrValidation)
	_, err = e.svc.ListMine(asRecruiter(r1), nil, app.PageRequest{Token: "!!!"})
	assert.ErrorIs(t, err, apperr.ErrValidation)

	// a ranked (text-search) cursor is not valid for ListMine
	rk := float32(0.5)
	tok := app.EncodeCursor(app.Cursor{Rank: &rk, At: tickClock()(), ID: uuid.New()})
	_, err = e.svc.ListMine(asRecruiter(r1), nil, app.PageRequest{Token: tok})
	assert.ErrorIs(t, err, apperr.ErrValidation)
}

func TestListMine_NextToken(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 3; i++ {
		e.create(t, r1)
	}
	p, err := e.svc.ListMine(asRecruiter(r1), nil, app.PageRequest{Size: 2})
	require.NoError(t, err)
	assert.Len(t, p.Jobs, 2)
	require.NotEmpty(t, p.NextToken)
	c, err := app.DecodeCursor(p.NextToken)
	require.NoError(t, err)
	assert.Equal(t, p.Jobs[1].ID, c.ID)
	assert.True(t, p.Jobs[1].CreatedAt.Equal(c.At))
	assert.Nil(t, c.Rank)
}
