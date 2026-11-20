package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

// fakeBackend is a read model returning canned pages.
type fakeBackend struct {
	err    error
	calls  []app.SearchQuery
	result app.SearchResult
}

func (f *fakeBackend) Search(_ context.Context, q app.SearchQuery) (app.SearchResult, error) {
	f.calls = append(f.calls, q)
	if f.err != nil {
		return app.SearchResult{}, f.err
	}
	return f.result, nil
}

// countingJobs records Postgres search calls and the cursor they got.
type countingJobs struct {
	pagingJobs
	calls  int
	cursor *app.Cursor
}

func (c *countingJobs) Search(ctx context.Context, f app.SearchFilter, a *app.Cursor, l int) ([]app.SearchHit, error) {
	c.calls++
	c.cursor = a
	return c.pagingJobs.Search(ctx, f, a, l)
}

func backendEnv(t *testing.T, b app.SearchBackend) (env, *countingJobs) {
	cj := &countingJobs{}
	e := newEnv(t, func(d *app.Deps, jobs *fakeJobs) {
		cj.pagingJobs = pagingJobs{jobs}
		d.Jobs = cj
		d.Search = b
	})
	return e, cj
}

func TestListOpen_UsesSearchBackend(t *testing.T) {
	b := &fakeBackend{result: app.SearchResult{
		Jobs:   []domain.Job{{Title: "a"}, {Title: "b"}},
		Next:   json.RawMessage(`[1,"x"]`),
		Facets: app.Facets{Skills: []app.FacetCount{{Value: "go", Count: 2}}},
	}}
	e, cj := backendEnv(t, b)
	p, err := e.svc.ListOpen(context.Background(), app.SearchFilter{Query: " Go "}, app.PageRequest{Size: 2})
	require.NoError(t, err)
	assert.Len(t, p.Jobs, 2)
	assert.False(t, p.Degraded)
	assert.Equal(t, b.result.Facets, p.Facets)
	assert.NotEmpty(t, p.NextToken)
	assert.Zero(t, cj.calls, "postgres not used")
	require.Len(t, b.calls, 1)
	assert.Equal(t, 2, b.calls[0].Size)
	assert.Equal(t, "Go", b.calls[0].Filter.Query)

	_, err = e.svc.ListOpen(context.Background(), app.SearchFilter{Query: "go"}, app.PageRequest{Size: 2, Token: p.NextToken})
	require.NoError(t, err)
	assert.JSONEq(t, `[1,"x"]`, string(b.calls[1].After))
}

func TestListOpen_FallbackOnError(t *testing.T) {
	e, cj := backendEnv(t, &fakeBackend{err: errors.New("connection refused")})
	e.seedOpen(t, 3)
	p, err := e.svc.ListOpen(context.Background(), app.SearchFilter{}, app.PageRequest{Size: 2})
	require.NoError(t, err)
	assert.True(t, p.Degraded)
	assert.Len(t, p.Jobs, 2)
	assert.Empty(t, p.Facets.Skills)
	assert.Equal(t, 1, cj.calls)
	assert.NotEmpty(t, p.NextToken)
}

func TestListOpen_TokenFromOtherBackend(t *testing.T) {
	ctx := context.Background()
	failing := &fakeBackend{err: errors.New("down")}
	e, cj := backendEnv(t, failing)
	e.seedOpen(t, 3)
	pg, err := e.svc.ListOpen(ctx, app.SearchFilter{}, app.PageRequest{Size: 1})
	require.NoError(t, err)
	require.NotEmpty(t, pg.NextToken)

	healthy := &fakeBackend{result: app.SearchResult{Next: json.RawMessage(`["x"]`)}}
	e2, _ := backendEnv(t, healthy)
	_, err = e2.svc.ListOpen(ctx, app.SearchFilter{}, app.PageRequest{Size: 1, Token: pg.NextToken})
	require.NoError(t, err)
	assert.Nil(t, healthy.calls[0].After, "postgres token gives page 1 of opensearch")

	osPage, err := e2.svc.ListOpen(ctx, app.SearchFilter{}, app.PageRequest{Size: 1})
	require.NoError(t, err)
	require.NotEmpty(t, osPage.NextToken)
	_, err = e.svc.ListOpen(ctx, app.SearchFilter{}, app.PageRequest{Size: 1, Token: osPage.NextToken})
	require.NoError(t, err)
	assert.Nil(t, cj.cursor, "opensearch token gives page 1 of postgres")
}

func TestListOpen_MalformedToken(t *testing.T) {
	e, _ := backendEnv(t, &fakeBackend{})
	for _, tok := range []string{"x", "e30", "eyJiIjoib3MifQ", "eyJiIjoicGcifQ"} { // "{}", {"b":"os"}, {"b":"pg"}
		_, err := e.svc.ListOpen(context.Background(), app.SearchFilter{}, app.PageRequest{Token: tok})
		assert.ErrorIs(t, err, apperr.ErrValidation, tok)
	}
}

func TestListOpen_OpenSearchTokenFromOtherQueryRejected(t *testing.T) {
	b := &fakeBackend{result: app.SearchResult{Next: json.RawMessage(`[1.2,"x"]`)}}
	e, _ := backendEnv(t, b)
	withQ, err := e.svc.ListOpen(context.Background(), app.SearchFilter{Query: "go"}, app.PageRequest{Size: 1})
	require.NoError(t, err)
	_, err = e.svc.ListOpen(context.Background(), app.SearchFilter{}, app.PageRequest{Size: 1, Token: withQ.NextToken})
	assert.ErrorIs(t, err, apperr.ErrValidation)
	assert.Len(t, b.calls, 1, "a mismatched token never reaches opensearch")
}
