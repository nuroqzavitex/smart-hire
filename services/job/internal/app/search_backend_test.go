package app_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/otel/oteltest"
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
		Next:   json.RawMessage(`[1.5,1732000000000,"x"]`),
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
	assert.JSONEq(t, `[1.5,1732000000000,"x"]`, string(b.calls[1].After))
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

	healthy := &fakeBackend{result: app.SearchResult{Next: json.RawMessage(`[1732000000000,"x"]`)}}
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
	b := &fakeBackend{result: app.SearchResult{Next: json.RawMessage(`[1.2,1732000000000,"x"]`)}}
	e, _ := backendEnv(t, b)
	withQ, err := e.svc.ListOpen(context.Background(), app.SearchFilter{Query: "go"}, app.PageRequest{Size: 1})
	require.NoError(t, err)
	_, err = e.svc.ListOpen(context.Background(), app.SearchFilter{}, app.PageRequest{Size: 1, Token: withQ.NextToken})
	assert.ErrorIs(t, err, apperr.ErrValidation)
	assert.Len(t, b.calls, 1, "a mismatched token never reaches opensearch")
}

func TestListOpen_CraftedOpenSearchTokenRejected(t *testing.T) {
	b := &fakeBackend{}
	e, cj := backendEnv(t, b)
	tok := func(raw string) string { return base64.RawURLEncoding.EncodeToString([]byte(raw)) }
	for _, c := range []struct {
		q   string
		raw string
	}{
		{"", `{"b":"os","sa":["x"]}`},                                  // wrong length
		{"", `{"b":"os","sa":["x","y"]}`},                              // published_at not a number
		{"", `{"b":"os","sa":[1.5,"y"]}`},                              // not an integer
		{"", `{"b":"os","sa":[1,2]}`},                                  // job_id not a string
		{"go", `{"b":"os","q":true,"sa":[1,"y"]}`},                     // missing score
		{"go", `{"b":"os","q":true,"sa":["s",1,"y"]}`},                 // score not a number
		{"go", `{"b":"os","q":true,"sa":[1.5,1,"y",4]}`},               // too long
		{"", `{"b":"os","sa":[1,"` + strings.Repeat("x", 200) + `"]}`}, // job_id too long
	} {
		_, err := e.svc.ListOpen(context.Background(), app.SearchFilter{Query: c.q}, app.PageRequest{Token: tok(c.raw)})
		assert.ErrorIs(t, err, apperr.ErrValidation, c.raw)
	}
	assert.Empty(t, b.calls, "crafted tokens never reach opensearch")
	assert.Zero(t, cj.calls)

	_, err := e.svc.ListOpen(context.Background(), app.SearchFilter{}, app.PageRequest{Token: tok(`{"b":"os","sa":[1732000000000,"a1b2"]}`)})
	assert.NoError(t, err, "well-formed recency token")
	_, err = e.svc.ListOpen(context.Background(), app.SearchFilter{Query: "go"}, app.PageRequest{Token: tok(`{"b":"os","q":true,"sa":[3.25,1732000000000,"a1b2"]}`)})
	assert.NoError(t, err, "well-formed text token")
}

func TestListOpen_CancelledRequestDoesNotFallBack(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e, cj := backendEnv(t, &fakeBackend{err: context.Canceled})
	_, err := e.svc.ListOpen(ctx, app.SearchFilter{}, app.PageRequest{})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, cj.calls, "a client that went away gets no postgres fallback")
}

func TestListOpen_FallbackCounted(t *testing.T) {
	m := oteltest.Install(t)
	e, _ := backendEnv(t, &fakeBackend{err: errors.New("connection refused")})
	e.seedOpen(t, 1)
	_, err := e.svc.ListOpen(context.Background(), app.SearchFilter{}, app.PageRequest{Size: 2})
	require.NoError(t, err)
	v, ok := m.Int64(t, "search.fallback")
	require.True(t, ok)
	assert.Equal(t, int64(1), v)
}
