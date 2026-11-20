//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/opensearch"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/bootstrap"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
	"github.com/nuroqzavitex/smarthire/services/job/internal/projection"
)

// TestListOpen_FallbackWhenOpenSearchStops runs its own OpenSearch so it can
// stop it without disturbing the shared one.
func TestListOpen_FallbackWhenOpenSearchStops(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	url, stop, err := startOpenSearch(ctx)
	require.NoError(t, err)
	defer stop()
	c, err := opensearch.New(url)
	require.NoError(t, err)
	require.NoError(t, c.EnsureIndex(ctx))

	j := seedOpen(t, "Fallback backend role", []string{"Go"}, "Hà Nội", domain.LevelMiddle, nil, nil, time.Now())
	_, err = c.Put(ctx, "jobs_v1", projection.FromJob(j))
	require.NoError(t, err)
	_, err = c.Search(ctx, app.SearchQuery{Size: 1}) // warm up
	require.NoError(t, err)
	osDoAt(t, url, "/jobs/_refresh")

	guarded := opensearch.NewGuarded(c, 2, time.Minute, 500*time.Millisecond)
	svc := bootstrap.NewService(pool, guarded, nil, nil)

	p, err := svc.ListOpen(ctx, app.SearchFilter{Query: "backend"}, app.PageRequest{})
	require.NoError(t, err)
	assert.False(t, p.Degraded)
	require.Len(t, p.Jobs, 1)
	assert.NotEmpty(t, p.Facets.Skills)

	stop()
	for i := 0; i < 3; i++ {
		p, err = svc.ListOpen(ctx, app.SearchFilter{Query: "backend"}, app.PageRequest{})
		require.NoError(t, err, "call %d", i)
		assert.True(t, p.Degraded, "call %d", i)
		require.Len(t, p.Jobs, 1)
		assert.Equal(t, j.ID, p.Jobs[0].ID)
	}
}

func osDoAt(t *testing.T, base, path string) {
	t.Helper()
	saved := osURL
	osURL = base
	defer func() { osURL = saved }()
	osDo(t, "POST", path, "")
}
