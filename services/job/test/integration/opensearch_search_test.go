//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/opensearch"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
	"github.com/nuroqzavitex/smarthire/services/job/internal/projection"
)

// indexJobs writes jobs to jobs_v1 as the projector would and refreshes.
func indexJobs(t *testing.T, c *opensearch.Client, jobs ...domain.Job) {
	t.Helper()
	for _, j := range jobs {
		_, err := c.Put(context.Background(), "jobs_v1", projection.FromJob(j))
		require.NoError(t, err)
	}
	refresh(t)
}

func osSearch(t *testing.T, c *opensearch.Client, f app.SearchFilter) app.SearchResult {
	t.Helper()
	res, err := c.Search(context.Background(), app.SearchQuery{Filter: f, Size: 100})
	require.NoError(t, err)
	return res
}

func jobIDs(jobs []domain.Job) []uuid.UUID {
	out := make([]uuid.UUID, len(jobs))
	for i, j := range jobs {
		out[i] = j.ID
	}
	return out
}

func TestSearch_UnaccentedMatchesAccented(t *testing.T) {
	truncate(t)
	c := resetIndices(t)
	j := seedOpen(t, "Kỹ sư Go tại Đà Nẵng", nil, "Hà Nội", domain.LevelMiddle, nil, nil, time.Now())
	indexJobs(t, c, j)
	for _, q := range []string{"ky su", "ha noi", "da nang", "Kỹ Sư"} {
		assert.Equal(t, []uuid.UUID{j.ID}, jobIDs(osSearch(t, c, app.SearchFilter{Query: q}).Jobs), q)
	}
	assert.Empty(t, osSearch(t, c, app.SearchFilter{Query: "ky su rust"}).Jobs, "every term must match")
}

func TestSearch_SkillSynonym(t *testing.T) {
	truncate(t)
	c := resetIndices(t)
	golang := seedOpen(t, "Backend developer", []string{"Golang"}, "Hà Nội", domain.LevelMiddle, nil, nil, time.Now())
	k8s := seedOpen(t, "Platform engineer", []string{"Kubernetes"}, "Hà Nội", domain.LevelMiddle, nil, nil, time.Now())
	indexJobs(t, c, golang, k8s)
	assert.Equal(t, []uuid.UUID{golang.ID}, jobIDs(osSearch(t, c, app.SearchFilter{Query: "go"}).Jobs))
	assert.Equal(t, []uuid.UUID{k8s.ID}, jobIDs(osSearch(t, c, app.SearchFilter{Query: "k8s"}).Jobs))
}

func TestSearch_ExcludesNonOpen(t *testing.T) {
	truncate(t)
	c := resetIndices(t)
	open := seedOpen(t, "Open backend role", nil, "Hà Nội", domain.LevelMiddle, nil, nil, time.Now())
	draft := seedDraft(t, seedOpts{title: "Draft backend role"})
	indexJobs(t, c, open, draft)
	for _, d := range []projection.Document{
		{JobID: uuid.NewString(), Version: 2, Status: projection.StatusClosed},
		{JobID: uuid.NewString(), Version: 3, Status: projection.StatusDeleted},
	} {
		_, err := c.Put(context.Background(), "jobs_v1", d)
		require.NoError(t, err)
	}
	refresh(t)
	assert.Equal(t, []uuid.UUID{open.ID}, jobIDs(osSearch(t, c, app.SearchFilter{}).Jobs))
	assert.Equal(t, []uuid.UUID{open.ID}, jobIDs(osSearch(t, c, app.SearchFilter{Query: "backend"}).Jobs))
}

func TestSearch_FilterParityWithPostgres(t *testing.T) {
	truncate(t)
	c := resetIndices(t)
	now := time.Now()
	jobs := []domain.Job{
		seedOpen(t, "Developer one", []string{"Go", "SQL"}, "Hà Nội", domain.LevelSenior, intp(1000), intp(3000), now),
		seedOpen(t, "Developer two", []string{"Golang"}, "Ha Noi", domain.LevelSenior, nil, nil, now.Add(time.Second)),
		seedOpen(t, "Developer three", []string{"Rust"}, "Hải Phòng", domain.LevelJunior, nil, intp(5000), now.Add(2*time.Second)),
		seedOpen(t, "Developer four", []string{"Python"}, "hà nội", domain.LevelMiddle, intp(500), intp(1500), now.Add(3*time.Second)),
		seedOpen(t, "Developer five", []string{"Java"}, "Đà Nẵng", domain.LevelLead, nil, intp(2999), now.Add(4*time.Second)),
		seedOpen(t, "Developer six", nil, "Hà Nội", domain.LevelSenior, nil, intp(3000), now.Add(5*time.Second)),
	}
	indexJobs(t, c, jobs...)
	senior := domain.LevelSenior
	for name, f := range map[string]app.SearchFilter{
		"skills":   {Skills: []string{"golang", "rust"}},
		"location": {Location: "ha noi"},
		"level":    {Level: &senior},
		"salary":   {SalaryMin: intp(3000)},
		"combined": {Location: "Hà Nội", Level: &senior, SalaryMin: intp(3000)},
		"text":     {Query: "developer"},
	} {
		assert.ElementsMatch(t, ids(search(t, f)), jobIDs(osSearch(t, c, f).Jobs), name)
	}
}

func TestSearch_PaginationNoDupNoGap(t *testing.T) {
	truncate(t)
	c := resetIndices(t)
	now := time.Now()
	var all []domain.Job
	for i := 0; i < 25; i++ {
		// Pairs share a publish time so the job_id tiebreaker matters.
		all = append(all, seedOpen(t, "Paged backend role", nil, "Hà Nội", domain.LevelMiddle, nil, nil, now.Add(time.Duration(i/2)*time.Second)))
	}
	indexJobs(t, c, all...)
	for _, q := range []string{"", "backend"} {
		seen := map[uuid.UUID]bool{}
		var after json.RawMessage
		pages := 0
		for {
			res, err := c.Search(context.Background(), app.SearchQuery{Filter: app.SearchFilter{Query: q}, After: after, Size: 10})
			require.NoError(t, err)
			pages++
			for _, j := range res.Jobs {
				assert.False(t, seen[j.ID], "duplicate %s", j.ID)
				seen[j.ID] = true
			}
			if res.Next == nil {
				break
			}
			after = res.Next
		}
		assert.Len(t, seen, 25, q)
		assert.Equal(t, 3, pages, q)
	}
}

func TestSearch_Facets(t *testing.T) {
	truncate(t)
	c := resetIndices(t)
	now := time.Now()
	indexJobs(t, c,
		seedOpen(t, "Facet one", []string{"Go", "SQL"}, "Hà Nội", domain.LevelSenior, nil, nil, now),
		seedOpen(t, "Facet two", []string{"go"}, "Hà Nội", domain.LevelJunior, nil, nil, now),
		seedOpen(t, "Facet three", []string{"Rust"}, "Huế", domain.LevelSenior, nil, nil, now),
	)
	f := osSearch(t, c, app.SearchFilter{}).Facets
	assert.Equal(t, []app.FacetCount{{Value: "go", Count: 2}, {Value: "rust", Count: 1}, {Value: "sql", Count: 1}}, f.Skills)
	assert.Equal(t, []app.FacetCount{{Value: "ha noi", Count: 2}, {Value: "hue", Count: 1}}, f.Locations)
	assert.Equal(t, []app.FacetCount{{Value: "senior", Count: 2}, {Value: "junior", Count: 1}}, f.Levels)
}

func TestSearch_MapsDocumentBack(t *testing.T) {
	truncate(t)
	c := resetIndices(t)
	j := seedOpen(t, "Mapped role", []string{"Go"}, "Hà Nội", domain.LevelLead, intp(100), intp(200), time.Now())
	indexJobs(t, c, j)
	got := osSearch(t, c, app.SearchFilter{}).Jobs
	require.Len(t, got, 1)
	assert.Equal(t, j.ID, got[0].ID)
	assert.Equal(t, j.RecruiterID, got[0].RecruiterID)
	assert.Equal(t, j.Title, got[0].Title)
	assert.Equal(t, j.RequiredSkills, got[0].RequiredSkills)
	assert.Equal(t, domain.LevelLead, got[0].Level)
	assert.Equal(t, domain.StatusOpen, got[0].Status)
	assert.Equal(t, j.Version, got[0].Version)
	assert.Equal(t, *j.SalaryMax, *got[0].SalaryMax)
	require.NotNil(t, got[0].PublishedAt)
	assert.True(t, j.PublishedAt.Equal(*got[0].PublishedAt))
}
