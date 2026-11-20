//go:build integration

package integration

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/opensearch"
	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
	"github.com/nuroqzavitex/smarthire/services/job/internal/projection"
)

// updatedEvent is the job.updated envelope the job service would emit for j.
func updatedEvent(t *testing.T, j domain.Job) event.Envelope {
	t.Helper()
	d := projection.FromJob(j)
	env, err := event.New(context.Background(), "job-service", "job.updated", 1, map[string]any{
		"job_id": d.JobID, "recruiter_id": d.RecruiterID, "title": d.Title, "description": d.Description,
		"required_skills": j.RequiredSkills, "nice_to_have_skills": []string{}, "level": d.Level,
		"location": d.Location, "salary_min": d.SalaryMin, "salary_max": d.SalaryMax,
		"status": d.Status, "version": d.Version, "published_at": d.PublishedAt,
	}, time.Now())
	require.NoError(t, err)
	return env
}

func aliasIndices(t *testing.T, alias string) []string {
	t.Helper()
	_, body := osDo(t, http.MethodGet, "/_cat/aliases/"+alias+"?h=index", "")
	return strings.Fields(string(body))
}

func TestRebuild_ConcurrentUpdatesSurvive(t *testing.T) {
	truncate(t)
	c := resetIndices(t)
	ctx := context.Background()
	repo := postgres.NewJobRepo(pool)
	var jobs []domain.Job
	for i := 0; i < 50; i++ {
		jobs = append(jobs, seedOpen(t, fmt.Sprintf("Rebuild role %d", i), []string{"Go"}, "Hà Nội", domain.LevelMiddle, nil, nil, time.Now()))
	}
	indexJobs(t, c, jobs...)

	p := &projection.Projector{Store: c, Validate: func(string, []byte) error { return nil }, AliasTTL: 200 * time.Millisecond, IndexGone: opensearch.IndexGone}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex // serializes the read-modify-write of one job
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := 0; n < 200; n++ {
			select {
			case <-stop:
				return
			default:
			}
			mu.Lock()
			i := rand.IntN(len(jobs))
			j := jobs[i]
			j.Version++
			j.Title = fmt.Sprintf("Rebuild role %d rev %d", i, j.Version)
			if err := repo.Update(ctx, j); err != nil {
				mu.Unlock()
				t.Error(err)
				return
			}
			jobs[i] = j
			mu.Unlock()
			if err := p.Handle(ctx, updatedEvent(t, j), nil); err != nil {
				t.Error(err)
				return
			}
			time.Sleep(15 * time.Millisecond)
		}
	}()
	var searchErrs atomic.Int32
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := c.Search(ctx, app.SearchQuery{Filter: app.SearchFilter{Query: "rebuild"}, Size: 10}); err != nil {
				searchErrs.Add(1)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	name, err := projection.Rebuild(ctx, postgres.NewJobRepo(pool), c, projection.RebuildOptions{AliasTTL: 200 * time.Millisecond, Batch: 7})
	close(stop)
	wg.Wait()
	require.NoError(t, err)
	assert.Equal(t, "jobs_v2", name)
	assert.Zero(t, searchErrs.Load(), "searches kept working during the rebuild")
	assert.Equal(t, []string{"jobs_v2"}, aliasIndices(t, "jobs"))
	assert.Equal(t, []string{"jobs_v2"}, aliasIndices(t, "jobs_write"))
	status, _ := osDo(t, http.MethodGet, "/jobs_v1", "")
	assert.Equal(t, http.StatusNotFound, status, "old index deleted")
	for _, j := range jobs {
		src, v := getDoc(t, "jobs_v2", j.ID.String())
		assert.Equal(t, j.Version, v, j.ID)
		assert.Equal(t, j.Title, src["title"])
	}
}

func TestRebuild_SoftDeletedBecomeTombstones(t *testing.T) {
	truncate(t)
	c := resetIndices(t)
	ctx := context.Background()
	j := seedOpen(t, "Soon deleted role", nil, "Hà Nội", domain.LevelMiddle, nil, nil, time.Now())
	require.NoError(t, j.Close(time.Now()))
	require.NoError(t, j.Delete(time.Now()))
	require.NoError(t, postgres.NewJobRepo(pool).Update(ctx, j))
	name, err := projection.Rebuild(ctx, postgres.NewJobRepo(pool), c, projection.RebuildOptions{Sleep: func(time.Duration) {}})
	require.NoError(t, err)
	src, v := getDoc(t, name, j.ID.String())
	assert.Equal(t, projection.StatusDeleted, src["status"])
	assert.Equal(t, j.Version, v)
}

func TestReconcile_DetectsMissingDoc(t *testing.T) {
	truncate(t)
	c := resetIndices(t)
	ctx := context.Background()
	a := seedOpen(t, "Reconciled role A", nil, "Hà Nội", domain.LevelMiddle, nil, nil, time.Now())
	b := seedOpen(t, "Reconciled role B", nil, "Hà Nội", domain.LevelMiddle, nil, nil, time.Now())
	draft := seedDraft(t, seedOpts{title: "Draft role"})
	indexJobs(t, c, a, b, draft)
	src := postgres.NewJobRepo(pool)
	r, err := projection.Reconcile(ctx, src, c)
	require.NoError(t, err)
	assert.True(t, r.OK(), "%+v", r)
	assert.Equal(t, 2, r.PGOpen)

	osDo(t, http.MethodDelete, "/jobs_v1/_doc/"+b.ID.String()+"?refresh=true", "")
	r, err = projection.Reconcile(ctx, src, c)
	require.NoError(t, err)
	assert.False(t, r.OK())
	assert.Equal(t, []string{b.ID.String()}, r.Diff)

	// OpenSearch remembers the deleted document's version for a while, so the
	// repair is a rebuild, not a re-put of the same version.
	_, err = projection.Rebuild(ctx, src, c, projection.RebuildOptions{Sleep: func(time.Duration) {}})
	require.NoError(t, err)
	refresh(t)
	r, err = projection.Reconcile(ctx, src, c)
	require.NoError(t, err)
	assert.True(t, r.OK(), "%+v", r)
}

var _ projection.IndexAdmin = (*opensearch.Client)(nil)
var _ projection.JobSource = (*postgres.JobRepo)(nil)
