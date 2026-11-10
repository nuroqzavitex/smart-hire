//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

const defaultDesc = "Mô tả công việc chung cho vị trí này"

type seedOpts struct {
	title, desc, location string
	skills                []string
	level                 domain.Level
	salaryMin, salaryMax  *int
	publishedAt           time.Time
	recruiter             uuid.UUID
}

func intp(n int) *int { return &n }

// seedOpen creates a job through JobRepo.Create and publishes it with Update.
func seedOpen(t *testing.T, title string, skills []string, location string, level domain.Level, salaryMin, salaryMax *int, publishedAt time.Time) domain.Job {
	t.Helper()
	return seed(t, seedOpts{title: title, skills: skills, location: location, level: level,
		salaryMin: salaryMin, salaryMax: salaryMax, publishedAt: publishedAt})
}

func seed(t *testing.T, o seedOpts) domain.Job {
	t.Helper()
	j := seedDraft(t, o)
	require.NoError(t, j.Publish(j.CreatedAt))
	require.NoError(t, postgres.NewJobRepo(pool).Update(context.Background(), j))
	return j
}

// seedDraft inserts a draft; o.publishedAt doubles as the creation time.
func seedDraft(t *testing.T, o seedOpts) domain.Job {
	t.Helper()
	if o.desc == "" {
		o.desc = defaultDesc
	}
	if o.location == "" {
		o.location = "Hà Nội"
	}
	if o.level == "" {
		o.level = domain.LevelMiddle
	}
	if o.recruiter == uuid.Nil {
		o.recruiter = uuid.New()
	}
	if o.publishedAt.IsZero() {
		o.publishedAt = time.Now().UTC().Truncate(time.Microsecond)
	}
	j, err := domain.NewJob(uuid.New(), o.recruiter, domain.Draft{
		Title: o.title, Description: o.desc, RequiredSkills: o.skills, Level: o.level,
		Location: o.location, SalaryMin: o.salaryMin, SalaryMax: o.salaryMax,
	}, o.publishedAt)
	require.NoError(t, err)
	require.NoError(t, postgres.NewJobRepo(pool).Create(context.Background(), j))
	return j
}

func search(t *testing.T, f app.SearchFilter) []app.SearchHit {
	t.Helper()
	hits, err := postgres.NewJobRepo(pool).Search(context.Background(), f, nil, 100)
	require.NoError(t, err)
	return hits
}

func ids(hits []app.SearchHit) []uuid.UUID {
	out := make([]uuid.UUID, len(hits))
	for i, h := range hits {
		out[i] = h.Job.ID
	}
	return out
}
