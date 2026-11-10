//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

func TestMigrations_UpDown(t *testing.T) {
	var super bool
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT rolsuper FROM pg_roles WHERE rolname = current_user`).Scan(&super))
	require.False(t, super, "migrations must run as a non-superuser owner")

	mg, err := newMigrate()
	require.NoError(t, err)
	defer func() { _, _ = mg.Close() }()
	require.NoError(t, mg.Down())
	require.NoError(t, mg.Up())
	assert.ErrorIs(t, mg.Up(), migrate.ErrNoChange)
}

func TestRepo_GetIgnoresDeleted(t *testing.T) {
	truncate(t)
	repo := postgres.NewJobRepo(pool)
	ctx := context.Background()
	j := seedDraft(t, seedOpts{title: "Backend Engineer"})
	got, err := repo.Get(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, j.Title, got.Title)

	require.NoError(t, j.Delete(time.Now()))
	require.NoError(t, repo.Update(ctx, j))
	_, err = repo.Get(ctx, j.ID)
	assert.ErrorIs(t, err, domain.ErrJobNotFound)
	_, err = repo.GetForUpdate(ctx, j.ID)
	assert.ErrorIs(t, err, domain.ErrJobNotFound)
	_, err = repo.Get(ctx, uuid.New())
	assert.ErrorIs(t, err, domain.ErrJobNotFound)
}

func TestRepo_RoundTripsAllFields(t *testing.T) {
	truncate(t)
	repo := postgres.NewJobRepo(pool)
	j := seed(t, seedOpts{title: "Round Trip Job", skills: []string{"Go", "SQL"}, location: "Đà Nẵng",
		level: domain.LevelSenior, salaryMin: intp(1000), salaryMax: intp(2000)})
	got, err := repo.Get(context.Background(), j.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"Go", "SQL"}, got.RequiredSkills)
	assert.Equal(t, []string{}, got.NiceToHaveSkills)
	assert.Equal(t, 1000, *got.SalaryMin)
	assert.Equal(t, 2000, *got.SalaryMax)
	assert.Equal(t, domain.StatusOpen, got.Status)
	assert.Equal(t, j.Version, got.Version)
	require.NotNil(t, got.PublishedAt)
	assert.True(t, j.PublishedAt.Equal(*got.PublishedAt))
}

func TestRepo_GetForUpdateLocksRow(t *testing.T) {
	truncate(t)
	repo := postgres.NewJobRepo(pool)
	tm := postgres.NewTxManager(pool)
	j := seedDraft(t, seedOpts{title: "Locked Job Title"})
	ctx := context.Background()

	locked, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- tm.WithTx(ctx, func(ctx context.Context) error {
			if _, err := repo.GetForUpdate(ctx, j.ID); err != nil {
				return err
			}
			close(locked)
			<-release
			return nil
		})
	}()
	<-locked
	got := make(chan struct{})
	go func() {
		_ = tm.WithTx(ctx, func(ctx context.Context) error {
			_, err := repo.GetForUpdate(ctx, j.ID)
			close(got)
			return err
		})
	}()
	select {
	case <-got:
		t.Fatal("second GetForUpdate did not block")
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-done)
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("second GetForUpdate never unblocked")
	}
}

func TestListByRecruiter_StatusAndPaging(t *testing.T) {
	truncate(t)
	repo := postgres.NewJobRepo(pool)
	ctx := context.Background()
	me, other := uuid.New(), uuid.New()
	base := time.Now().UTC().Truncate(time.Microsecond)
	for i := range 7 {
		o := seedOpts{title: "My Job Title", recruiter: me, publishedAt: base.Add(time.Duration(i/2) * time.Minute)} // pairs share created_at
		if i%2 == 0 {
			seed(t, o)
		} else {
			seedDraft(t, o)
		}
	}
	seed(t, seedOpts{title: "Someone Else", recruiter: other})
	deleted := seedDraft(t, seedOpts{title: "Deleted Draft", recruiter: me})
	require.NoError(t, deleted.Delete(time.Now()))
	require.NoError(t, repo.Update(ctx, deleted))

	all, err := repo.ListByRecruiter(ctx, me, nil, nil, 100)
	require.NoError(t, err)
	require.Len(t, all, 7)

	open := domain.StatusOpen
	got, err := repo.ListByRecruiter(ctx, me, &open, nil, 100)
	require.NoError(t, err)
	assert.Len(t, got, 4)

	var seen []uuid.UUID
	var cur *app.Cursor
	for range 5 {
		page, err := repo.ListByRecruiter(ctx, me, nil, cur, 3)
		require.NoError(t, err)
		for _, j := range page {
			seen = append(seen, j.ID)
		}
		if len(page) < 3 {
			break
		}
		last := page[len(page)-1]
		cur = &app.Cursor{At: last.CreatedAt, ID: last.ID}
	}
	want := make([]uuid.UUID, len(all))
	for i, j := range all {
		want[i] = j.ID
	}
	assert.Equal(t, want, seen, "paging must reproduce the single-query order without duplicates or gaps")
}
