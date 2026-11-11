//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

func strp(s string) *string { return &s }

func TestMigrations_UpDown(t *testing.T) {
	var super bool
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT rolsuper FROM pg_roles WHERE rolname = current_user`).Scan(&super))
	require.False(t, super, "migrations must run as a non-superuser owner")

	mg, err := newMigrate()
	require.NoError(t, err)
	defer func() { _, _ = mg.Close() }()
	require.NoError(t, mg.Down())
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename IN ('candidates','cvs','outbox','processed_events')`).Scan(&n))
	assert.Equal(t, 0, n)
	require.NoError(t, mg.Up())
	assert.ErrorIs(t, mg.Up(), migrate.ErrNoChange)
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename IN ('candidates','cvs','outbox','processed_events')`).Scan(&n))
	assert.Equal(t, 4, n)
}

// seedCV inserts an UPLOADING CV of candidate created at at.
func seedCV(t *testing.T, candidate uuid.UUID, at time.Time) domain.CV {
	t.Helper()
	cv, err := domain.NewCV(uuid.New(), candidate, "cv.pdf", at)
	require.NoError(t, err)
	require.NoError(t, postgres.NewCVRepo(pool).Create(context.Background(), cv))
	return cv
}

func TestRepo_ListByCandidateKeyset(t *testing.T) {
	truncate(t)
	owner := uuid.New()
	base := time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC)
	var all []domain.CV
	for i := range 20 {
		at := base.Add(time.Duration(i) * time.Second)
		all = append(all, seedCV(t, owner, at))
		if i%4 == 0 { // 5 timestamps shared by two CVs
			all = append(all, seedCV(t, owner, at))
		}
	}
	require.Len(t, all, 25)
	seedCV(t, uuid.New(), base.Add(time.Hour)) // another candidate's CV is never listed

	sort.Slice(all, func(i, j int) bool {
		if !all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].CreatedAt.After(all[j].CreatedAt)
		}
		return bytes.Compare(all[i].ID[:], all[j].ID[:]) > 0
	})

	ctx := asCandidate(owner)
	var got []uuid.UUID
	var sizes []int
	token := ""
	for range 10 {
		page, err := svc.ListMyCVs(ctx, app.PageRequest{Size: 10, Token: token})
		require.NoError(t, err)
		sizes = append(sizes, len(page.CVs))
		for _, cv := range page.CVs {
			got = append(got, cv.ID)
		}
		if page.NextToken == "" {
			break
		}
		token = page.NextToken
	}
	assert.Equal(t, []int{10, 10, 5}, sizes)
	want := make([]uuid.UUID, len(all))
	for i, cv := range all {
		want[i] = cv.ID
	}
	assert.Equal(t, want, got)
}

func TestRepo_UpsertKeepsCreatedAt(t *testing.T) {
	truncate(t)
	repo := postgres.NewCandidateRepo(pool)
	ctx := context.Background()
	uid := uuid.New()
	t1 := time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC)
	t2 := t1.Add(48 * time.Hour)

	_, err := repo.Get(ctx, uid)
	require.ErrorIs(t, err, domain.ErrProfileNotFound)

	first, err := repo.Upsert(ctx, domain.Candidate{UserID: uid, FullName: "An", Phone: strp("+84 912 345 678"),
		CreatedAt: t1, UpdatedAt: t1})
	require.NoError(t, err)
	assert.True(t, first.CreatedAt.Equal(t1))
	assert.Nil(t, first.Location)

	second, err := repo.Upsert(ctx, domain.Candidate{UserID: uid, FullName: "Bình", Location: strp("Hà Nội"),
		CreatedAt: t2, UpdatedAt: t2})
	require.NoError(t, err)
	assert.True(t, second.CreatedAt.Equal(t1), "created_at must be kept")
	assert.True(t, second.UpdatedAt.Equal(t2))
	assert.Equal(t, "Bình", second.FullName)
	assert.Nil(t, second.Phone)
	require.NotNil(t, second.Location)
	assert.Equal(t, "Hà Nội", *second.Location)

	got, err := repo.Get(ctx, uid)
	require.NoError(t, err)
	assert.Equal(t, second, got)
}

func TestRepo_CVRoundTripAndNotFound(t *testing.T) {
	truncate(t)
	repo := postgres.NewCVRepo(pool)
	ctx := context.Background()
	now := time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC)
	cv := seedCV(t, uuid.New(), now)

	got, err := repo.Get(ctx, cv.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusUploading, got.Status)
	assert.Empty(t, got.AttemptID)
	assert.Nil(t, got.ParsedData)
	assert.Nil(t, got.SizeBytes)
	assert.Nil(t, got.ParseError)
	assert.Nil(t, got.ParsedAt)
	assert.Equal(t, cv.ObjectKey, got.ObjectKey)
	assert.True(t, got.ConsentAt.Equal(now))

	attempt := ulid.Make().String()
	require.NoError(t, cv.Complete(1234, attempt, now.Add(time.Second)))
	parsed := json.RawMessage(`{"skills": ["Go"]}`)
	require.NoError(t, cv.ApplyParsed(attempt, parsed, now.Add(2*time.Second)))
	require.NoError(t, repo.Update(ctx, cv))

	got, err = repo.GetForUpdate(ctx, cv.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusParsed, got.Status)
	assert.Equal(t, attempt, got.AttemptID)
	assert.JSONEq(t, string(parsed), string(got.ParsedData))
	require.NotNil(t, got.SizeBytes)
	assert.EqualValues(t, 1234, *got.SizeBytes)
	require.NotNil(t, got.ParsedAt)
	assert.True(t, got.ParsedAt.Equal(now.Add(2*time.Second)))
	assert.Equal(t, int64(3), got.Version)

	require.NoError(t, cv.Reparse(ulid.Make().String(), now.Add(3*time.Second)))
	require.NoError(t, repo.Update(ctx, cv))
	got, err = repo.Get(ctx, cv.ID)
	require.NoError(t, err)
	assert.Nil(t, got.ParsedData, "reparse clears parsed_data to NULL")
	assert.Nil(t, got.ParsedAt)

	_, err = repo.Get(ctx, uuid.New())
	assert.ErrorIs(t, err, domain.ErrCVNotFound)
	_, err = repo.GetForUpdate(ctx, uuid.New())
	assert.ErrorIs(t, err, domain.ErrCVNotFound)
	assert.ErrorIs(t, repo.Update(ctx, domain.CV{ID: uuid.New(), Status: domain.StatusUploaded}), domain.ErrCVNotFound)
}
