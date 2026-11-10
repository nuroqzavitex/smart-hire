//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

func TestSearch_UnaccentMatches(t *testing.T) {
	truncate(t)
	seedOpen(t, "Lập trình viên Go", nil, "Hà Nội", domain.LevelMiddle, nil, nil, time.Now())
	assert.Len(t, search(t, app.SearchFilter{Query: "lap trinh"}), 1)
	assert.Len(t, search(t, app.SearchFilter{Query: "Lập Trình"}), 1)
}

func TestSearch_UnaccentHandlesDStrokeAndCase(t *testing.T) {
	truncate(t)
	seedOpen(t, "Kỹ sư Đường ống ĐÀ NẴNG", nil, "Hà Nội", domain.LevelMiddle, nil, nil, time.Now())
	assert.Len(t, search(t, app.SearchFilter{Query: "duong ong"}), 1)
	assert.Len(t, search(t, app.SearchFilter{Query: "da nang"}), 1)
}

func TestSearch_HostileQueryDoesNotError(t *testing.T) {
	truncate(t)
	repo := postgres.NewJobRepo(pool)
	for _, q := range []string{`c++ & | !`, `"unbalanced`, `((`, `!!!`, `-`, `or`} {
		_, err := repo.Search(context.Background(), app.SearchFilter{Query: q}, nil, 21)
		assert.NoError(t, err, q)
	}
}

func TestSearch_TitleRanksAboveDescription(t *testing.T) {
	truncate(t)
	now := time.Now()
	// The description-only match is newer, so recency alone would put it first.
	inTitle := seed(t, seedOpts{title: "Golang backend developer", publishedAt: now.Add(-time.Hour)})
	inDesc := seed(t, seedOpts{title: "Backend developer", desc: "Chúng tôi dùng golang cho mọi dịch vụ", publishedAt: now})
	hits := search(t, app.SearchFilter{Query: "golang"})
	require.Len(t, hits, 2)
	assert.Equal(t, inTitle.ID, hits[0].Job.ID)
	assert.Equal(t, inDesc.ID, hits[1].Job.ID)
	assert.Greater(t, hits[0].Rank, hits[1].Rank)
}

func TestSearch_Filters(t *testing.T) {
	truncate(t)
	now := time.Now()
	goHN := seedOpen(t, "Developer one", []string{"Go", "SQL"}, "Hà Nội", domain.LevelSenior, intp(1000), intp(3000), now)
	pyHP := seedOpen(t, "Developer two", []string{"Python"}, "Hải Phòng", domain.LevelJunior, nil, intp(1500), now.Add(time.Second))
	noSal := seedOpen(t, "Developer three", []string{"Java"}, "Hà Nội", domain.LevelSenior, nil, nil, now.Add(2*time.Second))

	for _, q := range []string{"", "developer"} {
		name := "query=" + q
		t.Run(name, func(t *testing.T) {
			f := func(mod func(*app.SearchFilter)) []uuid.UUID {
				flt := app.SearchFilter{Query: q}
				mod(&flt)
				return ids(search(t, flt))
			}
			assert.ElementsMatch(t, []uuid.UUID{goHN.ID}, f(func(s *app.SearchFilter) { s.Skills = []string{"go"} }))
			assert.ElementsMatch(t, []uuid.UUID{goHN.ID, noSal.ID}, f(func(s *app.SearchFilter) { s.Location = "HA NOI" }))
			assert.ElementsMatch(t, []uuid.UUID{goHN.ID, noSal.ID}, f(func(s *app.SearchFilter) { s.Location = "hà nội" }))
			assert.NotContains(t, f(func(s *app.SearchFilter) { s.Location = "hà nội" }), pyHP.ID)
			senior := domain.LevelSenior
			assert.ElementsMatch(t, []uuid.UUID{goHN.ID, noSal.ID}, f(func(s *app.SearchFilter) { s.Level = &senior }))
			// salary_min 2000: NULL salary_max and salary_max >= 2000 stay, 1500 goes.
			assert.ElementsMatch(t, []uuid.UUID{goHN.ID, noSal.ID}, f(func(s *app.SearchFilter) { s.SalaryMin = intp(2000) }))
		})
	}
}

func TestSearch_OnlyOpenNotDeleted(t *testing.T) {
	truncate(t)
	repo := postgres.NewJobRepo(pool)
	ctx := context.Background()
	open := seed(t, seedOpts{title: "Visible shared title"})
	seedDraft(t, seedOpts{title: "Draft shared title"})
	closed := seed(t, seedOpts{title: "Closed shared title"})
	require.NoError(t, closed.Close(time.Now()))
	require.NoError(t, repo.Update(ctx, closed))
	// open -> closed -> deleted (open jobs cannot be deleted directly)
	gone := seed(t, seedOpts{title: "Deleted shared title"})
	require.NoError(t, gone.Close(time.Now()))
	require.NoError(t, gone.Delete(time.Now()))
	require.NoError(t, repo.Update(ctx, gone))
	// A deleted job that still has status open (defensive: partial index must not leak it).
	_, err := pool.Exec(ctx, `UPDATE jobs SET deleted_at = now() WHERE id = $1`, seed(t, seedOpts{title: "Open but deleted shared title"}).ID)
	require.NoError(t, err)

	for _, q := range []string{"", "shared"} {
		hits := search(t, app.SearchFilter{Query: q})
		require.Len(t, hits, 1, "query=%q", q)
		assert.Equal(t, open.ID, hits[0].Job.ID)
	}
}

// TestSearch_CursorStableAcrossPages pages through 25 open jobs in groups of
// published_at ties (5 per timestamp) and, for the query variant, equal rank
// too, so every page boundary is decided by the id tie-break alone.
func TestSearch_CursorStableAcrossPages(t *testing.T) {
	for _, q := range []string{"", "golang"} {
		t.Run(fmt.Sprintf("query=%q", q), func(t *testing.T) {
			truncate(t)
			repo := postgres.NewJobRepo(pool)
			ctx := context.Background()
			base := time.Now().UTC().Truncate(time.Microsecond)
			for i := range 25 {
				seed(t, seedOpts{title: fmt.Sprintf("Golang developer %02d", i),
					publishedAt: base.Add(time.Duration(i/5) * time.Minute)})
			}
			f := app.SearchFilter{Query: q}
			all, err := repo.Search(ctx, f, nil, 100)
			require.NoError(t, err)
			require.Len(t, all, 25)

			var got []uuid.UUID
			var cur *app.Cursor
			pages := 0
			for {
				page, err := repo.Search(ctx, f, cur, 10)
				require.NoError(t, err)
				for _, h := range page {
					got = append(got, h.Job.ID)
				}
				pages++
				if len(page) < 10 {
					break
				}
				last := page[len(page)-1]
				c := app.Cursor{At: *last.Job.PublishedAt, ID: last.Job.ID}
				if q != "" {
					r := last.Rank
					c.Rank = &r
				}
				// round-trip through the opaque token like the app does
				dec, err := app.DecodeCursor(app.EncodeCursor(c))
				require.NoError(t, err)
				cur = &dec
			}
			assert.Equal(t, 3, pages)
			assert.Len(t, got, 25)
			assert.Equal(t, ids(all), got, "paged order must equal single-query order: no duplicates, no gaps")
		})
	}
}
