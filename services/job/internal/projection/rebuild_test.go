package projection_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
	"github.com/nuroqzavitex/smarthire/services/job/internal/projection"
)

type fakeSource struct {
	jobs []domain.Job // ordered by ID
	open map[uuid.UUID]int64
}

func (f *fakeSource) ScanAll(_ context.Context, afterID uuid.UUID, limit int) ([]domain.Job, error) {
	var out []domain.Job
	for _, j := range f.jobs {
		if j.ID.String() > afterID.String() {
			out = append(out, j)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func (f *fakeSource) OpenVersions(context.Context) (map[uuid.UUID]int64, error) { return f.open, nil }

type fakeAdmin struct {
	indices []string
	calls   []string
	open    map[uuid.UUID]int64
}

func (f *fakeAdmin) Indices(context.Context) ([]string, error) { return f.indices, nil }
func (f *fakeAdmin) CreateIndex(_ context.Context, name string) error {
	f.calls = append(f.calls, "create "+name)
	return nil
}
func (f *fakeAdmin) AddToWriteAlias(_ context.Context, index string) error {
	f.calls = append(f.calls, "write-alias "+index)
	return nil
}
func (f *fakeAdmin) SwapAliases(_ context.Context, newIndex string, old []string) error {
	f.calls = append(f.calls, fmt.Sprintf("swap %s %v", newIndex, old))
	return nil
}
func (f *fakeAdmin) DeleteIndex(_ context.Context, name string) error {
	f.calls = append(f.calls, "delete "+name)
	return nil
}
func (f *fakeAdmin) BulkPut(_ context.Context, index string, docs []projection.Document) error {
	f.calls = append(f.calls, fmt.Sprintf("bulk %s %d", index, len(docs)))
	return nil
}
func (f *fakeAdmin) OpenVersions(context.Context) (map[uuid.UUID]int64, error) { return f.open, nil }

func sortedJobs(n int) []domain.Job {
	out := make([]domain.Job, n)
	for i := range out {
		out[i] = domain.Job{ID: uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1)), Version: 1, Status: domain.StatusOpen}
	}
	return out
}

func TestRebuild_Sequence(t *testing.T) {
	admin := &fakeAdmin{indices: []string{"jobs_v1", "jobs_v3"}}
	var slept []time.Duration
	opts := projection.RebuildOptions{AliasTTL: 5 * time.Second, Batch: 2, Sleep: func(d time.Duration) { slept = append(slept, d) }}
	name, err := projection.Rebuild(context.Background(), &fakeSource{jobs: sortedJobs(5)}, admin, opts)
	require.NoError(t, err)
	assert.Equal(t, "jobs_v4", name)
	assert.Equal(t, []string{
		"create jobs_v4", "write-alias jobs_v4",
		"bulk jobs_v4 2", "bulk jobs_v4 2", "bulk jobs_v4 1",
		"swap jobs_v4 [jobs_v1 jobs_v3]", "delete jobs_v1", "delete jobs_v3",
	}, admin.calls)
	assert.Equal(t, []time.Duration{7 * time.Second, 7 * time.Second}, slept,
		"waits AliasTTL + 2s before reading postgres and again before deleting old indices")

	admin = &fakeAdmin{indices: []string{"jobs_v1"}}
	opts.KeepOld = true
	slept = nil
	_, err = projection.Rebuild(context.Background(), &fakeSource{jobs: sortedJobs(1)}, admin, opts)
	require.NoError(t, err)
	assert.NotContains(t, admin.calls, "delete jobs_v1")
	assert.Len(t, slept, 1, "no second wait when old indices are kept")
}

func TestReconcile_Diff(t *testing.T) {
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	src := &fakeSource{open: map[uuid.UUID]int64{a: 1, b: 2}}
	ok, err := projection.Reconcile(context.Background(), src, &fakeAdmin{open: map[uuid.UUID]int64{a: 1, b: 2}})
	require.NoError(t, err)
	assert.True(t, ok.OK())
	assert.Equal(t, 2, ok.PGOpen)

	r, err := projection.Reconcile(context.Background(), src, &fakeAdmin{open: map[uuid.UUID]int64{a: 1, b: 1, c: 5}})
	require.NoError(t, err)
	assert.False(t, r.OK())
	assert.Equal(t, 2, r.PGOpen)
	assert.Equal(t, 3, r.IndexOpen)
	assert.ElementsMatch(t, []string{b.String(), c.String()}, r.Diff)
}

func TestFromJob_SoftDeletedIsTombstone(t *testing.T) {
	now := time.Now()
	j := domain.Job{ID: uuid.New(), Version: 9, Status: domain.StatusClosed, Title: "x", DeletedAt: &now}
	assert.Equal(t, projection.Document{JobID: j.ID.String(), Version: 9, Status: projection.StatusDeleted}, projection.FromJob(j))
}
