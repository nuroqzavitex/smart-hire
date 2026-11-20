package projection

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

// IndexPrefix names physical read-model indices: jobs_v1, jobs_v2, ...
const IndexPrefix = "jobs_v"

// JobSource reads the write model (Postgres) for rebuild and reconcile.
type JobSource interface {
	// ScanAll pages through every job, soft-deleted included, by ID.
	ScanAll(ctx context.Context, afterID uuid.UUID, limit int) ([]domain.Job, error)
	// OpenVersions maps every open, non-deleted job to its version.
	OpenVersions(ctx context.Context) (map[uuid.UUID]int64, error)
}

// IndexAdmin manages read-model indices and aliases.
type IndexAdmin interface {
	// Indices lists the physical jobs_v* indices.
	Indices(ctx context.Context) ([]string, error)
	CreateIndex(ctx context.Context, name string) error
	// AddToWriteAlias attaches index to the write alias, keeping the others.
	AddToWriteAlias(ctx context.Context, index string) error
	// SwapAliases points the read alias at newIndex and leaves only newIndex
	// behind the write alias, atomically.
	SwapAliases(ctx context.Context, newIndex string, old []string) error
	DeleteIndex(ctx context.Context, name string) error
	// BulkPut writes docs with external versions; stale ones are skipped.
	BulkPut(ctx context.Context, index string, docs []Document) error
	// OpenVersions maps every open document behind the read alias to its version.
	OpenVersions(ctx context.Context) (map[uuid.UUID]int64, error)
}

// RebuildOptions tune Rebuild.
type RebuildOptions struct {
	// AliasTTL is the projector's write-alias cache TTL; Rebuild waits
	// AliasTTL + 2s after attaching the new index before reading Postgres.
	AliasTTL time.Duration
	Batch    int
	KeepOld  bool
	Sleep    func(time.Duration)
}

// Rebuild builds a fresh index from Postgres and swaps the aliases to it
// without losing concurrent updates (ADR 0009):
//  1. create jobs_v{max+1};
//  2. attach it to the write alias, so running projectors write both;
//  3. wait until every projector has re-resolved the write alias;
//  4. copy every row with external versions (newer projector writes win);
//  5. swap the read alias and detach the old indices from the write alias;
//  6. wait again until every projector has dropped the old indices, then
//     delete them (unless KeepOld).
func Rebuild(ctx context.Context, src JobSource, idx IndexAdmin, o RebuildOptions) (string, error) {
	if o.Batch <= 0 {
		o.Batch = 500
	}
	if o.Sleep == nil {
		o.Sleep = time.Sleep
	}
	old, err := idx.Indices(ctx)
	if err != nil {
		return "", fmt.Errorf("list indices: %w", err)
	}
	name := nextIndex(old)
	if err := idx.CreateIndex(ctx, name); err != nil {
		return "", fmt.Errorf("create %s: %w", name, err)
	}
	if err := idx.AddToWriteAlias(ctx, name); err != nil {
		return "", fmt.Errorf("attach %s to write alias: %w", name, err)
	}
	o.Sleep(o.AliasTTL + 2*time.Second)
	var after uuid.UUID
	for {
		jobs, err := src.ScanAll(ctx, after, o.Batch)
		if err != nil {
			return "", fmt.Errorf("scan jobs: %w", err)
		}
		if len(jobs) == 0 {
			break
		}
		docs := make([]Document, len(jobs))
		for i, j := range jobs {
			docs[i] = FromJob(j)
		}
		if err := idx.BulkPut(ctx, name, docs); err != nil {
			return "", fmt.Errorf("bulk index into %s: %w", name, err)
		}
		after = jobs[len(jobs)-1].ID
		if len(jobs) < o.Batch {
			break
		}
	}
	if err := idx.SwapAliases(ctx, name, old); err != nil {
		return "", fmt.Errorf("swap aliases: %w", err)
	}
	if !o.KeepOld {
		// Projectors may still hold the old write alias in cache.
		o.Sleep(o.AliasTTL + 2*time.Second)
		for _, prev := range old {
			if err := idx.DeleteIndex(ctx, prev); err != nil {
				return "", fmt.Errorf("delete %s: %w", prev, err)
			}
		}
	}
	return name, nil
}

// nextIndex returns jobs_v{n+1} for the highest existing n.
func nextIndex(existing []string) string {
	highest := 0
	for _, e := range existing {
		if !strings.HasPrefix(e, IndexPrefix) {
			continue
		}
		if n, err := strconv.Atoi(strings.TrimPrefix(e, IndexPrefix)); err == nil && n > highest {
			highest = n
		}
	}
	return IndexPrefix + strconv.Itoa(highest+1)
}

// Report compares open jobs in Postgres with open documents in the read model.
type Report struct {
	PGOpen, IndexOpen int
	// Diff lists up to 20 job IDs missing on one side or with different versions.
	Diff []string
}

// OK reports whether both sides agree.
func (r Report) OK() bool { return r.PGOpen == r.IndexOpen && len(r.Diff) == 0 }

const maxDiff = 20

// Reconcile counts and compares open jobs on both sides.
func Reconcile(ctx context.Context, src JobSource, idx IndexAdmin) (Report, error) {
	pg, err := src.OpenVersions(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("postgres open jobs: %w", err)
	}
	ix, err := idx.OpenVersions(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("read model open jobs: %w", err)
	}
	r := Report{PGOpen: len(pg), IndexOpen: len(ix)}
	var diff []string
	for id, v := range pg {
		if iv, ok := ix[id]; !ok || iv != v {
			diff = append(diff, id.String())
		}
	}
	for id := range ix {
		if _, ok := pg[id]; !ok {
			diff = append(diff, id.String())
		}
	}
	sort.Strings(diff)
	if len(diff) > maxDiff {
		diff = diff[:maxDiff]
	}
	r.Diff = diff
	return r, nil
}
