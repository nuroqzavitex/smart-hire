package app_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

var (
	r1 = uuid.MustParse("11111111-1111-1111-1111-111111111111")
	r2 = uuid.MustParse("22222222-2222-2222-2222-222222222222")
)

type env struct {
	svc  *app.Service
	jobs *fakeJobs
	ob   *fakeOutbox
}

func newEnv(t *testing.T, mod ...func(*app.Deps, *fakeJobs)) env {
	t.Helper()
	jobs, ob := newFakeJobs(), &fakeOutbox{}
	d := app.Deps{Jobs: jobs, Outbox: ob, Tx: fakeTx{jobs, ob}, Now: tickClock()}
	for _, m := range mod {
		m(&d, jobs)
	}
	return env{svc: app.NewService(d), jobs: jobs, ob: ob}
}

func asRecruiter(id uuid.UUID) context.Context {
	return authz.WithClaims(context.Background(), authz.Claims{Subject: id.String(), Role: authz.RoleRecruiter})
}

func asCandidate() context.Context {
	return authz.WithClaims(context.Background(), authz.Claims{Subject: uuid.NewString(), Role: authz.RoleCandidate})
}

func validDraft() domain.Draft {
	return domain.Draft{
		Title: "Senior Go Engineer", Description: "Build and run backend services in Go.",
		RequiredSkills: []string{"Go"}, Level: domain.LevelSenior, Location: "Hanoi",
	}
}

func (e env) create(t *testing.T, owner uuid.UUID) domain.Job {
	t.Helper()
	j, err := e.svc.Create(asRecruiter(owner), validDraft())
	require.NoError(t, err)
	return j
}

func TestCreate_RecruiterOnly(t *testing.T) {
	e := newEnv(t)
	_, err := e.svc.Create(context.Background(), validDraft())
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
	_, err = e.svc.Create(asCandidate(), validDraft())
	assert.ErrorIs(t, err, apperr.ErrForbidden)
	assert.Zero(t, e.ob.count())

	j, err := e.svc.Create(asRecruiter(r1), validDraft())
	require.NoError(t, err)
	assert.Equal(t, r1, j.RecruiterID)
	ev := e.ob.last()
	assert.Equal(t, "job.created", ev.EventType)
	assert.Equal(t, "job-service", ev.Source)
	assert.Equal(t, 1, ev.EventVersion)
	assert.Equal(t, j.ID.String(), e.ob.lastKey())
	assert.JSONEq(t, `{"job_id":"`+j.ID.String()+`","recruiter_id":"`+r1.String()+`",
		"title":"Senior Go Engineer","description":"Build and run backend services in Go.",
		"required_skills":["Go"],"nice_to_have_skills":[],"level":"senior","location":"Hanoi",
		"salary_min":null,"salary_max":null,"status":"draft","version":1}`, string(ev.Payload))
}

func TestCreate_InvalidDraftWritesNothing(t *testing.T) {
	e := newEnv(t)
	d := validDraft()
	d.Title = "x"
	_, err := e.svc.Create(asRecruiter(r1), d)
	assert.ErrorIs(t, err, apperr.ErrValidation)
	assert.Zero(t, e.ob.count())
	assert.Empty(t, e.jobs.m)
}

func TestCommands_EventTypesAndVersion(t *testing.T) {
	e := newEnv(t)
	ctx := asRecruiter(r1)
	j := e.create(t, r1)
	assert.Equal(t, int64(1), j.Version)

	title := "Staff Go Engineer"
	j, err := e.svc.Update(ctx, j.ID, domain.Patch{Title: &title})
	require.NoError(t, err)
	assert.Equal(t, int64(2), j.Version)
	assert.Equal(t, "job.updated", e.ob.last().EventType)

	j, err = e.svc.Publish(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(3), j.Version)
	assert.Equal(t, "job.updated", e.ob.last().EventType)
	assert.Contains(t, string(e.ob.last().Payload), `"status":"open"`)

	j, err = e.svc.Close(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(4), j.Version)
	assert.Equal(t, "job.closed", e.ob.last().EventType)
	assert.JSONEq(t, `{"job_id":"`+j.ID.String()+`","version":4}`, string(e.ob.last().Payload))

	require.NoError(t, e.svc.Delete(ctx, j.ID))
	assert.Equal(t, "job.deleted", e.ob.last().EventType)
	assert.JSONEq(t, `{"job_id":"`+j.ID.String()+`","version":5}`, string(e.ob.last().Payload))
	assert.Equal(t, 5, e.ob.count())
	for _, k := range e.ob.keys {
		assert.Equal(t, j.ID.String(), k)
	}
	for i := 1; i < e.ob.count(); i++ {
		assert.True(t, e.ob.evs[i].OccurredAt.After(e.ob.evs[i-1].OccurredAt))
	}
}

func TestCommands_NonOwnerGetsNotFound(t *testing.T) {
	e := newEnv(t)
	j := e.create(t, r1)
	n := e.ob.count()
	ctx := asRecruiter(r2)
	title := "Hijacked title"
	_, err := e.svc.Update(ctx, j.ID, domain.Patch{Title: &title})
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	_, err = e.svc.Publish(ctx, j.ID)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	_, err = e.svc.Close(ctx, j.ID)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	assert.ErrorIs(t, e.svc.Delete(ctx, j.ID), apperr.ErrNotFound)
	assert.Equal(t, n, e.ob.count())
	assert.Equal(t, int64(1), e.jobs.m[j.ID].Version)
	assert.Nil(t, e.jobs.m[j.ID].DeletedAt)
}

func TestCommands_UnknownJobAndAuth(t *testing.T) {
	e := newEnv(t)
	_, err := e.svc.Publish(asRecruiter(r1), uuid.New())
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	_, err = e.svc.Publish(context.Background(), uuid.New())
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
	_, err = e.svc.Publish(asCandidate(), uuid.New())
	assert.ErrorIs(t, err, apperr.ErrForbidden)
}

func TestCommands_DeletedJobIsNotFound(t *testing.T) {
	e := newEnv(t)
	ctx := asRecruiter(r1)
	j := e.create(t, r1)
	require.NoError(t, e.svc.Delete(ctx, j.ID))
	n, ver := e.ob.count(), e.jobs.m[j.ID].Version

	title := "Resurrected title"
	_, err := e.svc.Update(ctx, j.ID, domain.Patch{Title: &title})
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	_, err = e.svc.Update(ctx, j.ID, domain.Patch{})
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	_, err = e.svc.Publish(ctx, j.ID)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	_, err = e.svc.Close(ctx, j.ID)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	assert.ErrorIs(t, e.svc.Delete(ctx, j.ID), apperr.ErrNotFound)
	assert.Equal(t, n, e.ob.count())
	assert.Equal(t, ver, e.jobs.m[j.ID].Version)
}

func TestCommands_ClockReadAfterRowLock(t *testing.T) {
	var locksAtNow []int
	e := newEnv(t, func(d *app.Deps, jobs *fakeJobs) {
		inner := d.Now
		d.Now = func() time.Time {
			locksAtNow = append(locksAtNow, jobs.lock)
			return inner()
		}
	})
	j := e.create(t, r1)
	locksAtNow = nil
	_, err := e.svc.Publish(asRecruiter(r1), j.ID)
	require.NoError(t, err)
	assert.Equal(t, []int{1}, locksAtNow, "Now must be read once, after GetForUpdate")
}

func TestUpdate_EmptyPatchIsNoop(t *testing.T) {
	e := newEnv(t)
	j := e.create(t, r1)
	n := e.ob.count()
	got, err := e.svc.Update(asRecruiter(r1), j.ID, domain.Patch{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), got.Version)
	assert.Equal(t, n, e.ob.count())
}

func TestUpdate_InvalidPatchRollsBack(t *testing.T) {
	e := newEnv(t)
	j := e.create(t, r1)
	n := e.ob.count()
	bad := "x"
	_, err := e.svc.Update(asRecruiter(r1), j.ID, domain.Patch{Title: &bad})
	assert.ErrorIs(t, err, apperr.ErrValidation)
	assert.Equal(t, n, e.ob.count())
	assert.Equal(t, int64(1), e.jobs.m[j.ID].Version)
}

func TestDelete_OpenRejected(t *testing.T) {
	e := newEnv(t)
	ctx := asRecruiter(r1)
	j := e.create(t, r1)
	_, err := e.svc.Publish(ctx, j.ID)
	require.NoError(t, err)
	assert.ErrorIs(t, e.svc.Delete(ctx, j.ID), apperr.ErrValidation)
	assert.Nil(t, e.jobs.m[j.ID].DeletedAt)
}

func TestCommand_RollsBackOnOutboxError(t *testing.T) {
	e := newEnv(t)
	j := e.create(t, r1)
	n := e.ob.count()
	e.ob.err = errBoom
	_, err := e.svc.Publish(asRecruiter(r1), j.ID)
	assert.ErrorIs(t, err, errBoom)
	assert.Equal(t, domain.StatusDraft, e.jobs.m[j.ID].Status)
	assert.Equal(t, int64(1), e.jobs.m[j.ID].Version)
	assert.Equal(t, n, e.ob.count())

	_, err = e.svc.Create(asRecruiter(r1), validDraft())
	assert.ErrorIs(t, err, errBoom)
	assert.Len(t, e.jobs.m, 1)
}
