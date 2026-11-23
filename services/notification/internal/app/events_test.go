package app_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/notification/internal/app"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/domain"
)

func TestHandleEvent_Recipients(t *testing.T) {
	cand, rec, job, app1, cv := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	cases := []struct {
		name      string
		typ       string
		payload   map[string]any
		user      uuid.UUID
		kind      domain.Type
		wantEmail bool
	}{
		{"welcome", "user.registered", map[string]any{"user_id": cand.String(), "email": "c@x.local", "role": "candidate"},
			cand, domain.TypeWelcome, true},
		{"cv parsed", "cv.parsed", map[string]any{"cv_id": cv.String(), "candidate_id": cand.String(), "attempt_id": "a", "parsed": map[string]any{}, "model_version": "m"},
			cand, domain.TypeCVParsed, false},
		{"cv failed", "cv.parse_failed", map[string]any{"cv_id": cv.String(), "candidate_id": cand.String(), "attempt_id": "a", "reason": "no_text", "detail": ""},
			cand, domain.TypeCVParseFailed, false},
		{"application received", "application.submitted", map[string]any{"application_id": app1.String(), "job_id": job.String(), "candidate_id": cand.String(), "recruiter_id": rec.String(), "cv_id": cv.String()},
			rec, domain.TypeApplicationReceived, true},
		{"status changed", "application.status_changed", map[string]any{"application_id": app1.String(), "job_id": job.String(), "candidate_id": cand.String(), "recruiter_id": rec.String(), "from": "SCREENED", "to": "INTERVIEW", "reason": nil, "score": nil, "version": 4, "actor": rec.String()},
			cand, domain.TypeApplicationStatus, true},
		{"matched", "match.completed", map[string]any{"job_id": job.String(), "model_version": "m", "top": []any{map[string]any{"candidate_id": cand.String(), "cv_id": cv.String(), "score": 80}}},
			rec, domain.TypeCandidatesMatched, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			e.users.m[cand] = domain.User{ID: cand, Email: "c@x.local", Role: "candidate"}
			e.users.m[rec] = domain.User{ID: rec, Email: "r@x.local", Role: "recruiter"}
			e.jobs.m[job] = domain.Job{ID: job, RecruiterID: rec, Title: "Golang dev", Version: 1}
			e.handle(t, c.typ, c.payload)

			require.Len(t, e.notes.list, 1)
			n := e.notes.list[0]
			assert.Equal(t, c.user, n.UserID)
			assert.Equal(t, c.kind, n.Type)
			assert.NotEmpty(t, n.Title)
			assert.NotContains(t, n.Title+n.Body, "{{")
			assert.NotContains(t, n.Title+n.Body, "<no value>")
			assert.Equal(t, t0, n.CreatedAt)
			if c.wantEmail {
				em := e.emails.only(t)
				assert.Equal(t, n.ID, em.NotificationID)
				assert.Equal(t, domain.EmailPending, em.Status)
				assert.Equal(t, t0, em.NextAttemptAt)
				assert.Equal(t, n.Title, em.Subject)
				assert.Contains(t, em.HTML, "<html")
				assert.NotEmpty(t, em.Text)
			} else {
				assert.Empty(t, e.emails.m)
			}
			require.Len(t, e.pusher.got, 1)
			assert.Equal(t, c.user, e.pusher.got[0].user)
		})
	}
}

func TestHandleEvent_TitlesNameTheJob(t *testing.T) {
	e := newEnv(t)
	rec, job := uuid.New(), uuid.New()
	e.jobs.m[job] = domain.Job{ID: job, RecruiterID: rec, Title: "Golang dev", Version: 1}
	e.handle(t, "application.submitted", map[string]any{"application_id": uuid.NewString(), "job_id": job.String(),
		"candidate_id": uuid.NewString(), "recruiter_id": rec.String(), "cv_id": uuid.NewString()})
	n := e.notes.list[0]
	assert.Contains(t, n.Title, "Golang dev")
	assert.Equal(t, job.String(), n.Data["job_id"])
	assert.NotEmpty(t, n.Data["application_id"])
}

func TestHandleEvent_DuplicateIgnored(t *testing.T) {
	e := newEnv(t)
	raw := evt(t, "user.registered", map[string]any{"user_id": uuid.NewString(), "email": "a@x.local", "role": "candidate"})
	require.NoError(t, e.svc.HandleEvent(context.Background(), raw))
	require.NoError(t, e.svc.HandleEvent(context.Background(), raw))
	assert.Len(t, e.notes.list, 1)
	assert.Len(t, e.emails.m, 1)
	assert.Len(t, e.pusher.got, 1)
}

func TestHandleEvent_StatusChangeSkipsSystemAndInternalSteps(t *testing.T) {
	cand := uuid.New()
	base := func(to, actor string) map[string]any {
		return map[string]any{"application_id": uuid.NewString(), "job_id": uuid.NewString(), "candidate_id": cand.String(),
			"recruiter_id": uuid.NewString(), "from": "PENDING", "to": to, "reason": nil, "score": nil, "version": 2, "actor": actor}
	}
	e := newEnv(t)
	for _, p := range []map[string]any{base("SCREENING", "system"), base("SCREENED", "system"),
		base("CANCELLED", "system"), base("NEEDS_MANUAL_REVIEW", "system"), base("REJECTED", "system")} {
		e.handle(t, "application.status_changed", p)
	}
	assert.Empty(t, e.notes.list)
	for _, to := range []string{"INTERVIEW", "OFFERED", "HIRED", "REJECTED"} {
		e.handle(t, "application.status_changed", base(to, uuid.NewString()))
	}
	assert.Len(t, e.notes.list, 4)
}

func TestHandleEvent_MatchWithoutJobReplicaOrTopSkipped(t *testing.T) {
	e := newEnv(t)
	job := uuid.New()
	e.handle(t, "match.completed", map[string]any{"job_id": job.String(), "model_version": "m",
		"top": []any{map[string]any{"candidate_id": uuid.NewString(), "cv_id": uuid.NewString(), "score": 50}}})
	e.jobs.m[job] = domain.Job{ID: job, RecruiterID: uuid.New(), Title: "x", Version: 1}
	e.handle(t, "match.completed", map[string]any{"job_id": job.String(), "model_version": "m", "top": []any{}})
	assert.Empty(t, e.notes.list)
}

func TestHandleEvent_JobReplica(t *testing.T) {
	e := newEnv(t)
	job, rec := uuid.New(), uuid.New()
	payload := func(title string, v int) map[string]any {
		return map[string]any{"job_id": job.String(), "recruiter_id": rec.String(), "title": title, "version": v}
	}
	e.handle(t, "job.created", payload("v1", 1))
	e.handle(t, "job.updated", payload("v3", 3))
	e.handle(t, "job.updated", payload("v2 late", 2))
	assert.Equal(t, "v3", e.jobs.m[job].Title)
	assert.Equal(t, rec, e.jobs.m[job].RecruiterID)
	e.handle(t, "job.deleted", map[string]any{"job_id": job.String(), "version": 4})
	assert.True(t, e.jobs.m[job].Deleted)
	e.handle(t, "job.updated", payload("late after delete", 3))
	assert.True(t, e.jobs.m[job].Deleted, "a late update does not resurrect a deleted job")
	assert.Empty(t, e.notes.list)
}

func TestHandleEvent_NoEmailWithoutAddress(t *testing.T) {
	e := newEnv(t) // recruiter registered before M4a: no replica row
	e.handle(t, "application.submitted", map[string]any{"application_id": uuid.NewString(), "job_id": uuid.NewString(),
		"candidate_id": uuid.NewString(), "recruiter_id": uuid.NewString(), "cv_id": uuid.NewString()})
	require.Len(t, e.notes.list, 1, "in-app notification still created")
	assert.Empty(t, e.emails.m)
	assert.NotContains(t, e.notes.list[0].Title, "<no value>")
}

func TestHandleEvent_PushOnlyAfterCommit(t *testing.T) {
	e := newEnv(t)
	e.txErr = errBoom
	err := e.svc.HandleEvent(context.Background(), evt(t, "user.registered",
		map[string]any{"user_id": uuid.NewString(), "email": "a@x.local", "role": "candidate"}))
	require.Error(t, err)
	assert.Empty(t, e.pusher.got)
}

func TestHandleEvent_PoisonAndUnknown(t *testing.T) {
	e := newEnv(t)
	assert.ErrorIs(t, e.svc.HandleEvent(context.Background(), []byte("{")), app.ErrPoisonEvent)
	err := e.svc.HandleEvent(context.Background(), evt(t, "cv.parsed", map[string]any{"cv_id": "x", "candidate_id": "nope"}))
	assert.ErrorIs(t, err, app.ErrPoisonEvent)
	require.NoError(t, e.svc.HandleEvent(context.Background(), evt(t, "job.closed", map[string]any{"job_id": uuid.NewString()})))
	assert.Empty(t, e.notes.list)
}

func TestHandleEvent_ErrorsCarryNoEmail(t *testing.T) {
	e := newEnv(t)
	err := e.svc.HandleEvent(context.Background(), evt(t, "user.registered",
		map[string]any{"user_id": "not-a-uuid", "email": "secret@x.local", "role": "candidate"}))
	require.ErrorIs(t, err, app.ErrPoisonEvent)
	assert.False(t, strings.Contains(err.Error(), "secret@x.local"))
}
