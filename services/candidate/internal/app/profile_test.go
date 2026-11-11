package app_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

var (
	u1 = uuid.MustParse("11111111-1111-1111-1111-111111111111")
	u2 = uuid.MustParse("22222222-2222-2222-2222-222222222222")
)

type env struct {
	svc      *app.Service
	clock    *fakeClock
	cands    *fakeCandidates
	cvs      *fakeCVs
	storage  *fakeStorage
	ob       *fakeOutbox
	idem     *fakeIdem
	attempts *attemptSeq
	logs     *bytes.Buffer
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{
		clock:    newClock(),
		cands:    &fakeCandidates{m: map[uuid.UUID]domain.Candidate{}},
		cvs:      &fakeCVs{m: map[uuid.UUID]domain.CV{}},
		storage:  &fakeStorage{objs: map[string]object{}},
		ob:       &fakeOutbox{},
		idem:     &fakeIdem{seen: map[string]bool{}},
		attempts: &attemptSeq{},
		logs:     &bytes.Buffer{},
	}
	e.svc = app.NewService(app.Deps{
		Candidates:   e.cands,
		CVs:          e.cvs,
		Storage:      e.storage,
		Outbox:       e.ob,
		Idem:         e.idem,
		Tx:           fakeTx{cands: e.cands, cvs: e.cvs, ob: e.ob, idem: e.idem},
		Schema:       schemaValidator{},
		Now:          e.clock.Now,
		NewAttemptID: e.attempts.Next,
		Logger:       slog.New(slog.NewJSONHandler(e.logs, nil)),
	})
	return e
}

func asCandidate(id uuid.UUID) context.Context {
	return authz.WithClaims(context.Background(), authz.Claims{Subject: id.String(), Role: authz.RoleCandidate})
}

func asRecruiter(id uuid.UUID) context.Context {
	return authz.WithClaims(context.Background(), authz.Claims{Subject: id.String(), Role: authz.RoleRecruiter})
}

func TestRoles(t *testing.T) {
	e := newEnv(t)
	id := uuid.New()
	calls := map[string]func(ctx context.Context) error{
		"GetProfile": func(ctx context.Context) error { _, err := e.svc.GetProfile(ctx); return err },
		"UpsertProfile": func(ctx context.Context) error {
			_, err := e.svc.UpsertProfile(ctx, domain.ProfileInput{FullName: "An"})
			return err
		},
		"CreateUploadURL": func(ctx context.Context) error {
			_, err := e.svc.CreateUploadURL(ctx, true, "cv.pdf")
			return err
		},
		"CompleteUpload": func(ctx context.Context) error { _, err := e.svc.CompleteUpload(ctx, id); return err },
		"GetCV":          func(ctx context.Context) error { _, err := e.svc.GetCV(ctx, id); return err },
		"ListMyCVs":      func(ctx context.Context) error { _, err := e.svc.ListMyCVs(ctx, app.PageRequest{}); return err },
		"UpdateParsedData": func(ctx context.Context) error {
			_, err := e.svc.UpdateParsedData(ctx, id, domain.ParsedCV{})
			return err
		},
		"ReparseCV": func(ctx context.Context) error { _, err := e.svc.ReparseCV(ctx, id); return err },
	}
	badSubject := authz.WithClaims(context.Background(), authz.Claims{Subject: "not-a-uuid", Role: authz.RoleCandidate})
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			assert.ErrorIs(t, call(context.Background()), apperr.ErrUnauthenticated)
			assert.ErrorIs(t, call(asRecruiter(u1)), apperr.ErrForbidden)
			assert.ErrorIs(t, call(badSubject), apperr.ErrUnauthenticated)
		})
	}
	assert.Empty(t, e.cands.m)
	assert.Empty(t, e.cvs.m)
	assert.Zero(t, e.ob.count())
}

func TestProfile_LazyCreate(t *testing.T) {
	e := newEnv(t)
	_, err := e.svc.GetProfile(asCandidate(u1))
	assert.ErrorIs(t, err, apperr.ErrNotFound)

	p, err := e.svc.UpsertProfile(asCandidate(u1), domain.ProfileInput{FullName: "An"})
	require.NoError(t, err)
	assert.Equal(t, u1, p.UserID)
	created := p.CreatedAt

	e.clock.Advance(time.Hour)
	p, err = e.svc.UpsertProfile(asCandidate(u1), domain.ProfileInput{FullName: "Bình"})
	require.NoError(t, err)
	assert.Equal(t, created, p.CreatedAt)
	assert.Equal(t, "Bình", p.FullName)
	assert.True(t, p.UpdatedAt.After(created))

	got, err := e.svc.GetProfile(asCandidate(u1))
	require.NoError(t, err)
	assert.Equal(t, "Bình", got.FullName)

	_, err = e.svc.GetProfile(asCandidate(u2))
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	assert.Zero(t, e.ob.count())
}

func TestProfile_Validation(t *testing.T) {
	e := newEnv(t)
	_, err := e.svc.UpsertProfile(asCandidate(u1), domain.ProfileInput{FullName: "  "})
	assert.ErrorIs(t, err, apperr.ErrValidation)
	assert.Empty(t, e.cands.m)
}
