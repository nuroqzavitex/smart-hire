package grpc_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	applicationv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/application/v1"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/authz/authztest"
	"github.com/nuroqzavitex/smarthire/pkg/grpcx"
	appgrpc "github.com/nuroqzavitex/smarthire/services/application/internal/adapter/grpc"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

type fakeUC struct {
	a       domain.Application
	hist    []domain.HistoryEntry
	list    []domain.Application
	next    string
	err     error
	submit  app.SubmitInput
	listIn  app.ListInput
	id      uuid.UUID
	to      domain.Status
	callsTo int
}

func (f *fakeUC) Submit(_ context.Context, in app.SubmitInput) (domain.Application, error) {
	f.callsTo++
	f.submit = in
	return f.a, f.err
}

func (f *fakeUC) Get(_ context.Context, id uuid.UUID) (domain.Application, []domain.HistoryEntry, error) {
	f.callsTo++
	f.id = id
	return f.a, f.hist, f.err
}

func (f *fakeUC) List(_ context.Context, in app.ListInput) ([]domain.Application, string, error) {
	f.callsTo++
	f.listIn = in
	return f.list, f.next, f.err
}

func (f *fakeUC) UpdateStatus(_ context.Context, id uuid.UUID, to domain.Status) (domain.Application, error) {
	f.callsTo++
	f.id, f.to = id, to
	return f.a, f.err
}

type env struct {
	client applicationv1.ApplicationServiceClient
	issuer *authztest.KeyIssuer
	uc     *fakeUC
}

func newEnv(t *testing.T) *env {
	t.Helper()
	is := authztest.NewIssuer(t)
	v, err := authz.NewVerifier(t.Context(), is.JWKSURL(), authztest.Issuer, authztest.Audience)
	require.NoError(t, err)
	t.Cleanup(v.Close)
	uc := &fakeUC{}
	srv, _ := grpcx.NewServer(grpcx.ServerConfig{
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:      v,
		PublicMethods: appgrpc.PublicMethods,
	})
	applicationv1.RegisterApplicationServiceServer(srv, appgrpc.NewHandler(uc))
	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpcx.Dial("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return &env{client: applicationv1.NewApplicationServiceClient(conn), issuer: is, uc: uc}
}

func (e *env) ctx(t *testing.T, kv ...string) context.Context {
	t.Helper()
	tok := e.issuer.Sign(t, uuid.NewString(), authz.RoleCandidate, time.Minute)
	return metadata.AppendToOutgoingContext(context.Background(), append([]string{"authorization", "Bearer " + tok}, kv...)...)
}

var t0 = time.Date(2026, 11, 23, 9, 0, 0, 0, time.UTC)

func sample() domain.Application {
	score := 87.5
	return domain.Application{
		ID: uuid.New(), JobID: uuid.New(), CandidateID: uuid.New(), RecruiterID: uuid.New(), CVID: uuid.New(),
		Status: domain.StatusNeedsManualReview, Reason: domain.ReasonAIUnavailable, Score: &score,
		Breakdown:    json.RawMessage(`{"semantic_similarity":0.8,"required_skill_coverage":1,"experience_fit":0.5,"location_fit":1,"matched_skills":["Go"],"missing_skills":["Rust"]}`),
		ModelVersion: "fake-hash@768", Version: 4, CreatedAt: t0, UpdatedAt: t0.Add(time.Minute),
	}
}

func TestHandler_RequiresToken(t *testing.T) {
	e := newEnv(t)
	assert.Empty(t, appgrpc.PublicMethods)
	_, err := e.client.ListApplications(context.Background(), &applicationv1.ListApplicationsRequest{})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.Zero(t, e.uc.callsTo)
}

func TestHandler_SubmitReadsIdempotencyKey(t *testing.T) {
	e := newEnv(t)
	e.uc.a = sample()
	job, cv := uuid.New(), uuid.New()
	got, err := e.client.SubmitApplication(e.ctx(t, "idempotency-key", "abc-123"),
		&applicationv1.SubmitApplicationRequest{JobId: job.String(), CvId: cv.String()})
	require.NoError(t, err)
	assert.Equal(t, app.SubmitInput{JobID: job, CVID: cv, IdempotencyKey: "abc-123"}, e.uc.submit)
	assert.Equal(t, e.uc.a.ID.String(), got.GetId())

	_, err = e.client.SubmitApplication(e.ctx(t),
		&applicationv1.SubmitApplicationRequest{JobId: job.String(), CvId: cv.String()})
	require.NoError(t, err, "a missing key reaches the use case, which rejects it")
	assert.Empty(t, e.uc.submit.IdempotencyKey)
}

func TestHandler_MapsApplication(t *testing.T) {
	e := newEnv(t)
	e.uc.a = sample()
	from := domain.StatusScreening
	e.uc.hist = []domain.HistoryEntry{
		{To: domain.StatusPending, Actor: "u1", At: t0},
		{From: &from, To: domain.StatusNeedsManualReview, Actor: domain.ActorSystem, Reason: domain.ReasonAIUnavailable, At: t0.Add(time.Second)},
	}
	resp, err := e.client.GetApplication(e.ctx(t), &applicationv1.GetApplicationRequest{Id: e.uc.a.ID.String()})
	require.NoError(t, err)
	a := resp.GetApplication()
	assert.Equal(t, applicationv1.ApplicationStatus_APPLICATION_STATUS_NEEDS_MANUAL_REVIEW, a.GetStatus())
	assert.Equal(t, "ai_unavailable", a.GetReason())
	assert.InDelta(t, 87.5, a.GetScore(), 1e-9)
	assert.InDelta(t, 0.8, a.GetBreakdown().GetSemanticSimilarity(), 1e-9)
	assert.Equal(t, []string{"Go"}, a.GetBreakdown().GetMatchedSkills())
	assert.Equal(t, []string{"Rust"}, a.GetBreakdown().GetMissingSkills())
	assert.Equal(t, "fake-hash@768", a.GetModelVersion())
	assert.Equal(t, int64(4), a.GetVersion())
	assert.Equal(t, e.uc.a.CVID.String(), a.GetCvId())
	assert.True(t, a.GetUpdatedAt().AsTime().Equal(t0.Add(time.Minute)))

	h := resp.GetHistory()
	require.Len(t, h, 2)
	assert.Equal(t, applicationv1.ApplicationStatus_APPLICATION_STATUS_UNSPECIFIED, h[0].GetFrom())
	assert.Equal(t, applicationv1.ApplicationStatus_APPLICATION_STATUS_PENDING, h[0].GetTo())
	assert.Equal(t, applicationv1.ApplicationStatus_APPLICATION_STATUS_SCREENING, h[1].GetFrom())
	assert.Equal(t, "system", h[1].GetActor())
	assert.Equal(t, "ai_unavailable", h[1].GetReason())
}

func TestHandler_NoScoreLeavesFieldsEmpty(t *testing.T) {
	e := newEnv(t)
	a := sample()
	a.Score, a.Breakdown = nil, nil
	e.uc.a = a
	resp, err := e.client.GetApplication(e.ctx(t), &applicationv1.GetApplicationRequest{Id: a.ID.String()})
	require.NoError(t, err)
	assert.Nil(t, resp.GetApplication().Score)
	assert.Nil(t, resp.GetApplication().GetBreakdown())
}

func TestHandler_ListPassesFilters(t *testing.T) {
	e := newEnv(t)
	e.uc.list, e.uc.next = []domain.Application{sample()}, "tok2"
	job := uuid.New()
	resp, err := e.client.ListApplications(e.ctx(t), &applicationv1.ListApplicationsRequest{
		JobId: job.String(), Status: applicationv1.ApplicationStatus_APPLICATION_STATUS_SCREENED, PageSize: 5, PageToken: "tok",
	})
	require.NoError(t, err)
	require.NotNil(t, e.uc.listIn.JobID)
	assert.Equal(t, job, *e.uc.listIn.JobID)
	require.NotNil(t, e.uc.listIn.Status)
	assert.Equal(t, domain.StatusScreened, *e.uc.listIn.Status)
	assert.Equal(t, 5, e.uc.listIn.PageSize)
	assert.Equal(t, "tok", e.uc.listIn.PageToken)
	assert.Len(t, resp.GetApplications(), 1)
	assert.Equal(t, "tok2", resp.GetNextPageToken())

	_, err = e.client.ListApplications(e.ctx(t), &applicationv1.ListApplicationsRequest{})
	require.NoError(t, err)
	assert.Nil(t, e.uc.listIn.JobID)
	assert.Nil(t, e.uc.listIn.Status)
}

func TestHandler_UpdateStatus(t *testing.T) {
	e := newEnv(t)
	e.uc.a = sample()
	id := uuid.New()
	_, err := e.client.UpdateApplicationStatus(e.ctx(t), &applicationv1.UpdateApplicationStatusRequest{
		Id: id.String(), Status: applicationv1.ApplicationStatus_APPLICATION_STATUS_INTERVIEW,
	})
	require.NoError(t, err)
	assert.Equal(t, id, e.uc.id)
	assert.Equal(t, domain.StatusInterview, e.uc.to)

	_, err = e.client.UpdateApplicationStatus(e.ctx(t), &applicationv1.UpdateApplicationStatusRequest{Id: id.String()})
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "unspecified status rejected by validation")
}
