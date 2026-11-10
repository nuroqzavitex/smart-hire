package grpc_test

import (
	"context"
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
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	jobv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/job/v1"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/authz/authztest"
	"github.com/nuroqzavitex/smarthire/pkg/grpcx"
	jobgrpc "github.com/nuroqzavitex/smarthire/services/job/internal/adapter/grpc"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

// fakeUC records the arguments of the last call and returns canned results.
type fakeUC struct {
	job     domain.Job
	page    app.Page
	err     error
	calls   int
	draft   domain.Draft
	patch   domain.Patch
	id      uuid.UUID
	filter  app.SearchFilter
	pageReq app.PageRequest
	status  *domain.Status
}

func (f *fakeUC) Create(_ context.Context, d domain.Draft) (domain.Job, error) {
	f.calls++
	f.draft = d
	return f.job, f.err
}

func (f *fakeUC) Update(_ context.Context, id uuid.UUID, p domain.Patch) (domain.Job, error) {
	f.calls++
	f.id, f.patch = id, p
	return f.job, f.err
}

func (f *fakeUC) Publish(_ context.Context, id uuid.UUID) (domain.Job, error) {
	f.calls++
	f.id = id
	return f.job, f.err
}

func (f *fakeUC) Close(_ context.Context, id uuid.UUID) (domain.Job, error) {
	f.calls++
	f.id = id
	return f.job, f.err
}

func (f *fakeUC) Delete(_ context.Context, id uuid.UUID) error {
	f.calls++
	f.id = id
	return f.err
}

func (f *fakeUC) Get(_ context.Context, id uuid.UUID) (domain.Job, error) {
	f.calls++
	f.id = id
	return f.job, f.err
}

func (f *fakeUC) ListOpen(_ context.Context, sf app.SearchFilter, pr app.PageRequest) (app.Page, error) {
	f.calls++
	f.filter, f.pageReq = sf, pr
	return f.page, f.err
}

func (f *fakeUC) ListMine(_ context.Context, st *domain.Status, pr app.PageRequest) (app.Page, error) {
	f.calls++
	f.status, f.pageReq = st, pr
	return f.page, f.err
}

type env struct {
	client jobv1.JobServiceClient
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
		PublicMethods: append([]string{grpc_health_v1.Health_Check_FullMethodName}, jobgrpc.PublicMethods...),
	})
	jobv1.RegisterJobServiceServer(srv, jobgrpc.NewHandler(uc))
	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpcx.Dial("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return &env{client: jobv1.NewJobServiceClient(conn), issuer: is, uc: uc}
}

func (e *env) recruiterCtx(t *testing.T) context.Context {
	t.Helper()
	tok := e.issuer.Sign(t, uuid.NewString(), authz.RoleRecruiter, time.Minute)
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+tok)
}

func code(err error) codes.Code { return status.Code(err) }

func validCreate() *jobv1.CreateJobRequest {
	return &jobv1.CreateJobRequest{
		Title: "Senior Go Engineer", Description: "Build and run backend services in Go.",
		RequiredSkills: []string{"go"}, Level: jobv1.JobLevel_JOB_LEVEL_SENIOR, Location: "Hanoi",
	}
}

func TestHandler_PublicMethodsNoToken(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	_, err := e.client.ListJobs(ctx, &jobv1.ListJobsRequest{})
	require.NoError(t, err)
	_, err = e.client.GetJob(ctx, &jobv1.GetJobRequest{Id: uuid.NewString()})
	require.NoError(t, err)

	before := e.uc.calls
	_, err = e.client.CreateJob(ctx, validCreate())
	assert.Equal(t, codes.Unauthenticated, code(err))
	assert.Equal(t, before, e.uc.calls, "use case must not run without a token")
}

func TestHandler_CreateMapsFields(t *testing.T) {
	e := newEnv(t)
	jid := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)
	smin := 1000
	e.uc.job = domain.Job{
		ID: jid, RecruiterID: uuid.New(), Title: "Senior Go Engineer", Level: domain.LevelSenior,
		Status: domain.StatusDraft, Version: 1, CreatedAt: now, UpdatedAt: now, SalaryMin: &smin,
	}
	req := validCreate()
	req.NiceToHaveSkills = []string{"k8s"}
	lo, hi := int32(1000), int32(2000)
	req.SalaryMin, req.SalaryMax = &lo, &hi

	got, err := e.client.CreateJob(e.recruiterCtx(t), req)
	require.NoError(t, err)

	d := e.uc.draft
	assert.Equal(t, domain.LevelSenior, d.Level)
	assert.Equal(t, "Senior Go Engineer", d.Title)
	assert.Equal(t, []string{"go"}, d.RequiredSkills)
	assert.Equal(t, []string{"k8s"}, d.NiceToHaveSkills)
	require.NotNil(t, d.SalaryMin)
	require.NotNil(t, d.SalaryMax)
	assert.Equal(t, 1000, *d.SalaryMin)
	assert.Equal(t, 2000, *d.SalaryMax)

	assert.Equal(t, jid.String(), got.GetId())
	assert.Equal(t, jobv1.JobStatus_JOB_STATUS_DRAFT, got.GetStatus())
	assert.Equal(t, jobv1.JobLevel_JOB_LEVEL_SENIOR, got.GetLevel())
	assert.Equal(t, int64(1), got.GetVersion())
	assert.Equal(t, int32(1000), got.GetSalaryMin())
	assert.Nil(t, got.SalaryMax)
	assert.Nil(t, got.PublishedAt)

	// salary omitted maps to nil, not 0
	req2 := validCreate()
	_, err = e.client.CreateJob(e.recruiterCtx(t), req2)
	require.NoError(t, err)
	assert.Nil(t, e.uc.draft.SalaryMin)
	assert.Nil(t, e.uc.draft.SalaryMax)
}

func TestHandler_UpdateOnlyPresentFields(t *testing.T) {
	e := newEnv(t)
	id := uuid.New()
	title := "A new title"
	_, err := e.client.UpdateJob(e.recruiterCtx(t), &jobv1.UpdateJobRequest{
		Id: id.String(), Title: &title,
		RequiredSkills: &jobv1.StringList{Values: []string{"go", "sql"}},
	})
	require.NoError(t, err)
	assert.Equal(t, id, e.uc.id)
	p := e.uc.patch
	require.NotNil(t, p.Title)
	assert.Equal(t, title, *p.Title)
	require.NotNil(t, p.RequiredSkills)
	assert.Equal(t, []string{"go", "sql"}, *p.RequiredSkills)
	assert.Nil(t, p.Description)
	assert.Nil(t, p.NiceToHaveSkills)
	assert.Nil(t, p.Level)
	assert.Nil(t, p.Location)
	assert.Nil(t, p.SalaryMin)
	assert.Nil(t, p.SalaryMax)
}

func TestHandler_ListJobsMapsFilter(t *testing.T) {
	e := newEnv(t)
	e.uc.page = app.Page{NextToken: "tok"}
	got, err := e.client.ListJobs(context.Background(), &jobv1.ListJobsRequest{})
	require.NoError(t, err)
	assert.Equal(t, "tok", got.GetNextPageToken())
	assert.Nil(t, e.uc.filter.Level)
	assert.Nil(t, e.uc.filter.SalaryMin)
	assert.Equal(t, 0, e.uc.pageReq.Size)

	smin := int32(500)
	_, err = e.client.ListJobs(context.Background(), &jobv1.ListJobsRequest{
		Q: "go", Skills: []string{"Go"}, Location: "Hanoi", Level: jobv1.JobLevel_JOB_LEVEL_SENIOR,
		SalaryMin: &smin, PageSize: 50, PageToken: "abc",
	})
	require.NoError(t, err)
	f := e.uc.filter
	assert.Equal(t, "go", f.Query)
	assert.Equal(t, []string{"go"}, f.Skills)
	assert.Equal(t, "Hanoi", f.Location)
	require.NotNil(t, f.Level)
	assert.Equal(t, domain.LevelSenior, *f.Level)
	require.NotNil(t, f.SalaryMin)
	assert.Equal(t, 500, *f.SalaryMin)
	assert.Equal(t, app.PageRequest{Size: 50, Token: "abc"}, e.uc.pageReq)
}

func TestHandler_ListMyJobsMapsStatus(t *testing.T) {
	e := newEnv(t)
	_, err := e.client.ListMyJobs(e.recruiterCtx(t), &jobv1.ListMyJobsRequest{})
	require.NoError(t, err)
	assert.Nil(t, e.uc.status)
	_, err = e.client.ListMyJobs(e.recruiterCtx(t), &jobv1.ListMyJobsRequest{Status: jobv1.JobStatus_JOB_STATUS_OPEN})
	require.NoError(t, err)
	require.NotNil(t, e.uc.status)
	assert.Equal(t, domain.StatusOpen, *e.uc.status)
}

func TestHandler_ValidationErrors(t *testing.T) {
	e := newEnv(t)
	ctx := e.recruiterCtx(t)
	bad := validCreate()
	bad.Title = "abc"
	_, err := e.client.CreateJob(ctx, bad)
	assert.Equal(t, codes.InvalidArgument, code(err))

	_, err = e.client.PublishJob(ctx, &jobv1.PublishJobRequest{Id: "not-a-uuid"})
	assert.Equal(t, codes.InvalidArgument, code(err))
	assert.Zero(t, e.uc.calls)
}

func TestHandler_ErrorMapping(t *testing.T) {
	e := newEnv(t)
	ctx := e.recruiterCtx(t)
	id := uuid.NewString()

	e.uc.err = domain.ErrJobNotFound
	_, err := e.client.PublishJob(ctx, &jobv1.PublishJobRequest{Id: id})
	assert.Equal(t, codes.NotFound, code(err))
	_, err = e.client.DeleteJob(ctx, &jobv1.DeleteJobRequest{Id: id})
	assert.Equal(t, codes.NotFound, code(err))

	e.uc.err = domain.ErrInvalidTransition
	_, err = e.client.CloseJob(ctx, &jobv1.CloseJobRequest{Id: id})
	assert.Equal(t, codes.InvalidArgument, code(err))
}

func TestHandler_DeleteAndLifecycleCalls(t *testing.T) {
	e := newEnv(t)
	ctx := e.recruiterCtx(t)
	id := uuid.New()
	e.uc.job = domain.Job{ID: id, Status: domain.StatusOpen, Level: domain.LevelLead}
	got, err := e.client.PublishJob(ctx, &jobv1.PublishJobRequest{Id: id.String()})
	require.NoError(t, err)
	assert.Equal(t, jobv1.JobStatus_JOB_STATUS_OPEN, got.GetStatus())
	_, err = e.client.DeleteJob(ctx, &jobv1.DeleteJobRequest{Id: id.String()})
	require.NoError(t, err)
	assert.Equal(t, id, e.uc.id)
}

func TestPublicMethods(t *testing.T) {
	assert.ElementsMatch(t, []string{
		"/smarthire.job.v1.JobService/GetJob",
		"/smarthire.job.v1.JobService/ListJobs",
	}, jobgrpc.PublicMethods)
}
