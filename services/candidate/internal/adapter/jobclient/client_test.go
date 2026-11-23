package jobclient_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	jobv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/job/v1"
	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/jobclient"
)

type fakeJobs struct {
	jobv1.UnimplementedJobServiceServer
	auth []string
	ids  []string
	err  error
}

func (f *fakeJobs) BatchGetJobs(ctx context.Context, req *jobv1.BatchGetJobsRequest) (*jobv1.BatchGetJobsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	f.auth = md.Get("authorization")
	f.ids = req.GetIds()
	if f.err != nil {
		return nil, f.err
	}
	smin := int32(1000)
	return &jobv1.BatchGetJobsResponse{Jobs: []*jobv1.Job{{
		Id: req.GetIds()[0], Title: "Go dev", Level: jobv1.JobLevel_JOB_LEVEL_SENIOR, Location: "Hà Nội", SalaryMin: &smin,
	}}}, nil
}

func start(t *testing.T, f *fakeJobs) *jobclient.Client {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	jobv1.RegisterJobServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return jobclient.New(conn, time.Second)
}

func TestJobClient_ForwardsAuthorization(t *testing.T) {
	f := &fakeJobs{}
	c := start(t, f)
	id := uuid.New()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer tok", "x-other", "no"))
	got, err := c.OpenJobs(ctx, []uuid.UUID{id})
	require.NoError(t, err)
	assert.Equal(t, []string{"Bearer tok"}, f.auth)
	assert.Equal(t, []string{id.String()}, f.ids)
	require.Len(t, got, 1)
	assert.Equal(t, id, got[0].ID)
	assert.Equal(t, "senior", got[0].Level)
	require.NotNil(t, got[0].SalaryMin)
	assert.Equal(t, 1000, *got[0].SalaryMin)
	assert.Nil(t, got[0].SalaryMax)
}

func TestJobClient_ErrorsAreUnavailable(t *testing.T) {
	c := start(t, &fakeJobs{err: status.Error(codes.Internal, "boom")})
	_, err := c.OpenJobs(context.Background(), []uuid.UUID{uuid.New()})
	assert.ErrorIs(t, err, apperr.ErrUnavailable)
}

// An access token that expires between the candidate and job services is the
// caller's problem (401), not a job service outage (503).
func TestJobClient_UnauthenticatedPassesThrough(t *testing.T) {
	c := start(t, &fakeJobs{err: status.Error(codes.Unauthenticated, "token expired")})
	_, err := c.OpenJobs(context.Background(), []uuid.UUID{uuid.New()})
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
	assert.NotErrorIs(t, err, apperr.ErrUnavailable)
}
