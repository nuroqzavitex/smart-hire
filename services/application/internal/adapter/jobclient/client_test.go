package jobclient_test

import (
	"context"
	"net"
	"sync"
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

	candidatev1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/candidate/v1"
	jobv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/job/v1"
	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/cvclient"
	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/jobclient"
)

type fakeUpstream struct {
	jobv1.UnimplementedJobServiceServer
	candidatev1.UnimplementedCandidateServiceServer
	mu     sync.Mutex
	auth   string
	err    error
	job    *jobv1.Job
	cv     *candidatev1.CV
	jobReq string
	cvReq  string
}

func (f *fakeUpstream) GetJob(ctx context.Context, req *jobv1.GetJobRequest) (*jobv1.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(ctx)
	f.jobReq = req.GetId()
	return f.job, f.err
}

func (f *fakeUpstream) GetCV(ctx context.Context, req *candidatev1.GetCVRequest) (*candidatev1.CV, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record(ctx)
	f.cvReq = req.GetId()
	return f.cv, f.err
}

func (f *fakeUpstream) record(ctx context.Context) {
	md, _ := metadata.FromIncomingContext(ctx)
	if a := md.Get("authorization"); len(a) > 0 {
		f.auth = a[0]
	}
}

// with mutates the fake under its lock (handlers read it concurrently).
func (f *fakeUpstream) with(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn()
}

func dial(t *testing.T, f *fakeUpstream) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	jobv1.RegisterJobServiceServer(srv, f)
	candidatev1.RegisterCandidateServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func userCtx() context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer user-token"))
}

func TestJobs_ForwardsAuthAndMapsStatus(t *testing.T) {
	jobID, rec := uuid.New(), uuid.New()
	f := &fakeUpstream{job: &jobv1.Job{Id: jobID.String(), RecruiterId: rec.String(), Status: jobv1.JobStatus_JOB_STATUS_OPEN}}
	c := jobclient.New(dial(t, f), time.Second)
	got, err := c.GetJob(userCtx(), jobID)
	require.NoError(t, err)
	assert.Equal(t, "Bearer user-token", f.auth)
	assert.Equal(t, jobID.String(), f.jobReq)
	assert.Equal(t, jobID, got.ID)
	assert.Equal(t, rec, got.RecruiterID)
	assert.True(t, got.Open)

	f.with(func() { f.job.Status = jobv1.JobStatus_JOB_STATUS_CLOSED })
	got, err = c.GetJob(userCtx(), jobID)
	require.NoError(t, err)
	assert.False(t, got.Open)
}

func TestJobs_MapsErrors(t *testing.T) {
	f := &fakeUpstream{err: status.Error(codes.NotFound, "job not found")}
	c := jobclient.New(dial(t, f), time.Second)
	_, err := c.GetJob(userCtx(), uuid.New())
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	f.with(func() { f.err = status.Error(codes.Unavailable, "down") })
	_, err = c.GetJob(userCtx(), uuid.New())
	assert.ErrorIs(t, err, apperr.ErrUnavailable)
}

func TestCVs_MapsParsed(t *testing.T) {
	cvID := uuid.New()
	f := &fakeUpstream{cv: &candidatev1.CV{Id: cvID.String(), Status: candidatev1.CVStatus_CV_STATUS_PARSED}}
	c := cvclient.New(dial(t, f), time.Second)
	got, err := c.GetCV(userCtx(), cvID)
	require.NoError(t, err)
	assert.Equal(t, "Bearer user-token", f.auth)
	assert.Equal(t, cvID, got.ID)
	assert.True(t, got.Parsed)

	f.with(func() { f.cv.Status = candidatev1.CVStatus_CV_STATUS_UPLOADED })
	got, err = c.GetCV(userCtx(), cvID)
	require.NoError(t, err)
	assert.False(t, got.Parsed)

	f.with(func() { f.err = status.Error(codes.NotFound, "cv not found") })
	_, err = c.GetCV(userCtx(), cvID)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	f.with(func() { f.err = status.Error(codes.DeadlineExceeded, "slow") })
	_, err = c.GetCV(userCtx(), cvID)
	assert.ErrorIs(t, err, apperr.ErrUnavailable)
}
