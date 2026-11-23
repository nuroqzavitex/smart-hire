package dialx_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	jobv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/job/v1"
	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/dialx"
)

// flakyJobs fails the first failN calls with code, then succeeds.
type flakyJobs struct {
	jobv1.UnimplementedJobServiceServer
	calls atomic.Int32
	failN int32
	code  codes.Code
}

func (f *flakyJobs) GetJob(context.Context, *jobv1.GetJobRequest) (*jobv1.Job, error) {
	if n := f.calls.Add(1); n <= f.failN {
		return nil, status.Error(f.code, "flaky")
	}
	return &jobv1.Job{Id: "j"}, nil
}

func client(t *testing.T, srv *flakyJobs) jobv1.JobServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	jobv1.RegisterJobServiceServer(s, srv)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		dialx.RetryPolicy(jobv1.JobService_ServiceDesc.ServiceName))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return jobv1.NewJobServiceClient(conn)
}

func TestRetryPolicy_RetriesUnavailable(t *testing.T) {
	srv := &flakyJobs{failN: 2, code: codes.Unavailable}
	_, err := client(t, srv).GetJob(context.Background(), &jobv1.GetJobRequest{Id: "x"})
	require.NoError(t, err)
	assert.Equal(t, int32(3), srv.calls.Load())
}

func TestRetryPolicy_GivesUpAfterThreeAttempts(t *testing.T) {
	srv := &flakyJobs{failN: 10, code: codes.Unavailable}
	_, err := client(t, srv).GetJob(context.Background(), &jobv1.GetJobRequest{Id: "x"})
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Equal(t, int32(3), srv.calls.Load())
}

func TestRetryPolicy_DoesNotRetryOtherCodes(t *testing.T) {
	srv := &flakyJobs{failN: 10, code: codes.InvalidArgument}
	_, err := client(t, srv).GetJob(context.Background(), &jobv1.GetJobRequest{Id: "x"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Equal(t, int32(1), srv.calls.Load())
}
