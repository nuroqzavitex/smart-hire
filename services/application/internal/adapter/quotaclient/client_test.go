package quotaclient_test

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
	"google.golang.org/protobuf/types/known/emptypb"

	candidatev1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/candidate/v1"
	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/quotaclient"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
)

type fakeCandidate struct {
	candidatev1.UnimplementedCandidateServiceServer
	mu       sync.Mutex
	err      error
	reserve  *candidatev1.ReserveQuotaRequest
	release  *candidatev1.ReleaseQuotaRequest
	authSeen bool
}

func (f *fakeCandidate) ReserveQuota(ctx context.Context, req *candidatev1.ReserveQuotaRequest) (*emptypb.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	md, _ := metadata.FromIncomingContext(ctx)
	f.authSeen = len(md.Get("authorization")) > 0
	f.reserve = req
	return &emptypb.Empty{}, f.err
}

func (f *fakeCandidate) ReleaseQuota(_ context.Context, req *candidatev1.ReleaseQuotaRequest) (*emptypb.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.release = req
	return &emptypb.Empty{}, f.err
}

func (f *fakeCandidate) set(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func start(t *testing.T, f *fakeCandidate) *quotaclient.Client {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	candidatev1.RegisterCandidateServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return quotaclient.New(conn, time.Second)
}

func TestQuota_SendsIDsWithoutToken(t *testing.T) {
	f := &fakeCandidate{}
	c := start(t, f)
	appID, cand := uuid.New(), uuid.New()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer user"))
	require.NoError(t, c.Reserve(ctx, appID, cand))
	assert.Equal(t, appID.String(), f.reserve.GetApplicationId())
	assert.Equal(t, cand.String(), f.reserve.GetCandidateId())
	assert.False(t, f.authSeen, "internal RPC: no user token is forwarded")
	require.NoError(t, c.Release(context.Background(), appID))
	assert.Equal(t, appID.String(), f.release.GetApplicationId())
}

func TestQuota_MapsCodes(t *testing.T) {
	f := &fakeCandidate{err: status.Error(codes.ResourceExhausted, "used up")}
	c := start(t, f)
	assert.ErrorIs(t, c.Reserve(context.Background(), uuid.New(), uuid.New()), app.ErrQuotaExhausted)
	f.set(status.Error(codes.Aborted, "released"))
	assert.ErrorIs(t, c.Reserve(context.Background(), uuid.New(), uuid.New()), app.ErrReservationReleased)
	f.set(status.Error(codes.Unavailable, "down"))
	err := c.Reserve(context.Background(), uuid.New(), uuid.New())
	require.Error(t, err)
	assert.NotErrorIs(t, err, app.ErrQuotaExhausted)
	assert.Error(t, c.Release(context.Background(), uuid.New()))
}
