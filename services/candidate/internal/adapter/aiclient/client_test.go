package aiclient_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	aiv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/ai/v1"
	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/otel/oteltest"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/aiclient"
)

type fakeAI struct {
	aiv1.UnimplementedAIServiceServer
	calls atomic.Int32
	err   error
	delay time.Duration
	jobID string
}

func (f *fakeAI) RecommendJobs(ctx context.Context, _ *aiv1.RecommendJobsRequest) (*aiv1.RecommendJobsResponse, error) {
	f.calls.Add(1)
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	return &aiv1.RecommendJobsResponse{ModelVersion: "m@768", Jobs: []*aiv1.RankedJob{{
		JobId: f.jobID, Score: 66.5,
		Breakdown: &aiv1.ScoreBreakdown{SemanticSimilarity: 0.7, RequiredSkillCoverage: 0.5, MatchedSkills: []string{"Go"}},
	}}}, nil
}

func start(t *testing.T, ai *fakeAI) *aiclient.Client {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	aiv1.RegisterAIServiceServer(srv, ai)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return aiclient.New(conn, 3, time.Minute, 200*time.Millisecond)
}

func TestClient_MapsResponse(t *testing.T) {
	job := uuid.New()
	c := start(t, &fakeAI{jobID: job.String()})
	got, err := c.RecommendJobs(context.Background(), uuid.New(), 5)
	require.NoError(t, err)
	assert.Equal(t, "m@768", got.ModelVersion)
	require.Len(t, got.Jobs, 1)
	assert.Equal(t, job, got.Jobs[0].JobID)
	assert.InDelta(t, 66.5, got.Jobs[0].Score, 1e-9)
	assert.Equal(t, []string{"Go"}, got.Jobs[0].Breakdown.MatchedSkills)
	assert.InDelta(t, 0.5, got.Jobs[0].Breakdown.RequiredSkillCoverage, 1e-9)
}

func TestClient_BreakerOpensAfterFailures(t *testing.T) {
	ai := &fakeAI{err: status.Error(codes.Unavailable, "down")}
	c := start(t, ai)
	for range 4 {
		_, err := c.RecommendJobs(context.Background(), uuid.New(), 0)
		assert.ErrorIs(t, err, apperr.ErrUnavailable)
	}
	assert.Equal(t, int32(3), ai.calls.Load())
}

func TestClient_TimeoutIsUnavailable(t *testing.T) {
	c := start(t, &fakeAI{delay: time.Second})
	_, err := c.RecommendJobs(context.Background(), uuid.New(), 0)
	assert.ErrorIs(t, err, apperr.ErrUnavailable)
}

func TestClient_NotMatchedYetIsEmptyNotError(t *testing.T) {
	ai := &fakeAI{err: status.Error(codes.NotFound, "cv not matched yet")}
	c := start(t, ai)
	for range 5 {
		got, err := c.RecommendJobs(context.Background(), uuid.New(), 0)
		require.NoError(t, err)
		assert.Empty(t, got.Jobs)
	}
	assert.Equal(t, int32(5), ai.calls.Load(), "not found does not trip the breaker")
}

func TestClient_BreakerStateMetric(t *testing.T) {
	m := oteltest.Install(t)
	c := start(t, &fakeAI{err: status.Error(codes.Unavailable, "down")})
	for range 4 {
		_, _ = c.RecommendJobs(context.Background(), uuid.New(), 0)
	}
	v, ok := m.Int64(t, "circuit_breaker.state", "breaker", "candidate-ai-matching")
	require.True(t, ok)
	assert.Equal(t, int64(2), v)
}
