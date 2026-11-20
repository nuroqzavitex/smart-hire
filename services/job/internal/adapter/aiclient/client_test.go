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
	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/aiclient"
)

type fakeAI struct {
	aiv1.UnimplementedAIServiceServer
	calls atomic.Int32
	err   error
	delay time.Duration
	last  *aiv1.FindCandidatesRequest
}

func (f *fakeAI) FindCandidates(ctx context.Context, req *aiv1.FindCandidatesRequest) (*aiv1.FindCandidatesResponse, error) {
	f.calls.Add(1)
	f.last = req
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
	return &aiv1.FindCandidatesResponse{ModelVersion: "fake-hash@768", Candidates: []*aiv1.RankedCandidate{{
		CandidateId: "7d0c2a4e-1b9f-4e2a-8c3d-5f6a7b8c9d0e", CvId: "0b8f3c9e-6f1a-4c55-9a51-3c1f2e9b7d10", Score: 87.5,
		Breakdown: &aiv1.ScoreBreakdown{SemanticSimilarity: 0.8, RequiredSkillCoverage: 1, ExperienceFit: 0.5,
			LocationFit: 1, MatchedSkills: []string{"Go"}, MissingSkills: []string{"Rust"}},
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
	ai := &fakeAI{}
	c := start(t, ai)
	job := uuid.New()
	got, err := c.FindCandidates(context.Background(), job, 7)
	require.NoError(t, err)
	assert.Equal(t, job.String(), ai.last.GetJobId())
	assert.Equal(t, int32(7), ai.last.GetTopK())
	assert.Equal(t, "fake-hash@768", got.ModelVersion)
	require.Len(t, got.Candidates, 1)
	m := got.Candidates[0]
	assert.Equal(t, uuid.MustParse("0b8f3c9e-6f1a-4c55-9a51-3c1f2e9b7d10"), m.CVID)
	assert.InDelta(t, 87.5, m.Score, 1e-9)
	assert.Equal(t, []string{"Go"}, m.Breakdown.MatchedSkills)
	assert.Equal(t, []string{"Rust"}, m.Breakdown.MissingSkills)
	assert.InDelta(t, 0.5, m.Breakdown.ExperienceFit, 1e-9)
}

func TestClient_BreakerOpensAfterFailures(t *testing.T) {
	ai := &fakeAI{err: status.Error(codes.Unavailable, "down")}
	c := start(t, ai)
	for range 3 {
		_, err := c.FindCandidates(context.Background(), uuid.New(), 0)
		assert.ErrorIs(t, err, apperr.ErrUnavailable)
	}
	_, err := c.FindCandidates(context.Background(), uuid.New(), 0)
	assert.ErrorIs(t, err, apperr.ErrUnavailable)
	assert.Equal(t, int32(3), ai.calls.Load(), "open breaker does not call the AI service")
}

func TestClient_TimeoutIsUnavailable(t *testing.T) {
	ai := &fakeAI{delay: time.Second}
	c := start(t, ai)
	_, err := c.FindCandidates(context.Background(), uuid.New(), 0)
	assert.ErrorIs(t, err, apperr.ErrUnavailable)
}

func TestClient_NotFoundDoesNotTrip(t *testing.T) {
	ai := &fakeAI{err: status.Error(codes.NotFound, "job not matched yet")}
	c := start(t, ai)
	for range 5 {
		_, err := c.FindCandidates(context.Background(), uuid.New(), 0)
		assert.ErrorIs(t, err, apperr.ErrNotFound)
	}
	assert.Equal(t, int32(5), ai.calls.Load())
}

func TestClient_CallerCancelDoesNotTrip(t *testing.T) {
	ai := &fakeAI{delay: 100 * time.Millisecond}
	c := start(t, ai)
	for range 5 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		_, err := c.FindCandidates(ctx, uuid.New(), 0)
		cancel()
		assert.Error(t, err)
	}
	ai.delay = 0
	_, err := c.FindCandidates(context.Background(), uuid.New(), 0)
	require.NoError(t, err, "breaker still closed")
}
