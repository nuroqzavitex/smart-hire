package aiclient_test

import (
	"context"
	"encoding/json"
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
	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/aiclient"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
)

// fakeAI is shared by concurrent handlers, so every field is synchronized
// or set before the server starts.
type fakeAI struct {
	aiv1.UnimplementedAIServiceServer
	calls atomic.Int32
	err   error
	delay atomic.Int64 // time.Duration
}

func (f *fakeAI) ScoreMatch(ctx context.Context, _ *aiv1.ScoreMatchRequest) (*aiv1.ScoreMatchResponse, error) {
	f.calls.Add(1)
	if d := time.Duration(f.delay.Load()); d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	return &aiv1.ScoreMatchResponse{Score: 87.5, ModelVersion: "fake-hash@768", Breakdown: &aiv1.ScoreBreakdown{
		SemanticSimilarity: 0.8, RequiredSkillCoverage: 1, ExperienceFit: 0.5, LocationFit: 1,
		MatchedSkills: []string{"Go"}, MissingSkills: []string{"Rust"},
	}}, nil
}

func start(t *testing.T, ai *fakeAI, timeout time.Duration) *aiclient.Client {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	aiv1.RegisterAIServiceServer(srv, ai)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return aiclient.New(conn, 5, time.Minute, timeout, nil)
}

func TestAI_MapsScore(t *testing.T) {
	c := start(t, &fakeAI{}, time.Second)
	got, err := c.Score(context.Background(), uuid.New(), uuid.New())
	require.NoError(t, err)
	assert.InDelta(t, 87.5, got.Value, 1e-9)
	assert.Equal(t, "fake-hash@768", got.ModelVersion)
	var b map[string]any
	require.NoError(t, json.Unmarshal(got.Breakdown, &b))
	assert.InDelta(t, 0.8, b["semantic_similarity"], 1e-9)
	assert.InDelta(t, 1, b["required_skill_coverage"], 1e-9)
	assert.Equal(t, []any{"Go"}, b["matched_skills"])
	assert.Equal(t, []any{"Rust"}, b["missing_skills"])
}

func TestAI_BreakerOpensAfterFiveFailures(t *testing.T) {
	ai := &fakeAI{err: status.Error(codes.Unavailable, "down")}
	c := start(t, ai, time.Second)
	for range 5 {
		_, err := c.Score(context.Background(), uuid.New(), uuid.New())
		assert.ErrorIs(t, err, app.ErrScorerUnavailable)
	}
	begin := time.Now()
	_, err := c.Score(context.Background(), uuid.New(), uuid.New())
	assert.ErrorIs(t, err, app.ErrScorerUnavailable)
	assert.Less(t, time.Since(begin), 100*time.Millisecond, "open breaker fails fast")
	assert.Equal(t, int32(5), ai.calls.Load(), "open breaker does not call the AI service")
}

func TestAI_NotFoundDoesNotTrip(t *testing.T) {
	ai := &fakeAI{err: status.Error(codes.NotFound, "cv not matched yet")}
	c := start(t, ai, time.Second)
	for range 7 {
		_, err := c.Score(context.Background(), uuid.New(), uuid.New())
		assert.ErrorIs(t, err, app.ErrNotMatched)
	}
	assert.Equal(t, int32(7), ai.calls.Load())
}

func TestAI_TimeoutIsUnavailable(t *testing.T) {
	ai := &fakeAI{}
	ai.delay.Store(int64(time.Second))
	c := start(t, ai, 100*time.Millisecond)
	_, err := c.Score(context.Background(), uuid.New(), uuid.New())
	assert.ErrorIs(t, err, app.ErrScorerUnavailable)
}

func TestAI_CallerCancelDoesNotTrip(t *testing.T) {
	ai := &fakeAI{}
	ai.delay.Store(int64(200 * time.Millisecond))
	c := start(t, ai, time.Second)
	for range 6 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		_, err := c.Score(ctx, uuid.New(), uuid.New())
		cancel()
		assert.Error(t, err)
	}
	ai.delay.Store(0)
	_, err := c.Score(context.Background(), uuid.New(), uuid.New())
	require.NoError(t, err, "breaker still closed")
}
