// Package aiclient calls the AI service's RecommendJobs with a per-call
// timeout and a circuit breaker (ADR 0010): when the AI service is down the
// recommendations endpoint fails fast with apperr.ErrUnavailable (HTTP 503).
package aiclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sony/gobreaker/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	aiv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/ai/v1"
	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	pkgotel "github.com/nuroqzavitex/smarthire/pkg/otel"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
)

// Client implements app.Recommender.
type Client struct {
	ai      aiv1.AIServiceClient
	cb      *gobreaker.CircuitBreaker[*aiv1.RecommendJobsResponse]
	timeout time.Duration
}

// New opens the breaker after failures consecutive errors, lets one trial
// call through after open, and bounds each call by timeout.
func New(conn grpc.ClientConnInterface, failures uint32, open, timeout time.Duration) *Client {
	cb := gobreaker.NewCircuitBreaker[*aiv1.RecommendJobsResponse](gobreaker.Settings{
		OnStateChange: func(name string, _, to gobreaker.State) {
			pkgotel.RecordBreakerState(context.Background(), name, pkgotel.BreakerStateFromString(to.String()))
		},
		Name:         "candidate-ai-matching",
		MaxRequests:  1,
		Timeout:      open,
		ReadyToTrip:  func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= failures },
		IsSuccessful: healthy,
	})
	pkgotel.RecordBreakerState(context.Background(), "candidate-ai-matching", pkgotel.BreakerClosed)
	return &Client{ai: aiv1.NewAIServiceClient(conn), cb: cb, timeout: timeout}
}

type callerGone struct{ err error }

func (e *callerGone) Error() string { return e.err.Error() }
func (e *callerGone) Unwrap() error { return e.err }

// healthy reports whether err says nothing bad about the AI service.
func healthy(err error) bool {
	var gone *callerGone
	if err == nil || errors.As(err, &gone) {
		return true
	}
	switch status.Code(err) {
	case codes.NotFound, codes.InvalidArgument:
		return true
	}
	return false
}

// RecommendJobs implements app.Recommender. A CV the matcher has not
// embedded yet (NotFound) yields an empty list.
func (c *Client) RecommendJobs(ctx context.Context, cvID uuid.UUID, topK int) (app.Recommendations, error) {
	resp, err := c.cb.Execute(func() (*aiv1.RecommendJobsResponse, error) {
		cctx, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		resp, err := c.ai.RecommendJobs(cctx, &aiv1.RecommendJobsRequest{CvId: cvID.String(), TopK: int32(topK)}) //nolint:gosec // topK ≤ 50 (validated)
		if err != nil && ctx.Err() != nil {
			return nil, &callerGone{err: err}
		}
		return resp, err
	})
	if err != nil {
		var gone *callerGone
		switch {
		case errors.As(err, &gone):
			return app.Recommendations{}, ctx.Err()
		case status.Code(err) == codes.NotFound:
			return app.Recommendations{Jobs: []app.RankedJob{}}, nil
		case status.Code(err) == codes.InvalidArgument:
			return app.Recommendations{}, apperr.New(apperr.ErrValidation, "invalid matching request")
		}
		return app.Recommendations{}, apperr.New(apperr.ErrUnavailable, "matching temporarily unavailable")
	}
	out := app.Recommendations{ModelVersion: resp.GetModelVersion(), Jobs: make([]app.RankedJob, 0, len(resp.GetJobs()))}
	for _, j := range resp.GetJobs() {
		id, err := uuid.Parse(j.GetJobId())
		if err != nil {
			return app.Recommendations{}, fmt.Errorf("ai: invalid job id in response: %w", apperr.ErrUnavailable)
		}
		b := j.GetBreakdown()
		out.Jobs = append(out.Jobs, app.RankedJob{JobID: id, Score: j.GetScore(), Breakdown: app.MatchBreakdown{
			SemanticSimilarity:    b.GetSemanticSimilarity(),
			RequiredSkillCoverage: b.GetRequiredSkillCoverage(),
			ExperienceFit:         b.GetExperienceFit(),
			LocationFit:           b.GetLocationFit(),
			MatchedSkills:         b.GetMatchedSkills(),
			MissingSkills:         b.GetMissingSkills(),
		}})
	}
	return out, nil
}
