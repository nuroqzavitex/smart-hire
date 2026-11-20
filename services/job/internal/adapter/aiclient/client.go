// Package aiclient calls the AI service's matching RPCs with a per-call
// timeout and a circuit breaker (ADR 0010): when the AI service is down the
// matching endpoints fail fast with apperr.ErrUnavailable (HTTP 503) instead
// of piling up requests.
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
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
)

// Client implements app.Matcher.
type Client struct {
	ai      aiv1.AIServiceClient
	cb      *gobreaker.CircuitBreaker[*aiv1.FindCandidatesResponse]
	timeout time.Duration
}

// New opens the breaker after failures consecutive errors, lets one trial
// call through after open, and bounds each call by timeout.
func New(conn grpc.ClientConnInterface, failures uint32, open, timeout time.Duration) *Client {
	cb := gobreaker.NewCircuitBreaker[*aiv1.FindCandidatesResponse](gobreaker.Settings{
		Name:         "job-ai-matching",
		MaxRequests:  1,
		Timeout:      open,
		ReadyToTrip:  func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= failures },
		IsSuccessful: healthy,
	})
	return &Client{ai: aiv1.NewAIServiceClient(conn), cb: cb, timeout: timeout}
}

// callerGone marks an error caused by the caller's own context ending.
type callerGone struct{ err error }

func (e *callerGone) Error() string { return e.err.Error() }
func (e *callerGone) Unwrap() error { return e.err }

// healthy reports whether err says nothing bad about the AI service: success,
// the caller going away, or a request-level answer (not found, bad argument).
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

// FindCandidates implements app.Matcher.
func (c *Client) FindCandidates(ctx context.Context, jobID uuid.UUID, topK int) (app.MatchList, error) {
	resp, err := c.cb.Execute(func() (*aiv1.FindCandidatesResponse, error) {
		cctx, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		resp, err := c.ai.FindCandidates(cctx, &aiv1.FindCandidatesRequest{JobId: jobID.String(), TopK: int32(topK)}) //nolint:gosec // topK ≤ 50 (validated)
		if err != nil && ctx.Err() != nil {
			return nil, &callerGone{err: err}
		}
		return resp, err
	})
	if err != nil {
		return app.MatchList{}, mapErr(ctx, err)
	}
	out := app.MatchList{ModelVersion: resp.GetModelVersion(), Candidates: make([]app.MatchedCandidate, 0, len(resp.GetCandidates()))}
	for _, rc := range resp.GetCandidates() {
		cand, err1 := uuid.Parse(rc.GetCandidateId())
		cv, err2 := uuid.Parse(rc.GetCvId())
		if err1 != nil || err2 != nil {
			return app.MatchList{}, fmt.Errorf("ai: invalid id in response: %w", apperr.ErrUnavailable)
		}
		out.Candidates = append(out.Candidates, app.MatchedCandidate{
			CandidateID: cand, CVID: cv, Score: rc.GetScore(), Breakdown: Breakdown(rc.GetBreakdown()),
		})
	}
	return out, nil
}

// Breakdown maps the AI breakdown to the app type.
func Breakdown(b *aiv1.ScoreBreakdown) app.MatchBreakdown {
	return app.MatchBreakdown{
		SemanticSimilarity:    b.GetSemanticSimilarity(),
		RequiredSkillCoverage: b.GetRequiredSkillCoverage(),
		ExperienceFit:         b.GetExperienceFit(),
		LocationFit:           b.GetLocationFit(),
		MatchedSkills:         b.GetMatchedSkills(),
		MissingSkills:         b.GetMissingSkills(),
	}
}

func mapErr(ctx context.Context, err error) error {
	var gone *callerGone
	if errors.As(err, &gone) {
		return ctx.Err()
	}
	switch status.Code(err) {
	case codes.NotFound:
		return apperr.New(apperr.ErrNotFound, "no match data for this job yet")
	case codes.InvalidArgument:
		return apperr.New(apperr.ErrValidation, "invalid matching request")
	}
	// Breaker open, timeout, AI down or internal error: the details stay here.
	return apperr.New(apperr.ErrUnavailable, "matching temporarily unavailable")
}
