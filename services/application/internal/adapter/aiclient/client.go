// Package aiclient calls the AI service's ScoreMatch for the submit saga
// with a per-call timeout and a circuit breaker (spec §14.2): when the AI is
// down the SCORE step fails fast and the application goes to
// NEEDS_MANUAL_REVIEW instead of waiting for every timeout.
package aiclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/sony/gobreaker/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	aiv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/ai/v1"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
)

// Client implements app.Scorer.
type Client struct {
	ai      aiv1.AIServiceClient
	cb      *gobreaker.CircuitBreaker[*aiv1.ScoreMatchResponse]
	timeout time.Duration
}

// New opens the breaker after failures consecutive errors, lets one trial
// call through after open, and bounds each call by timeout. Breaker state
// changes are logged on log (nil discards).
func New(conn grpc.ClientConnInterface, failures uint32, open, timeout time.Duration, log *slog.Logger) *Client {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	cb := gobreaker.NewCircuitBreaker[*aiv1.ScoreMatchResponse](gobreaker.Settings{
		Name:         "application-ai-score",
		MaxRequests:  1,
		Timeout:      open,
		ReadyToTrip:  func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= failures },
		IsSuccessful: healthy,
		OnStateChange: func(name string, from, to gobreaker.State) {
			log.Warn("circuit breaker state changed", "breaker", name, "from", from.String(), "to", to.String())
		},
	})
	return &Client{ai: aiv1.NewAIServiceClient(conn), cb: cb, timeout: timeout}
}

// callerGone marks an error caused by the caller's own context ending.
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

type breakdown struct {
	SemanticSimilarity    float64  `json:"semantic_similarity"`
	RequiredSkillCoverage float64  `json:"required_skill_coverage"`
	ExperienceFit         float64  `json:"experience_fit"`
	LocationFit           float64  `json:"location_fit"`
	MatchedSkills         []string `json:"matched_skills"`
	MissingSkills         []string `json:"missing_skills"`
}

// Score implements app.Scorer.
func (c *Client) Score(ctx context.Context, cvID, jobID uuid.UUID) (app.Score, error) {
	resp, err := c.cb.Execute(func() (*aiv1.ScoreMatchResponse, error) {
		cctx, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		resp, err := c.ai.ScoreMatch(cctx, &aiv1.ScoreMatchRequest{CvId: cvID.String(), JobId: jobID.String()})
		if err != nil && ctx.Err() != nil {
			return nil, &callerGone{err: err}
		}
		return resp, err
	})
	if err != nil {
		var gone *callerGone
		switch {
		case errors.As(err, &gone):
			return app.Score{}, ctx.Err()
		case status.Code(err) == codes.NotFound:
			return app.Score{}, app.ErrNotMatched
		}
		// Breaker open, timeout, AI down or internal error.
		return app.Score{}, fmt.Errorf("%w: %s", app.ErrScorerUnavailable, status.Code(err))
	}
	b := resp.GetBreakdown()
	raw, err := json.Marshal(breakdown{
		SemanticSimilarity: b.GetSemanticSimilarity(), RequiredSkillCoverage: b.GetRequiredSkillCoverage(),
		ExperienceFit: b.GetExperienceFit(), LocationFit: b.GetLocationFit(),
		MatchedSkills: nonNil(b.GetMatchedSkills()), MissingSkills: nonNil(b.GetMissingSkills()),
	})
	if err != nil {
		return app.Score{}, fmt.Errorf("marshal breakdown: %w", err)
	}
	return app.Score{Value: resp.GetScore(), Breakdown: raw, ModelVersion: resp.GetModelVersion()}, nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
