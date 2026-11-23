// Package jobclient reads a job from the job service on behalf of the
// signed-in caller: the caller's access token is forwarded, so the job
// service authorizes the call itself (two-layer JWT, ADR 0003).
package jobclient

import (
	"context"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	jobv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/job/v1"
	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
)

// Client implements app.Jobs.
type Client struct {
	jobs    jobv1.JobServiceClient
	timeout time.Duration
}

// New returns a client bounding each call by timeout.
func New(conn grpc.ClientConnInterface, timeout time.Duration) *Client {
	return &Client{jobs: jobv1.NewJobServiceClient(conn), timeout: timeout}
}

// GetJob implements app.Jobs.
func (c *Client) GetJob(ctx context.Context, id uuid.UUID) (app.JobInfo, error) {
	cctx, cancel := context.WithTimeout(ForwardAuth(ctx), c.timeout)
	defer cancel()
	j, err := c.jobs.GetJob(cctx, &jobv1.GetJobRequest{Id: id.String()})
	if err != nil {
		return app.JobInfo{}, MapErr(ctx, err, "job")
	}
	rec, err := uuid.Parse(j.GetRecruiterId())
	if err != nil {
		return app.JobInfo{}, apperr.New(apperr.ErrUnavailable, "job service returned an invalid job")
	}
	return app.JobInfo{ID: id, RecruiterID: rec, Open: j.GetStatus() == jobv1.JobStatus_JOB_STATUS_OPEN}, nil
}

// ForwardAuth copies the caller's authorization metadata to the outgoing
// context.
func ForwardAuth(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	if auth := md.Get("authorization"); len(auth) > 0 {
		return metadata.AppendToOutgoingContext(ctx, "authorization", auth[0])
	}
	return ctx
}

// MapErr maps an upstream read error: NotFound stays NotFound (the caller
// may not see the resource), the caller going away stays its context error,
// and anything else is ErrUnavailable without upstream details.
func MapErr(ctx context.Context, err error, what string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	switch status.Code(err) {
	case codes.NotFound:
		return apperr.New(apperr.ErrNotFound, what+" not found")
	case codes.Unauthenticated:
		return apperr.New(apperr.ErrUnauthenticated, "authentication required")
	}
	return apperr.New(apperr.ErrUnavailable, what+" service temporarily unavailable")
}
