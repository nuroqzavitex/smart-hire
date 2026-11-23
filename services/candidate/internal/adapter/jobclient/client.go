// Package jobclient reads open jobs from the job service on behalf of the
// signed-in caller: the caller's access token is forwarded, so the job
// service authorizes the call itself (two-layer JWT, ADR 0001).
package jobclient

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	jobv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/job/v1"
	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
)

// Client implements app.JobDirectory.
type Client struct {
	jobs    jobv1.JobServiceClient
	timeout time.Duration
}

// New returns a client bounding each call by timeout.
func New(conn grpc.ClientConnInterface, timeout time.Duration) *Client {
	return &Client{jobs: jobv1.NewJobServiceClient(conn), timeout: timeout}
}

// OpenJobs implements app.JobDirectory.
func (c *Client) OpenJobs(ctx context.Context, ids []uuid.UUID) ([]app.JobSummary, error) {
	req := &jobv1.BatchGetJobsRequest{Ids: make([]string, len(ids))}
	for i, id := range ids {
		req.Ids[i] = id.String()
	}
	cctx, cancel := context.WithTimeout(forwardAuth(ctx), c.timeout)
	defer cancel()
	resp, err := c.jobs.BatchGetJobs(cctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if status.Code(err) == codes.Unauthenticated {
			// The caller's token expired on the way: theirs to renew (401).
			return nil, apperr.New(apperr.ErrUnauthenticated, "authentication required")
		}
		return nil, apperr.New(apperr.ErrUnavailable, "jobs temporarily unavailable")
	}
	out := make([]app.JobSummary, 0, len(resp.GetJobs()))
	for _, j := range resp.GetJobs() {
		id, err := uuid.Parse(j.GetId())
		if err != nil {
			return nil, errors.Join(apperr.ErrUnavailable, err)
		}
		out = append(out, app.JobSummary{
			ID:        id,
			Title:     j.GetTitle(),
			Level:     strings.ToLower(strings.TrimPrefix(j.GetLevel().String(), "JOB_LEVEL_")),
			Location:  j.GetLocation(),
			SalaryMin: intPtr(j.SalaryMin),
			SalaryMax: intPtr(j.SalaryMax),
		})
	}
	return out, nil
}

func forwardAuth(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	if auth := md.Get("authorization"); len(auth) > 0 {
		return metadata.AppendToOutgoingContext(ctx, "authorization", auth[0])
	}
	return ctx
}

func intPtr(v *int32) *int {
	if v == nil {
		return nil
	}
	n := int(*v)
	return &n
}
