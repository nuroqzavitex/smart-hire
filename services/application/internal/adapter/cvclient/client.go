// Package cvclient reads one of the caller's CVs from the candidate service,
// forwarding the caller's access token (GetCV is owner-only there).
package cvclient

import (
	"context"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	candidatev1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/candidate/v1"
	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/jobclient"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
)

// Client implements app.CVs.
type Client struct {
	cand    candidatev1.CandidateServiceClient
	timeout time.Duration
}

// New returns a client bounding each call by timeout.
func New(conn grpc.ClientConnInterface, timeout time.Duration) *Client {
	return &Client{cand: candidatev1.NewCandidateServiceClient(conn), timeout: timeout}
}

// GetCV implements app.CVs.
func (c *Client) GetCV(ctx context.Context, id uuid.UUID) (app.CVInfo, error) {
	cctx, cancel := context.WithTimeout(jobclient.ForwardAuth(ctx), c.timeout)
	defer cancel()
	cv, err := c.cand.GetCV(cctx, &candidatev1.GetCVRequest{Id: id.String()})
	if err != nil {
		return app.CVInfo{}, jobclient.MapErr(ctx, err, "cv")
	}
	return app.CVInfo{ID: id, Parsed: cv.GetStatus() == candidatev1.CVStatus_CV_STATUS_PARSED}, nil
}
