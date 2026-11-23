// Package quotaclient calls the candidate service's internal quota RPCs for
// the submit saga. The worker has no user token, so none is sent: the RPCs
// are allow-listed without JWT and have no HTTP route (ADR 0011).
package quotaclient

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	candidatev1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/candidate/v1"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
)

// Client implements app.Quota.
type Client struct {
	cand    candidatev1.CandidateServiceClient
	timeout time.Duration
}

// New returns a client bounding each call by timeout.
func New(conn grpc.ClientConnInterface, timeout time.Duration) *Client {
	return &Client{cand: candidatev1.NewCandidateServiceClient(conn), timeout: timeout}
}

// Reserve implements app.Quota.
func (c *Client) Reserve(ctx context.Context, appID, candidateID uuid.UUID) error {
	cctx, cancel := c.callCtx(ctx)
	defer cancel()
	_, err := c.cand.ReserveQuota(cctx, &candidatev1.ReserveQuotaRequest{
		ApplicationId: appID.String(), CandidateId: candidateID.String(),
	})
	switch status.Code(err) {
	case codes.OK:
		return nil
	case codes.ResourceExhausted:
		return app.ErrQuotaExhausted
	case codes.Aborted:
		return app.ErrReservationReleased
	}
	return fmt.Errorf("reserve quota: %s", status.Code(err))
}

// Release implements app.Quota.
func (c *Client) Release(ctx context.Context, appID uuid.UUID) error {
	cctx, cancel := c.callCtx(ctx)
	defer cancel()
	if _, err := c.cand.ReleaseQuota(cctx, &candidatev1.ReleaseQuotaRequest{ApplicationId: appID.String()}); err != nil {
		return fmt.Errorf("release quota: %s", status.Code(err))
	}
	return nil
}

// callCtx drops any incoming metadata (no user token travels to the
// internal RPCs) and applies the timeout.
func (c *Client) callCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx = metadata.NewIncomingContext(ctx, metadata.MD{})
	ctx = metadata.NewOutgoingContext(ctx, metadata.MD{})
	return context.WithTimeout(ctx, c.timeout)
}
