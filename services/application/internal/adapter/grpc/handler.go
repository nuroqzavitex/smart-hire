// Package grpc adapts the application use cases to the generated
// ApplicationService gRPC API.
package grpc

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"

	applicationv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/application/v1"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

// PublicMethods are the RPCs callable without an access token: none.
var PublicMethods = []string{}

// IdempotencyKeyHeader is the metadata key the gateway forwards the
// Idempotency-Key HTTP header as.
const IdempotencyKeyHeader = "idempotency-key"

// UseCases is satisfied by *app.Service.
type UseCases interface {
	Submit(ctx context.Context, in app.SubmitInput) (domain.Application, error)
	Get(ctx context.Context, id uuid.UUID) (domain.Application, []domain.HistoryEntry, error)
	List(ctx context.Context, in app.ListInput) ([]domain.Application, string, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, to domain.Status) (domain.Application, error)
}

// Handler implements applicationv1.ApplicationServiceServer.
type Handler struct {
	applicationv1.UnimplementedApplicationServiceServer
	uc UseCases
}

// NewHandler returns the ApplicationService implementation backed by uc.
func NewHandler(uc UseCases) *Handler { return &Handler{uc: uc} }

// SubmitApplication starts an application; the saga continues in the
// background.
func (h *Handler) SubmitApplication(ctx context.Context, req *applicationv1.SubmitApplicationRequest) (*applicationv1.Application, error) {
	job, err := parseID(req.GetJobId())
	if err != nil {
		return nil, err
	}
	cv, err := parseID(req.GetCvId())
	if err != nil {
		return nil, err
	}
	key := ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(IdempotencyKeyHeader); len(v) > 0 {
			key = v[0]
		}
	}
	a, err := h.uc.Submit(ctx, app.SubmitInput{JobID: job, CVID: cv, IdempotencyKey: key})
	if err != nil {
		return nil, err
	}
	return applicationToProto(a)
}

// GetApplication returns one application with its history.
func (h *Handler) GetApplication(ctx context.Context, req *applicationv1.GetApplicationRequest) (*applicationv1.GetApplicationResponse, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	a, hist, err := h.uc.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	pa, err := applicationToProto(a)
	if err != nil {
		return nil, err
	}
	resp := &applicationv1.GetApplicationResponse{Application: pa, History: make([]*applicationv1.HistoryEntry, len(hist))}
	for i, e := range hist {
		resp.History[i] = historyToProto(e)
	}
	return resp, nil
}

// ListApplications pages the caller's applications.
func (h *Handler) ListApplications(ctx context.Context, req *applicationv1.ListApplicationsRequest) (*applicationv1.ListApplicationsResponse, error) {
	in := app.ListInput{PageSize: int(req.GetPageSize()), PageToken: req.GetPageToken()}
	if req.GetJobId() != "" {
		id, err := parseID(req.GetJobId())
		if err != nil {
			return nil, err
		}
		in.JobID = &id
	}
	if req.GetStatus() != applicationv1.ApplicationStatus_APPLICATION_STATUS_UNSPECIFIED {
		st, err := statusFromProto(req.GetStatus())
		if err != nil {
			return nil, err
		}
		in.Status = &st
	}
	as, next, err := h.uc.List(ctx, in)
	if err != nil {
		return nil, err
	}
	resp := &applicationv1.ListApplicationsResponse{Applications: make([]*applicationv1.Application, len(as)), NextPageToken: next}
	for i, a := range as {
		if resp.Applications[i], err = applicationToProto(a); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

// UpdateApplicationStatus moves an application (recruiter or admin).
func (h *Handler) UpdateApplicationStatus(ctx context.Context, req *applicationv1.UpdateApplicationStatusRequest) (*applicationv1.Application, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	to, err := statusFromProto(req.GetStatus())
	if err != nil {
		return nil, err
	}
	a, err := h.uc.UpdateStatus(ctx, id, to)
	if err != nil {
		return nil, err
	}
	return applicationToProto(a)
}
