// Package grpc adapts the job use cases to the generated JobService gRPC API.
package grpc

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/emptypb"

	jobv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/job/v1"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

// PublicMethods are the RPCs callable without an access token.
var PublicMethods = []string{
	jobv1.JobService_GetJob_FullMethodName,
	jobv1.JobService_ListJobs_FullMethodName,
}

// UseCases is satisfied by *app.Service.
type UseCases interface {
	Create(ctx context.Context, d domain.Draft) (domain.Job, error)
	Update(ctx context.Context, id uuid.UUID, p domain.Patch) (domain.Job, error)
	Publish(ctx context.Context, id uuid.UUID) (domain.Job, error)
	Close(ctx context.Context, id uuid.UUID) (domain.Job, error)
	Delete(ctx context.Context, id uuid.UUID) error
	Get(ctx context.Context, id uuid.UUID) (domain.Job, error)
	ListOpen(ctx context.Context, f app.SearchFilter, pr app.PageRequest) (app.Page, error)
	ListMine(ctx context.Context, status *domain.Status, pr app.PageRequest) (app.Page, error)
}

// Handler implements jobv1.JobServiceServer.
type Handler struct {
	jobv1.UnimplementedJobServiceServer
	uc UseCases
}

// NewHandler returns the JobService implementation backed by uc.
func NewHandler(uc UseCases) *Handler { return &Handler{uc: uc} }

// CreateJob creates a draft job.
func (h *Handler) CreateJob(ctx context.Context, req *jobv1.CreateJobRequest) (*jobv1.Job, error) {
	d, err := draftFromProto(req)
	if err != nil {
		return nil, err
	}
	j, err := h.uc.Create(ctx, d)
	if err != nil {
		return nil, err
	}
	return toProto(j), nil
}

// UpdateJob applies the fields present in the request.
func (h *Handler) UpdateJob(ctx context.Context, req *jobv1.UpdateJobRequest) (*jobv1.Job, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	p, err := patchFromProto(req)
	if err != nil {
		return nil, err
	}
	j, err := h.uc.Update(ctx, id, p)
	if err != nil {
		return nil, err
	}
	return toProto(j), nil
}

// PublishJob opens a draft job.
func (h *Handler) PublishJob(ctx context.Context, req *jobv1.PublishJobRequest) (*jobv1.Job, error) {
	return h.transition(ctx, req.GetId(), h.uc.Publish)
}

// CloseJob closes an open job.
func (h *Handler) CloseJob(ctx context.Context, req *jobv1.CloseJobRequest) (*jobv1.Job, error) {
	return h.transition(ctx, req.GetId(), h.uc.Close)
}

func (h *Handler) transition(ctx context.Context, rawID string,
	fn func(context.Context, uuid.UUID) (domain.Job, error),
) (*jobv1.Job, error) {
	id, err := parseID(rawID)
	if err != nil {
		return nil, err
	}
	j, err := fn(ctx, id)
	if err != nil {
		return nil, err
	}
	return toProto(j), nil
}

// DeleteJob soft-deletes a job.
func (h *Handler) DeleteJob(ctx context.Context, req *jobv1.DeleteJobRequest) (*emptypb.Empty, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	if err := h.uc.Delete(ctx, id); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// GetJob returns one job.
func (h *Handler) GetJob(ctx context.Context, req *jobv1.GetJobRequest) (*jobv1.Job, error) {
	return h.transition(ctx, req.GetId(), h.uc.Get)
}

// ListJobs searches open jobs.
func (h *Handler) ListJobs(ctx context.Context, req *jobv1.ListJobsRequest) (*jobv1.ListJobsResponse, error) {
	page, err := h.uc.ListOpen(ctx, filterFromProto(req),
		app.PageRequest{Size: int(req.GetPageSize()), Token: req.GetPageToken()})
	if err != nil {
		return nil, err
	}
	return pageToProto(page), nil
}

// ListMyJobs lists the calling recruiter's jobs.
func (h *Handler) ListMyJobs(ctx context.Context, req *jobv1.ListMyJobsRequest) (*jobv1.ListJobsResponse, error) {
	var st *domain.Status
	if s, ok := statusFromProto(req.GetStatus()); ok {
		st = &s
	}
	page, err := h.uc.ListMine(ctx, st, app.PageRequest{Size: int(req.GetPageSize()), Token: req.GetPageToken()})
	if err != nil {
		return nil, err
	}
	return pageToProto(page), nil
}
