// Package grpc adapts the candidate use cases to the generated
// CandidateService gRPC API.
package grpc

import (
	"context"

	"github.com/google/uuid"

	candidatev1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/candidate/v1"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

// PublicMethods are the RPCs callable without an access token: none.
var PublicMethods = []string{}

// UseCases is satisfied by *app.Service.
type UseCases interface {
	GetProfile(ctx context.Context) (domain.Candidate, error)
	UpsertProfile(ctx context.Context, in domain.ProfileInput) (domain.Candidate, error)
	CreateUploadURL(ctx context.Context, consent bool, fileName string) (app.UploadTicket, error)
	CompleteUpload(ctx context.Context, id uuid.UUID) (domain.CV, error)
	GetCV(ctx context.Context, id uuid.UUID) (domain.CV, error)
	ListMyCVs(ctx context.Context, pr app.PageRequest) (app.Page, error)
	UpdateParsedData(ctx context.Context, id uuid.UUID, p domain.ParsedCV) (domain.CV, error)
	ReparseCV(ctx context.Context, id uuid.UUID) (domain.CV, error)
	ListRecommendedJobs(ctx context.Context, cvID uuid.UUID, topK int) (app.RecommendedJobs, error)
}

// Handler implements candidatev1.CandidateServiceServer.
type Handler struct {
	candidatev1.UnimplementedCandidateServiceServer
	uc UseCases
}

// NewHandler returns the CandidateService implementation backed by uc.
func NewHandler(uc UseCases) *Handler { return &Handler{uc: uc} }

// GetMyProfile returns the caller's profile.
func (h *Handler) GetMyProfile(ctx context.Context, _ *candidatev1.GetMyProfileRequest) (*candidatev1.Profile, error) {
	c, err := h.uc.GetProfile(ctx)
	if err != nil {
		return nil, err
	}
	return profileToProto(c), nil
}

// UpsertMyProfile creates or replaces the caller's profile.
func (h *Handler) UpsertMyProfile(ctx context.Context, req *candidatev1.UpsertMyProfileRequest) (*candidatev1.Profile, error) {
	c, err := h.uc.UpsertProfile(ctx, domain.ProfileInput{
		FullName: req.GetFullName(),
		Phone:    req.Phone,
		Location: req.Location,
	})
	if err != nil {
		return nil, err
	}
	return profileToProto(c), nil
}

// CreateUploadURL records consent and returns a presigned PUT URL for a new CV.
func (h *Handler) CreateUploadURL(ctx context.Context, req *candidatev1.CreateUploadURLRequest) (*candidatev1.CreateUploadURLResponse, error) {
	t, err := h.uc.CreateUploadURL(ctx, req.GetConsent(), req.GetFileName())
	if err != nil {
		return nil, err
	}
	return ticketToProto(t), nil
}

// CompleteUpload verifies the uploaded object and starts parsing.
func (h *Handler) CompleteUpload(ctx context.Context, req *candidatev1.CompleteUploadRequest) (*candidatev1.CV, error) {
	return h.cv(ctx, req.GetId(), h.uc.CompleteUpload)
}

// GetCV returns one of the caller's CVs.
func (h *Handler) GetCV(ctx context.Context, req *candidatev1.GetCVRequest) (*candidatev1.CV, error) {
	return h.cv(ctx, req.GetId(), h.uc.GetCV)
}

// ReparseCV discards the parse result and starts a new attempt.
func (h *Handler) ReparseCV(ctx context.Context, req *candidatev1.ReparseCVRequest) (*candidatev1.CV, error) {
	return h.cv(ctx, req.GetId(), h.uc.ReparseCV)
}

// ListMyCVs pages through the caller's CVs.
func (h *Handler) ListMyCVs(ctx context.Context, req *candidatev1.ListMyCVsRequest) (*candidatev1.ListMyCVsResponse, error) {
	page, err := h.uc.ListMyCVs(ctx, app.PageRequest{Size: int(req.GetPageSize()), Token: req.GetPageToken()})
	if err != nil {
		return nil, err
	}
	out := &candidatev1.ListMyCVsResponse{Cvs: make([]*candidatev1.CV, len(page.CVs)), NextPageToken: page.NextToken}
	for i, cv := range page.CVs {
		if out.Cvs[i], err = cvToProto(cv); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// UpdateParsedData replaces the parsed data of a PARSED CV.
func (h *Handler) UpdateParsedData(ctx context.Context, req *candidatev1.UpdateParsedDataRequest) (*candidatev1.CV, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	cv, err := h.uc.UpdateParsedData(ctx, id, parsedFromProto(req.GetParsed()))
	if err != nil {
		return nil, err
	}
	return cvToProto(cv)
}

func (h *Handler) cv(ctx context.Context, rawID string,
	fn func(context.Context, uuid.UUID) (domain.CV, error),
) (*candidatev1.CV, error) {
	id, err := parseID(rawID)
	if err != nil {
		return nil, err
	}
	cv, err := fn(ctx, id)
	if err != nil {
		return nil, err
	}
	return cvToProto(cv)
}

// ListRecommendedJobs ranks open jobs for one of the caller's parsed CVs.
func (h *Handler) ListRecommendedJobs(ctx context.Context, req *candidatev1.ListRecommendedJobsRequest) (*candidatev1.ListRecommendedJobsResponse, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	recs, err := h.uc.ListRecommendedJobs(ctx, id, int(req.GetTopK()))
	if err != nil {
		return nil, err
	}
	return recommendationsToProto(recs), nil
}
