package grpc

import (
	"strings"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	jobv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/job/v1"
	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

func parseID(s string) (uuid.UUID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, apperr.New(apperr.ErrValidation, "id must be a uuid")
	}
	return id, nil
}

// parseLevelString parses a level case-insensitively (spec §4.3);
// domain.ParseLevel itself is case-sensitive.
func parseLevelString(s string) (domain.Level, error) {
	return domain.ParseLevel(strings.ToLower(strings.TrimSpace(s)))
}

// levelFromProto maps the enum to a domain level; unspecified or unknown
// values report false.
func levelFromProto(l jobv1.JobLevel) (domain.Level, bool) {
	if l == jobv1.JobLevel_JOB_LEVEL_UNSPECIFIED {
		return "", false
	}
	v, err := parseLevelString(strings.TrimPrefix(l.String(), "JOB_LEVEL_"))
	if err != nil {
		return "", false
	}
	return v, true
}

func levelToProto(l domain.Level) jobv1.JobLevel {
	if v, ok := jobv1.JobLevel_value["JOB_LEVEL_"+strings.ToUpper(string(l))]; ok {
		return jobv1.JobLevel(v)
	}
	return jobv1.JobLevel_JOB_LEVEL_UNSPECIFIED
}

func statusToProto(s domain.Status) jobv1.JobStatus {
	switch s {
	case domain.StatusDraft:
		return jobv1.JobStatus_JOB_STATUS_DRAFT
	case domain.StatusOpen:
		return jobv1.JobStatus_JOB_STATUS_OPEN
	case domain.StatusClosed:
		return jobv1.JobStatus_JOB_STATUS_CLOSED
	}
	return jobv1.JobStatus_JOB_STATUS_UNSPECIFIED
}

// statusFromProto reports false for unspecified (meaning "any status").
func statusFromProto(s jobv1.JobStatus) (domain.Status, bool) {
	switch s {
	case jobv1.JobStatus_JOB_STATUS_DRAFT:
		return domain.StatusDraft, true
	case jobv1.JobStatus_JOB_STATUS_OPEN:
		return domain.StatusOpen, true
	case jobv1.JobStatus_JOB_STATUS_CLOSED:
		return domain.StatusClosed, true
	}
	return "", false
}

func toProto(j domain.Job) *jobv1.Job {
	out := &jobv1.Job{
		Id:               j.ID.String(),
		RecruiterId:      j.RecruiterID.String(),
		Title:            j.Title,
		Description:      j.Description,
		RequiredSkills:   j.RequiredSkills,
		NiceToHaveSkills: j.NiceToHaveSkills,
		Level:            levelToProto(j.Level),
		Location:         j.Location,
		SalaryMin:        toInt32(j.SalaryMin),
		SalaryMax:        toInt32(j.SalaryMax),
		Status:           statusToProto(j.Status),
		Version:          j.Version,
		CreatedAt:        timestamppb.New(j.CreatedAt),
		UpdatedAt:        timestamppb.New(j.UpdatedAt),
	}
	if j.PublishedAt != nil {
		out.PublishedAt = timestamppb.New(*j.PublishedAt)
	}
	return out
}

func draftFromProto(req *jobv1.CreateJobRequest) (domain.Draft, error) {
	level, ok := levelFromProto(req.GetLevel())
	if !ok {
		return domain.Draft{}, apperr.New(apperr.ErrValidation, "invalid job level")
	}
	return domain.Draft{
		Title:            req.GetTitle(),
		Description:      req.GetDescription(),
		RequiredSkills:   req.GetRequiredSkills(),
		NiceToHaveSkills: req.GetNiceToHaveSkills(),
		Level:            level,
		Location:         req.GetLocation(),
		SalaryMin:        fromInt32(req.SalaryMin),
		SalaryMax:        fromInt32(req.SalaryMax),
	}, nil
}

func patchFromProto(req *jobv1.UpdateJobRequest) (domain.Patch, error) {
	p := domain.Patch{
		Title:       req.Title,
		Description: req.Description,
		Location:    req.Location,
		SalaryMin:   fromInt32(req.SalaryMin),
		SalaryMax:   fromInt32(req.SalaryMax),
	}
	if req.RequiredSkills != nil {
		v := append([]string{}, req.RequiredSkills.GetValues()...)
		p.RequiredSkills = &v
	}
	if req.NiceToHaveSkills != nil {
		v := append([]string{}, req.NiceToHaveSkills.GetValues()...)
		p.NiceToHaveSkills = &v
	}
	if req.Level != nil {
		l, ok := levelFromProto(req.GetLevel())
		if !ok {
			return domain.Patch{}, apperr.New(apperr.ErrValidation, "invalid job level")
		}
		p.Level = &l
	}
	return p, nil
}

func filterFromProto(req *jobv1.ListJobsRequest) app.SearchFilter {
	f := app.SearchFilter{
		Query:     req.GetQ(),
		Location:  req.GetLocation(),
		SalaryMin: fromInt32(req.SalaryMin),
	}
	for _, s := range req.GetSkills() {
		f.Skills = append(f.Skills, strings.ToLower(strings.TrimSpace(s)))
	}
	if l, ok := levelFromProto(req.GetLevel()); ok {
		f.Level = &l
	}
	return f
}

func pageToProto(p app.Page) *jobv1.ListJobsResponse {
	out := &jobv1.ListJobsResponse{Jobs: make([]*jobv1.Job, len(p.Jobs)), NextPageToken: p.NextToken}
	for i, j := range p.Jobs {
		out.Jobs[i] = toProto(j)
	}
	return out
}

func toInt32(v *int) *int32 {
	if v == nil {
		return nil
	}
	x := int32(*v) //nolint:gosec // bounded by int32 proto validation on input
	return &x
}

func fromInt32(v *int32) *int {
	if v == nil {
		return nil
	}
	x := int(*v)
	return &x
}
