package grpc

import (
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	candidatev1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/candidate/v1"
	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

// errCorruptParsedData is returned when stored parsed data cannot be decoded.
// It never carries the data itself (PII); clients see "internal error".
var errCorruptParsedData = errors.New("stored parsed data is not a valid ParsedCV")

func parseID(s string) (uuid.UUID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, apperr.New(apperr.ErrValidation, "id must be a uuid")
	}
	return id, nil
}

func profileToProto(c domain.Candidate) *candidatev1.Profile {
	return &candidatev1.Profile{
		UserId:    c.UserID.String(),
		FullName:  c.FullName,
		Phone:     c.Phone,
		Location:  c.Location,
		CreatedAt: timestamppb.New(c.CreatedAt),
		UpdatedAt: timestamppb.New(c.UpdatedAt),
	}
}

func ticketToProto(t app.UploadTicket) *candidatev1.CreateUploadURLResponse {
	return &candidatev1.CreateUploadURLResponse{
		CvId:      t.CVID.String(),
		UploadUrl: t.URL,
		ExpiresAt: timestamppb.New(t.ExpiresAt),
	}
}

func statusToProto(s domain.Status) candidatev1.CVStatus {
	switch s {
	case domain.StatusUploading:
		return candidatev1.CVStatus_CV_STATUS_UPLOADING
	case domain.StatusUploaded:
		return candidatev1.CVStatus_CV_STATUS_UPLOADED
	case domain.StatusParsed:
		return candidatev1.CVStatus_CV_STATUS_PARSED
	case domain.StatusFailed:
		return candidatev1.CVStatus_CV_STATUS_FAILED
	}
	return candidatev1.CVStatus_CV_STATUS_UNSPECIFIED
}

// cvToProto maps a CV for its owner. The object key is never exposed.
func cvToProto(cv domain.CV) (*candidatev1.CV, error) {
	out := &candidatev1.CV{
		Id:        cv.ID.String(),
		Status:    statusToProto(cv.Status),
		FileName:  cv.FileName,
		SizeBytes: cv.SizeBytes,
		Version:   cv.Version,
		CreatedAt: timestamppb.New(cv.CreatedAt),
		UpdatedAt: timestamppb.New(cv.UpdatedAt),
	}
	if cv.ParseError != nil {
		out.ParseError = *cv.ParseError
	}
	if cv.ParsedAt != nil {
		out.ParsedAt = timestamppb.New(*cv.ParsedAt)
	}
	if len(cv.ParsedData) > 0 {
		var p domain.ParsedCV
		if err := json.Unmarshal(cv.ParsedData, &p); err != nil {
			return nil, errCorruptParsedData
		}
		out.Parsed = parsedToProto(p)
	}
	return out, nil
}

func parsedToProto(p domain.ParsedCV) *candidatev1.ParsedCV {
	out := &candidatev1.ParsedCV{
		FullName:          p.FullName,
		Email:             p.Email,
		Skills:            p.Skills,
		YearsOfExperience: p.YearsOfExperience,
		CurrentTitle:      p.CurrentTitle,
		Education:         make([]*candidatev1.Education, len(p.Education)),
		Experiences:       make([]*candidatev1.Experience, len(p.Experiences)),
		Languages:         p.Languages,
		Location:          p.Location,
	}
	for i, e := range p.Education {
		out.Education[i] = &candidatev1.Education{
			School: e.School, Degree: e.Degree, Major: e.Major, EndYear: toInt32(e.EndYear),
		}
	}
	for i, e := range p.Experiences {
		out.Experiences[i] = &candidatev1.Experience{
			Company: e.Company, Title: e.Title, Start: e.Start, End: e.End, Highlights: e.Highlights,
		}
	}
	return out
}

// parsedFromProto maps an edited ParsedCV: unset optionals become nil and
// repeated fields are always non-nil slices.
func parsedFromProto(p *candidatev1.ParsedCV) domain.ParsedCV {
	out := domain.ParsedCV{
		FullName:          p.FullName,
		Email:             p.Email,
		Skills:            clone(p.GetSkills()),
		YearsOfExperience: p.YearsOfExperience,
		CurrentTitle:      p.CurrentTitle,
		Education:         make([]domain.Education, len(p.GetEducation())),
		Experiences:       make([]domain.Experience, len(p.GetExperiences())),
		Languages:         clone(p.GetLanguages()),
		Location:          p.Location,
	}
	for i, e := range p.GetEducation() {
		out.Education[i] = domain.Education{
			School: e.GetSchool(), Degree: e.Degree, Major: e.Major, EndYear: fromInt32(e.EndYear),
		}
	}
	for i, e := range p.GetExperiences() {
		out.Experiences[i] = domain.Experience{
			Company: e.GetCompany(), Title: e.GetTitle(), Start: e.Start, End: e.End,
			Highlights: clone(e.GetHighlights()),
		}
	}
	return out
}

func clone(s []string) []string { return append([]string{}, s...) }

func toInt32(v *int) *int32 {
	if v == nil {
		return nil
	}
	x := int32(*v) //nolint:gosec // end_year is schema-bounded to 1900-2100
	return &x
}

func fromInt32(v *int32) *int {
	if v == nil {
		return nil
	}
	x := int(*v)
	return &x
}
