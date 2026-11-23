package grpc

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	applicationv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/application/v1"
	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

const statusPrefix = "APPLICATION_STATUS_"

func parseID(s string) (uuid.UUID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, apperr.New(apperr.ErrValidation, "id must be a uuid")
	}
	return id, nil
}

func statusToProto(s domain.Status) applicationv1.ApplicationStatus {
	return applicationv1.ApplicationStatus(applicationv1.ApplicationStatus_value[statusPrefix+string(s)])
}

func statusFromProto(s applicationv1.ApplicationStatus) (domain.Status, error) {
	st, err := domain.ParseStatus(strings.TrimPrefix(s.String(), statusPrefix))
	if err != nil {
		return "", apperr.New(apperr.ErrValidation, "invalid status")
	}
	return st, nil
}

type breakdown struct {
	SemanticSimilarity    float64  `json:"semantic_similarity"`
	RequiredSkillCoverage float64  `json:"required_skill_coverage"`
	ExperienceFit         float64  `json:"experience_fit"`
	LocationFit           float64  `json:"location_fit"`
	MatchedSkills         []string `json:"matched_skills"`
	MissingSkills         []string `json:"missing_skills"`
}

func applicationToProto(a domain.Application) (*applicationv1.Application, error) {
	out := &applicationv1.Application{
		Id: a.ID.String(), JobId: a.JobID.String(), CandidateId: a.CandidateID.String(), CvId: a.CVID.String(),
		Status: statusToProto(a.Status), Reason: a.Reason, Score: a.Score, ModelVersion: a.ModelVersion,
		Version: a.Version, CreatedAt: timestamppb.New(a.CreatedAt), UpdatedAt: timestamppb.New(a.UpdatedAt),
	}
	if len(a.Breakdown) > 0 {
		var b breakdown
		if err := json.Unmarshal(a.Breakdown, &b); err != nil {
			return nil, fmt.Errorf("decode score breakdown: %w", err)
		}
		out.Breakdown = &applicationv1.ScoreBreakdown{
			SemanticSimilarity: b.SemanticSimilarity, RequiredSkillCoverage: b.RequiredSkillCoverage,
			ExperienceFit: b.ExperienceFit, LocationFit: b.LocationFit,
			MatchedSkills: b.MatchedSkills, MissingSkills: b.MissingSkills,
		}
	}
	return out, nil
}

func historyToProto(e domain.HistoryEntry) *applicationv1.HistoryEntry {
	out := &applicationv1.HistoryEntry{To: statusToProto(e.To), Actor: e.Actor, Reason: e.Reason, At: timestamppb.New(e.At)}
	if e.From != nil {
		out.From = statusToProto(*e.From)
	}
	return out
}
