package grpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	jobv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/job/v1"
	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

func TestLevelFromProto_AllEnums(t *testing.T) {
	cases := map[jobv1.JobLevel]domain.Level{
		jobv1.JobLevel_JOB_LEVEL_INTERN: domain.LevelIntern,
		jobv1.JobLevel_JOB_LEVEL_JUNIOR: domain.LevelJunior,
		jobv1.JobLevel_JOB_LEVEL_MIDDLE: domain.LevelMiddle,
		jobv1.JobLevel_JOB_LEVEL_SENIOR: domain.LevelSenior,
		jobv1.JobLevel_JOB_LEVEL_LEAD:   domain.LevelLead,
	}
	for in, want := range cases {
		got, ok := levelFromProto(in)
		assert.True(t, ok, in.String())
		assert.Equal(t, want, got, in.String())
		assert.Equal(t, in, levelToProto(got))
	}
	_, ok := levelFromProto(jobv1.JobLevel_JOB_LEVEL_UNSPECIFIED)
	assert.False(t, ok)
}

func TestParseLevelString_CaseInsensitive(t *testing.T) {
	for _, in := range []string{"Senior", "SENIOR", " senior "} {
		got, err := parseLevelString(in)
		require.NoError(t, err, in)
		assert.Equal(t, domain.LevelSenior, got, in)
	}
	_, err := parseLevelString("wizard")
	assert.ErrorIs(t, err, apperr.ErrValidation)
}

func TestFilterFromProto_LowercasesSkills(t *testing.T) {
	f := filterFromProto(&jobv1.ListJobsRequest{Skills: []string{"Go", "PostgreSQL"}, Level: jobv1.JobLevel_JOB_LEVEL_SENIOR})
	assert.Equal(t, []string{"go", "postgresql"}, f.Skills)
	require.NotNil(t, f.Level)
	assert.Equal(t, domain.LevelSenior, *f.Level)
}

func TestParseID_Invalid(t *testing.T) {
	_, err := parseID("nope")
	assert.ErrorIs(t, err, apperr.ErrValidation)
}
