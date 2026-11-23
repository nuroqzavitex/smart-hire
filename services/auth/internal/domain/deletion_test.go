package domain_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

func TestNewDeletionRequest(t *testing.T) {
	at := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	user := uuid.New()
	r := domain.NewDeletionRequest(user, authz.RoleCandidate, []string{"job", "ai"}, at)
	assert.NotEqual(t, uuid.Nil, r.ID)
	assert.Equal(t, user, r.UserID)
	assert.Equal(t, domain.DeletionPending, r.Status)
	assert.Equal(t, []string{"job", "ai"}, r.Expected)
	assert.Empty(t, r.Purged)
	assert.Equal(t, at, r.RequestedAt)
	assert.Equal(t, at, r.LastPublishedAt)
	assert.Nil(t, r.CompletedAt)
}

func TestDeletionRequest_MarkPurgedIsASet(t *testing.T) {
	at := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	r := domain.NewDeletionRequest(uuid.New(), authz.RoleCandidate, []string{"job", "ai"}, at)

	done, err := r.MarkPurged("ai", at)
	require.NoError(t, err)
	assert.False(t, done)
	done, err = r.MarkPurged("ai", at)
	require.NoError(t, err)
	assert.False(t, done, "a duplicate confirmation does not count twice")
	assert.Equal(t, []string{"ai"}, r.Purged)

	_, err = r.MarkPurged("auth", at)
	assert.ErrorIs(t, err, domain.ErrUnknownService)

	later := at.Add(time.Minute)
	done, err = r.MarkPurged("job", later)
	require.NoError(t, err)
	assert.True(t, done)
	assert.Equal(t, domain.DeletionCompleted, r.Status)
	require.NotNil(t, r.CompletedAt)
	assert.Equal(t, later, *r.CompletedAt)
}

func TestDeletionRequest_StuckStillCompletes(t *testing.T) {
	at := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	r := domain.NewDeletionRequest(uuid.New(), authz.RoleRecruiter, []string{"job"}, at)
	r.Status = domain.DeletionStuck
	done, err := r.MarkPurged("job", at)
	require.NoError(t, err)
	assert.True(t, done)
	assert.Equal(t, domain.DeletionCompleted, r.Status)
}

func TestDeletionRequest_CompletedIgnoresLateConfirmations(t *testing.T) {
	at := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	r := domain.NewDeletionRequest(uuid.New(), authz.RoleRecruiter, []string{"job"}, at)
	_, err := r.MarkPurged("job", at)
	require.NoError(t, err)
	done, err := r.MarkPurged("job", at.Add(time.Hour))
	require.NoError(t, err)
	assert.False(t, done, "already completed: nothing left to do")
	assert.Equal(t, at, *r.CompletedAt)
}
