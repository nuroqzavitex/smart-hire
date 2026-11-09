package domain_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

func TestNewEmail_Normalizes(t *testing.T) {
	e, err := domain.NewEmail("  Foo@X.com ")
	require.NoError(t, err)
	assert.Equal(t, "foo@x.com", e)
	for _, bad := range []string{"", "x", "a@", "Foo <foo@x.com>", strings.Repeat("a", 250) + "@x.com"} {
		_, err := domain.NewEmail(bad)
		assert.ErrorIs(t, err, apperr.ErrValidation, bad)
	}
}

func TestValidatePassword(t *testing.T) {
	assert.ErrorIs(t, domain.ValidatePassword("1234567"), apperr.ErrValidation)
	assert.NoError(t, domain.ValidatePassword("12345678"))
	assert.NoError(t, domain.ValidatePassword(strings.Repeat("é", 128)))
	assert.ErrorIs(t, domain.ValidatePassword(strings.Repeat("a", 129)), apperr.ErrValidation)
}

func TestUser_CanAuthenticate(t *testing.T) {
	assert.NoError(t, domain.User{Status: domain.StatusActive}.CanAuthenticate())
	assert.ErrorIs(t, domain.User{Status: domain.StatusLocked}.CanAuthenticate(), domain.ErrAccountLocked)
	assert.ErrorIs(t, domain.User{Status: domain.StatusDeletionPending}.CanAuthenticate(), domain.ErrAccountPendingDeletion)
	assert.ErrorIs(t, domain.User{Status: "BOGUS"}.CanAuthenticate(), apperr.ErrForbidden)
}
