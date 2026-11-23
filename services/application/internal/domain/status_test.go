package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

func TestCanRecruiterMove(t *testing.T) {
	allowed := map[[2]domain.Status]bool{
		{domain.StatusScreened, domain.StatusInterview}:          true,
		{domain.StatusScreened, domain.StatusRejected}:           true,
		{domain.StatusNeedsManualReview, domain.StatusInterview}: true,
		{domain.StatusNeedsManualReview, domain.StatusRejected}:  true,
		{domain.StatusInterview, domain.StatusOffered}:           true,
		{domain.StatusInterview, domain.StatusRejected}:          true,
		{domain.StatusOffered, domain.StatusHired}:               true,
		{domain.StatusOffered, domain.StatusRejected}:            true,
	}
	for _, from := range domain.AllStatuses {
		for _, to := range domain.AllStatuses {
			want := allowed[[2]domain.Status{from, to}]
			assert.Equal(t, want, domain.CanRecruiterMove(from, to), "%s -> %s", from, to)
		}
	}
}

func TestParseStatus(t *testing.T) {
	for _, s := range domain.AllStatuses {
		got, err := domain.ParseStatus(string(s))
		assert.NoError(t, err)
		assert.Equal(t, s, got)
	}
	assert.Len(t, domain.AllStatuses, 9)
	for _, bad := range []string{"", "pending", "UNKNOWN"} {
		_, err := domain.ParseStatus(bad)
		assert.Error(t, err, bad)
	}
}
