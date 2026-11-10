package app_test

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
)

func b64(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

func TestDecodeCursor_Rejects(t *testing.T) {
	for _, s := range []string{
		"!!!",
		b64("not json"),
		b64(`{"p":"2026-01-01T00:00:00Z"}`),
		b64(`{"id":"` + uuid.NewString() + `"}`),
		b64(`{"id":"` + uuid.Nil.String() + `","p":"2026-01-01T00:00:00Z"}`),
	} {
		_, err := app.DecodeCursor(s)
		assert.ErrorIs(t, err, apperr.ErrValidation, s)
	}
}

func TestCursor_RoundTrip(t *testing.T) {
	rk := float32(0.5)
	for _, c := range []app.Cursor{
		{Rank: &rk, At: time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC), ID: uuid.New()},
		{At: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), ID: uuid.New()},
	} {
		got, err := app.DecodeCursor(app.EncodeCursor(c))
		require.NoError(t, err)
		assert.Equal(t, c.ID, got.ID)
		assert.True(t, c.At.Equal(got.At))
		assert.Equal(t, c.Rank, got.Rank)
	}
}
