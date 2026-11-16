package app_test

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/event/eventschema"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

func TestGetCV_OwnerOnly(t *testing.T) {
	e := newEnv(t)
	cv := e.newUpload(t, u1)
	got, err := e.svc.GetCV(asCandidate(u1), cv.ID)
	require.NoError(t, err)
	assert.Equal(t, cv, got)

	_, err = e.svc.GetCV(asCandidate(u2), cv.ID)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	_, err = e.svc.GetCV(asCandidate(u1), uuid.New())
	assert.ErrorIs(t, err, apperr.ErrNotFound)
}

func TestUpdateParsedData(t *testing.T) {
	e := newEnv(t)
	skills := func(n int, s string) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = s
		}
		return out
	}
	name := "Trần Thị Bích"
	edit := domain.ParsedCV{FullName: &name, Skills: []string{"Go", "Rust"},
		Experiences: []domain.Experience{{Company: "Acme", Title: "Dev"}}}

	t.Run("only when parsed", func(t *testing.T) {
		cv := e.uploaded(t, u1)
		_, err := e.svc.UpdateParsedData(asCandidate(u1), cv.ID, edit)
		assert.ErrorIs(t, err, apperr.ErrValidation)
		assert.Equal(t, cv, e.cvs.m[cv.ID])
	})

	t.Run("valid edit", func(t *testing.T) {
		cv := e.parsed(t, u1)
		events := e.ob.count()
		out, err := e.svc.UpdateParsedData(asCandidate(u1), cv.ID, edit)
		require.NoError(t, err)
		assert.Equal(t, cv.Version+1, out.Version)
		want, err := edit.MarshalCanonical()
		require.NoError(t, err)
		assert.JSONEq(t, string(want), string(out.ParsedData))
		assert.NoError(t, eventschema.ValidateDef("cv.parsed", "ParsedCV", out.ParsedData))
		assert.Equal(t, out, e.cvs.m[cv.ID])
		assert.Equal(t, events, e.ob.count(), "no event")
	})

	t.Run("fixture edit", func(t *testing.T) {
		cv := e.parsed(t, u1)
		var p domain.ParsedCV
		require.NoError(t, json.Unmarshal(fixture(t, "valid/full.json"), &p))
		out, err := e.svc.UpdateParsedData(asCandidate(u1), cv.ID, p)
		require.NoError(t, err)
		assert.JSONEq(t, string(fixture(t, "valid/full.json")), string(out.ParsedData))
	})

	t.Run("schema violation", func(t *testing.T) {
		cv := e.parsed(t, u1)
		_, err := e.svc.UpdateParsedData(asCandidate(u1), cv.ID, domain.ParsedCV{Skills: skills(51, "SecretSkillZ")})
		require.ErrorIs(t, err, apperr.ErrValidation)
		assert.EqualError(t, err, "invalid parsed data")
		assert.NotContains(t, err.Error(), "SecretSkillZ")
		assert.Equal(t, cv, e.cvs.m[cv.ID])
	})

	t.Run("non owner", func(t *testing.T) {
		cv := e.parsed(t, u1)
		_, err := e.svc.UpdateParsedData(asCandidate(u2), cv.ID, edit)
		assert.ErrorIs(t, err, apperr.ErrNotFound)
		assert.Equal(t, cv, e.cvs.m[cv.ID])
	})
}

func TestReparse_EmitsNewAttempt(t *testing.T) {
	e := newEnv(t)
	cv := e.parsed(t, u1)
	events := e.ob.count()

	out, err := e.svc.ReparseCV(asCandidate(u1), cv.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusUploaded, out.Status)
	assert.NotEqual(t, cv.AttemptID, out.AttemptID)
	assert.Nil(t, out.ParsedData)
	assert.Nil(t, out.ParsedAt)
	assert.Equal(t, cv.Version+1, out.Version)
	assert.Equal(t, out, e.cvs.m[cv.ID])

	require.Equal(t, events+1, e.ob.count())
	ev := e.ob.last()
	assert.Equal(t, "cv.uploaded", ev.EventType)
	assert.Equal(t, cv.ID.String(), e.ob.lastKey())
	raw, err := json.Marshal(ev)
	require.NoError(t, err)
	assert.NoError(t, eventschema.Validate("cv.uploaded", raw))
	var p map[string]string
	require.NoError(t, json.Unmarshal(ev.Payload, &p))
	assert.Equal(t, out.AttemptID, p["attempt_id"])

	// Failed CVs can be reparsed too.
	f := e.uploaded(t, u1)
	require.NoError(t, e.svc.HandleParseResult(t.Context(), failedEvent(t, f, f.AttemptID, "no_text")))
	out, err = e.svc.ReparseCV(asCandidate(u1), f.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusUploaded, out.Status)
	assert.Nil(t, out.ParseError)

	up := e.newUpload(t, u1)
	n := e.ob.count()
	_, err = e.svc.ReparseCV(asCandidate(u1), up.ID)
	assert.ErrorIs(t, err, apperr.ErrValidation)
	_, err = e.svc.ReparseCV(asCandidate(u2), cv.ID)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	assert.Equal(t, n, e.ob.count())
	assert.Equal(t, domain.StatusUploading, e.cvs.m[up.ID].Status)
}

func TestReparse_StaleUploaded(t *testing.T) {
	e := newEnv(t)
	cv := e.uploaded(t, u1) // attempt A, its cv.uploaded may have been dead-lettered
	attemptA := cv.AttemptID
	n := e.ob.count()

	// Still within the threshold: the attempt may be in flight.
	e.clock.Advance(domain.StaleUploadAfter - 2*time.Second)
	_, err := e.svc.ReparseCV(asCandidate(u1), cv.ID)
	require.ErrorIs(t, err, apperr.ErrValidation)
	assert.Equal(t, n, e.ob.count())
	assert.Equal(t, cv, e.cvs.m[cv.ID])

	e.clock.Advance(2 * time.Second)
	out, err := e.svc.ReparseCV(asCandidate(u1), cv.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusUploaded, out.Status)
	assert.NotEqual(t, attemptA, out.AttemptID)
	assert.Equal(t, cv.Version+1, out.Version)
	require.Equal(t, n+1, e.ob.count())
	ev := e.ob.last()
	assert.Equal(t, "cv.uploaded", ev.EventType)
	var p map[string]string
	require.NoError(t, json.Unmarshal(ev.Payload, &p))
	assert.Equal(t, out.AttemptID, p["attempt_id"])

	// A late result of attempt A is ignored; the new attempt's result applies.
	require.NoError(t, e.svc.HandleParseResult(t.Context(), failedEvent(t, cv, attemptA, "no_text")))
	assert.Equal(t, out, e.cvs.m[cv.ID])
	require.NoError(t, e.svc.HandleParseResult(t.Context(), parsedEvent(t, out, out.AttemptID, fixture(t, "valid/minimal.json"))))
	assert.Equal(t, domain.StatusParsed, e.cvs.m[cv.ID].Status)
}

func TestListMyCVs_PagesOwnOnly(t *testing.T) {
	e := newEnv(t)
	mine := map[uuid.UUID]bool{}
	for range 45 {
		mine[e.newUpload(t, u1).ID] = true
	}
	for range 3 {
		e.newUpload(t, u2)
	}

	var sizes []int
	seen := map[uuid.UUID]bool{}
	var prev *domain.CV
	tok := ""
	for {
		p, err := e.svc.ListMyCVs(asCandidate(u1), app.PageRequest{Size: 20, Token: tok})
		require.NoError(t, err)
		sizes = append(sizes, len(p.CVs))
		for _, cv := range p.CVs {
			assert.True(t, mine[cv.ID], "only own CVs")
			assert.False(t, seen[cv.ID], "no duplicates")
			seen[cv.ID] = true
			if prev != nil {
				assert.True(t, cv.CreatedAt.Before(prev.CreatedAt), "newest first")
			}
			prev = &cv
		}
		if p.NextToken == "" {
			break
		}
		tok = p.NextToken
	}
	assert.Equal(t, []int{20, 20, 5}, sizes)
	assert.Len(t, seen, 45)

	p, err := e.svc.ListMyCVs(asCandidate(u1), app.PageRequest{})
	require.NoError(t, err)
	assert.Len(t, p.CVs, 20, "default size 20")

	p, err = e.svc.ListMyCVs(asCandidate(u1), app.PageRequest{Size: 50})
	require.NoError(t, err)
	assert.Len(t, p.CVs, 45)
	assert.Empty(t, p.NextToken)

	for _, bad := range []app.PageRequest{{Size: 51}, {Size: -1}, {Token: "!!!"}} {
		_, err = e.svc.ListMyCVs(asCandidate(u1), bad)
		assert.ErrorIs(t, err, apperr.ErrValidation)
	}
}

func TestCursor_RoundTripAndRejects(t *testing.T) {
	c := app.Cursor{At: time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC), ID: u1}
	got, err := app.DecodeCursor(app.EncodeCursor(c))
	require.NoError(t, err)
	assert.True(t, c.At.Equal(got.At))
	assert.Equal(t, c.ID, got.ID)

	b64 := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	for _, s := range []string{
		"!!!",
		b64("not json"),
		b64(`{"p":"2026-11-11T09:00:00Z"}`),
		b64(`{"id":"` + u1.String() + `"}`),
	} {
		_, err := app.DecodeCursor(s)
		require.ErrorIs(t, err, apperr.ErrValidation, s)
		assert.EqualError(t, err, "invalid page token")
	}
}
