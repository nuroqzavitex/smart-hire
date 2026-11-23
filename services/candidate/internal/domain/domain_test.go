package domain_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

var (
	t0 = time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC)
	t1 = t0.Add(time.Minute)
)

func ptr[T any](v T) *T { return &v }

var sampleParsed = json.RawMessage(`{"skills":["Go"]}`)

func newCV(t *testing.T) domain.CV {
	t.Helper()
	cv, err := domain.NewCV(uuid.New(), uuid.New(), "cv.pdf", t0)
	require.NoError(t, err)
	return cv
}

// cvIn builds a CV in the given status through the public transitions.
func cvIn(t *testing.T, st domain.Status) domain.CV {
	t.Helper()
	cv := newCV(t)
	if st == domain.StatusUploading {
		return cv
	}
	require.NoError(t, cv.Complete(1024, "A", t0))
	switch st {
	case domain.StatusUploaded:
	case domain.StatusParsed:
		require.NoError(t, cv.ApplyParsed("A", sampleParsed, t0))
	case domain.StatusFailed:
		require.NoError(t, cv.ApplyFailed("A", "no_text", t0))
	default:
		t.Fatalf("unknown status %q", st)
	}
	require.Equal(t, st, cv.Status)
	return cv
}

type action struct {
	name string
	do   func(c *domain.CV) error
}

var (
	actComplete    = action{"Complete", func(c *domain.CV) error { return c.Complete(2048, "N", t1) }}
	actApplyParsed = action{"ApplyParsed", func(c *domain.CV) error { return c.ApplyParsed("A", sampleParsed, t1) }}
	actApplyFailed = action{"ApplyFailed", func(c *domain.CV) error { return c.ApplyFailed("A", "no_text", t1) }}
	actUpdate      = action{"UpdateParsed", func(c *domain.CV) error { return c.UpdateParsed(json.RawMessage(`{}`), t1) }}
	actReparse     = action{"Reparse", func(c *domain.CV) error { return c.Reparse("R", t1) }}
)

func TestTransitions(t *testing.T) {
	tests := []struct {
		from    domain.Status
		act     action
		wantErr error // nil = ok
		wantTo  domain.Status
	}{
		{domain.StatusUploading, actComplete, nil, domain.StatusUploaded},
		{domain.StatusUploading, actApplyParsed, domain.ErrStaleResult, ""},
		{domain.StatusUploading, actApplyFailed, domain.ErrStaleResult, ""},
		{domain.StatusUploading, actUpdate, domain.ErrInvalidTransition, ""},
		{domain.StatusUploading, actReparse, domain.ErrInvalidTransition, ""},

		{domain.StatusUploaded, actComplete, domain.ErrInvalidTransition, ""},
		{domain.StatusUploaded, actApplyParsed, nil, domain.StatusParsed},
		{domain.StatusUploaded, actApplyFailed, nil, domain.StatusFailed},
		{domain.StatusUploaded, actUpdate, domain.ErrInvalidTransition, ""},
		{domain.StatusUploaded, actReparse, domain.ErrInvalidTransition, ""},

		{domain.StatusParsed, actComplete, domain.ErrInvalidTransition, ""},
		{domain.StatusParsed, actApplyParsed, domain.ErrStaleResult, ""},
		{domain.StatusParsed, actApplyFailed, domain.ErrStaleResult, ""},
		{domain.StatusParsed, actUpdate, nil, domain.StatusParsed},
		{domain.StatusParsed, actReparse, nil, domain.StatusUploaded},

		{domain.StatusFailed, actComplete, domain.ErrInvalidTransition, ""},
		{domain.StatusFailed, actApplyParsed, domain.ErrStaleResult, ""},
		{domain.StatusFailed, actApplyFailed, domain.ErrStaleResult, ""},
		{domain.StatusFailed, actUpdate, domain.ErrInvalidTransition, ""},
		{domain.StatusFailed, actReparse, nil, domain.StatusUploaded},
	}
	for _, tc := range tests {
		t.Run(string(tc.from)+"/"+tc.act.name, func(t *testing.T) {
			cv := cvIn(t, tc.from)
			before := cv
			err := tc.act.do(&cv)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				assert.Equal(t, before, cv, "failed transition must not mutate the CV")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantTo, cv.Status)
			assert.Equal(t, before.Version+1, cv.Version)
			assert.Equal(t, t1, cv.UpdatedAt)
		})
	}
}

func TestInvalidTransitionIsValidation(t *testing.T) {
	cv := cvIn(t, domain.StatusUploading)
	err := cv.Reparse("R", t1)
	assert.ErrorIs(t, err, apperr.ErrValidation)
}

func TestComplete_SetsSizeAndAttempt(t *testing.T) {
	cv := newCV(t)
	require.NoError(t, cv.Complete(4096, "01ATTEMPT", t1))
	assert.Equal(t, domain.StatusUploaded, cv.Status)
	assert.Equal(t, "01ATTEMPT", cv.AttemptID)
	require.NotNil(t, cv.SizeBytes)
	assert.Equal(t, int64(4096), *cv.SizeBytes)
	assert.Equal(t, int64(2), cv.Version)
}

func TestApplyParsed_SetsResult(t *testing.T) {
	cv := cvIn(t, domain.StatusUploaded)
	require.NoError(t, cv.ApplyParsed("A", sampleParsed, t1))
	assert.JSONEq(t, string(sampleParsed), string(cv.ParsedData))
	assert.Nil(t, cv.ParseError)
	require.NotNil(t, cv.ParsedAt)
	assert.Equal(t, t1, *cv.ParsedAt)
}

func TestApplyFailed_SetsReason(t *testing.T) {
	cv := cvIn(t, domain.StatusUploaded)
	require.NoError(t, cv.ApplyFailed("A", "not_pdf", t1))
	assert.Equal(t, domain.StatusFailed, cv.Status)
	require.NotNil(t, cv.ParseError)
	assert.Equal(t, "not_pdf", *cv.ParseError)
	require.NotNil(t, cv.ParsedAt)
	assert.Equal(t, t1, *cv.ParsedAt)
}

func TestUpdateParsed_ReplacesData(t *testing.T) {
	cv := cvIn(t, domain.StatusParsed)
	newData := json.RawMessage(`{"skills":["Rust"]}`)
	require.NoError(t, cv.UpdateParsed(newData, t1))
	assert.JSONEq(t, string(newData), string(cv.ParsedData))
	assert.Equal(t, domain.StatusParsed, cv.Status)
}

func TestApplyParsed_WrongAttemptIsStale(t *testing.T) {
	for _, act := range []func(c *domain.CV) error{
		func(c *domain.CV) error { return c.ApplyParsed("B", sampleParsed, t1) },
		func(c *domain.CV) error { return c.ApplyFailed("B", "no_text", t1) },
	} {
		cv := cvIn(t, domain.StatusUploaded) // attempt "A"
		before := cv
		require.ErrorIs(t, act(&cv), domain.ErrStaleResult)
		assert.Equal(t, before, cv)
	}
}

func TestReparse_ClearsResultAndSetsAttempt(t *testing.T) {
	for _, from := range []domain.Status{domain.StatusParsed, domain.StatusFailed} {
		cv := cvIn(t, from)
		require.NoError(t, cv.Reparse("NEW", t1))
		assert.Equal(t, domain.StatusUploaded, cv.Status)
		assert.Equal(t, "NEW", cv.AttemptID)
		assert.Nil(t, cv.ParsedData)
		assert.Nil(t, cv.ParseError)
		assert.Nil(t, cv.ParsedAt)

		// A result for the old attempt arriving late is stale.
		before := cv
		require.ErrorIs(t, cv.ApplyParsed("A", sampleParsed, t1), domain.ErrStaleResult)
		assert.Equal(t, before, cv)
		require.NoError(t, cv.ApplyParsed("NEW", sampleParsed, t1))
	}
}

func TestReparse_StaleUploaded(t *testing.T) {
	// cvIn completes at t0, so UpdatedAt = t0 for the current attempt "A".
	t.Run("before threshold is rejected", func(t *testing.T) {
		cv := cvIn(t, domain.StatusUploaded)
		before := cv
		err := cv.Reparse("R", t0.Add(domain.StaleUploadAfter-time.Second)) // 14:59
		require.ErrorIs(t, err, domain.ErrInvalidTransition)
		assert.ErrorIs(t, err, apperr.ErrValidation)
		assert.Equal(t, before, cv)
	})
	for _, age := range []time.Duration{domain.StaleUploadAfter, domain.StaleUploadAfter + time.Second, time.Hour} {
		t.Run("after "+age.String()+" is allowed", func(t *testing.T) {
			cv := cvIn(t, domain.StatusUploaded)
			now := t0.Add(age)
			require.NoError(t, cv.Reparse("R", now))
			assert.Equal(t, domain.StatusUploaded, cv.Status)
			assert.Equal(t, "R", cv.AttemptID)
			assert.Equal(t, now, cv.UpdatedAt)
			assert.Equal(t, int64(3), cv.Version)
			// The late result of the dead-lettered attempt is stale.
			require.ErrorIs(t, cv.ApplyParsed("A", sampleParsed, now), domain.ErrStaleResult)
			require.NoError(t, cv.ApplyParsed("R", sampleParsed, now))
		})
	}
	t.Run("threshold restarts with the new attempt", func(t *testing.T) {
		cv := cvIn(t, domain.StatusUploaded)
		now := t0.Add(domain.StaleUploadAfter)
		require.NoError(t, cv.Reparse("R", now))
		require.ErrorIs(t, cv.Reparse("S", now.Add(domain.StaleUploadAfter-time.Second)), domain.ErrInvalidTransition)
	})
	t.Run("uploading is never reparsable", func(t *testing.T) {
		cv := cvIn(t, domain.StatusUploading)
		require.ErrorIs(t, cv.Reparse("R", t0.Add(time.Hour)), domain.ErrInvalidTransition)
	})
	assert.Equal(t, 15*time.Minute, domain.StaleUploadAfter)
}

func TestNewCV_FileName(t *testing.T) {
	id, cand := uuid.New(), uuid.New()
	for _, name := range []string{"", "   ", strings.Repeat("a", 256)} {
		_, err := domain.NewCV(id, cand, name, t0)
		assert.ErrorIs(t, err, apperr.ErrValidation, "name len %d", len(name))
	}
	cv, err := domain.NewCV(id, cand, strings.Repeat("á", 255), t0)
	require.NoError(t, err)
	assert.Equal(t, strings.Repeat("á", 255), cv.FileName)

	cv, err = domain.NewCV(id, cand, "  my cv.pdf ", t0)
	require.NoError(t, err)
	assert.Equal(t, "my cv.pdf", cv.FileName)
	assert.Equal(t, id, cv.ID)
	assert.Equal(t, cand, cv.CandidateID)
	assert.Equal(t, domain.StatusUploading, cv.Status)
	assert.Equal(t, int64(1), cv.Version)
	assert.Equal(t, t0, cv.ConsentAt)
	assert.Equal(t, t0, cv.CreatedAt)
	assert.Equal(t, t0, cv.UpdatedAt)
	assert.Empty(t, cv.AttemptID)
	assert.Nil(t, cv.SizeBytes)
	assert.Equal(t, "cvs/"+cand.String()+"/"+id.String()+".pdf", cv.ObjectKey)
	assert.Equal(t, cv.ObjectKey, domain.ObjectKey(cand, id))
}

func TestNewProfile_Validation(t *testing.T) {
	uid := uuid.New()
	bad := []domain.ProfileInput{
		{FullName: "  "},
		{FullName: strings.Repeat("á", 201)},
		{FullName: "An", Phone: ptr("abc")},
		{FullName: "An", Phone: ptr("12345")},                 // too short
		{FullName: "An", Phone: ptr(strings.Repeat("1", 31))}, // too long
		{FullName: "An", Location: ptr(strings.Repeat("á", 201))},
	}
	for i, in := range bad {
		_, err := domain.NewProfile(uid, in, t0)
		assert.ErrorIs(t, err, apperr.ErrValidation, "case %d", i)
	}

	c, err := domain.NewProfile(uid, domain.ProfileInput{
		FullName: "  Nguyễn Văn An ",
		Phone:    ptr(" +84 (912) 345-678 "),
		Location: ptr(" Hà Nội "),
	}, t0)
	require.NoError(t, err)
	assert.Equal(t, uid, c.UserID)
	assert.Equal(t, "Nguyễn Văn An", c.FullName)
	require.NotNil(t, c.Phone)
	assert.Equal(t, "+84 (912) 345-678", *c.Phone)
	require.NotNil(t, c.Location)
	assert.Equal(t, "Hà Nội", *c.Location)
	assert.Equal(t, t0, c.CreatedAt)
	assert.Equal(t, t0, c.UpdatedAt)

	c, err = domain.NewProfile(uid, domain.ProfileInput{
		FullName: strings.Repeat("á", 200), Phone: ptr("  "), Location: ptr(""),
	}, t0)
	require.NoError(t, err)
	assert.Nil(t, c.Phone)
	assert.Nil(t, c.Location)
}

func TestParsedCV_MarshalCanonicalEmptySlices(t *testing.T) {
	raw, err := domain.ParsedCV{Experiences: []domain.Experience{{Company: "A", Title: "B"}}}.MarshalCanonical()
	require.NoError(t, err)
	assert.JSONEq(t, `{"full_name":null,"email":null,"skills":[],"years_of_experience":null,"current_title":null,"education":[],
	  "experiences":[{"company":"A","title":"B","start":null,"end":null,"highlights":[]}],"languages":[],"location":null}`, string(raw))
}

func TestParsedCV_MarshalCanonicalFull(t *testing.T) {
	p := domain.ParsedCV{
		FullName:          ptr("An"),
		Skills:            []string{"Go"},
		YearsOfExperience: ptr(3.5),
		Education:         []domain.Education{{School: "HUST", Degree: ptr("BSc"), EndYear: ptr(2020)}},
	}
	raw, err := p.MarshalCanonical()
	require.NoError(t, err)
	assert.JSONEq(t, `{"full_name":"An","email":null,"skills":["Go"],"years_of_experience":3.5,"current_title":null,
	  "education":[{"school":"HUST","degree":"BSc","major":null,"end_year":2020}],"experiences":[],"languages":[],"location":null}`, string(raw))
}

func TestParsedCV_MarshalCanonicalDoesNotMutate(t *testing.T) {
	p := domain.ParsedCV{Experiences: []domain.Experience{{Company: "A", Title: "B"}}}
	_, err := p.MarshalCanonical()
	require.NoError(t, err)
	assert.Nil(t, p.Experiences[0].Highlights)
}

func TestOwnedBy(t *testing.T) {
	cv := newCV(t)
	assert.True(t, cv.OwnedBy(cv.CandidateID.String()))
	assert.False(t, cv.OwnedBy(uuid.NewString()))
	assert.False(t, cv.OwnedBy(""))
}

func TestCV_TombstoneDropsPIIAndBlocksLateResults(t *testing.T) {
	now := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	cv, err := domain.NewCV(uuid.New(), uuid.New(), "my-cv.pdf", now)
	require.NoError(t, err)
	require.NoError(t, cv.Complete(10, "a1", now))
	require.NoError(t, cv.ApplyParsed("a1", []byte(`{"skills":["go"]}`), now))
	v := cv.Version

	later := now.Add(time.Hour)
	assert.True(t, cv.Tombstone(later))
	assert.Equal(t, domain.StatusDeleted, cv.Status)
	assert.Empty(t, cv.FileName)
	assert.Empty(t, cv.AttemptID)
	assert.Nil(t, cv.ParsedData)
	assert.Nil(t, cv.ParseError)
	assert.Nil(t, cv.ParsedAt)
	assert.Equal(t, v+1, cv.Version)
	assert.Equal(t, later, cv.UpdatedAt)

	assert.False(t, cv.Tombstone(later), "already a tombstone")
	assert.Equal(t, v+1, cv.Version)
	assert.ErrorIs(t, cv.ApplyParsed("a1", []byte(`{}`), later), domain.ErrStaleResult)
	assert.ErrorIs(t, cv.ApplyFailed("", "x", later), domain.ErrStaleResult)
	assert.ErrorIs(t, cv.Reparse("a2", later.Add(time.Hour)), domain.ErrInvalidTransition)
	assert.ErrorIs(t, cv.UpdateParsed([]byte(`{}`), later), domain.ErrInvalidTransition)
}
