package app_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/event/eventschema"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

var validPDF = object{size: 1234, contentType: "application/pdf", head: "%PDF-1.7\n"}

// newUpload creates an UPLOADING CV owned by owner.
func (e *env) newUpload(t *testing.T, owner uuid.UUID) domain.CV {
	t.Helper()
	tk, err := e.svc.CreateUploadURL(asCandidate(owner), true, "cv.pdf")
	require.NoError(t, err)
	return e.cvs.m[tk.CVID]
}

// uploaded creates a CV, puts a valid PDF and completes it.
func (e *env) uploaded(t *testing.T, owner uuid.UUID) domain.CV {
	t.Helper()
	cv := e.newUpload(t, owner)
	e.storage.put(cv.ObjectKey, validPDF)
	out, err := e.svc.CompleteUpload(asCandidate(owner), cv.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusUploaded, out.Status)
	return out
}

func TestCreateUploadURL_RequiresConsent(t *testing.T) {
	e := newEnv(t)
	_, err := e.svc.CreateUploadURL(asCandidate(u1), false, "cv.pdf")
	require.ErrorIs(t, err, apperr.ErrValidation)
	assert.EqualError(t, err, "consent is required")
	assert.Empty(t, e.cvs.m)

	_, err = e.svc.CreateUploadURL(asCandidate(u1), true, "  ")
	assert.ErrorIs(t, err, apperr.ErrValidation)
	assert.Empty(t, e.cvs.m)
}

func TestCreateUploadURL_CreatesUploadingCV(t *testing.T) {
	e := newEnv(t)
	tk, err := e.svc.CreateUploadURL(asCandidate(u1), true, "My CV.pdf")
	require.NoError(t, err)

	cv, ok := e.cvs.m[tk.CVID]
	require.True(t, ok)
	assert.Equal(t, domain.StatusUploading, cv.Status)
	assert.Equal(t, u1, cv.CandidateID)
	assert.Equal(t, "cvs/"+u1.String()+"/"+cv.ID.String()+".pdf", cv.ObjectKey)
	assert.Equal(t, "My CV.pdf", cv.FileName)
	assert.Equal(t, cv.CreatedAt, cv.ConsentAt)
	assert.Equal(t, "https://minio.test/"+cv.ObjectKey+"?sig=x", tk.URL)
	assert.Equal(t, cv.CreatedAt.Add(10*time.Minute), tk.ExpiresAt)
	assert.Equal(t, "application/pdf", e.storage.presignContentType)
	assert.Equal(t, 10*time.Minute, e.storage.presignTTL)
	assert.Zero(t, e.ob.count())
}

func TestComplete_HappyPathEmitsEvent(t *testing.T) {
	e := newEnv(t)
	cv := e.newUpload(t, u1)
	e.storage.put(cv.ObjectKey, validPDF)

	out, err := e.svc.CompleteUpload(asCandidate(u1), cv.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusUploaded, out.Status)
	require.NotNil(t, out.SizeBytes)
	assert.EqualValues(t, 1234, *out.SizeBytes)
	assert.Equal(t, "01HATTEMPT0000000000000001", out.AttemptID)
	assert.EqualValues(t, 2, out.Version)
	assert.Equal(t, out, e.cvs.m[cv.ID])

	require.Equal(t, 1, e.ob.count())
	ev := e.ob.last()
	assert.Equal(t, "cv.uploaded", ev.EventType)
	assert.Equal(t, "candidate-service", ev.Source)
	assert.Equal(t, 1, ev.EventVersion)
	assert.Equal(t, cv.ID.String(), e.ob.lastKey())
	raw, err := json.Marshal(ev)
	require.NoError(t, err)
	assert.NoError(t, eventschema.Validate("cv.uploaded", raw))
	assert.JSONEq(t, `{"cv_id":"`+cv.ID.String()+`","candidate_id":"`+u1.String()+`",
		"object_key":"`+cv.ObjectKey+`","attempt_id":"01HATTEMPT0000000000000001"}`, string(ev.Payload))
	assert.Zero(t, e.storage.deletes)
}

func TestComplete_Rejections(t *testing.T) {
	cases := []struct {
		name    string
		obj     *object
		msg     string
		deletes int
	}{
		{"missing object", nil, "uploaded file not found", 0},
		{"too large", &object{size: domain.MaxCVBytes + 1, contentType: "application/pdf", head: "%PDF-"}, "file exceeds 5 MiB", 1},
		{"wrong content type", &object{size: 100, contentType: "text/html", head: "%PDF-"}, "file must be application/pdf", 1},
		{"not a pdf", &object{size: 100, contentType: "application/pdf", head: "<html><body>"}, "file is not a PDF", 1},
		{"short file", &object{size: 3, contentType: "application/pdf", head: "%PD"}, "file is not a PDF", 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			cv := e.newUpload(t, u1)
			if c.obj != nil {
				e.storage.put(cv.ObjectKey, *c.obj)
			}
			_, err := e.svc.CompleteUpload(asCandidate(u1), cv.ID)
			require.ErrorIs(t, err, apperr.ErrValidation)
			assert.EqualError(t, err, c.msg)
			assert.Equal(t, c.deletes, e.storage.deletes)
			if c.deletes > 0 {
				assert.NotContains(t, e.storage.objs, cv.ObjectKey)
			}
			got := e.cvs.m[cv.ID]
			assert.Equal(t, domain.StatusUploading, got.Status)
			assert.EqualValues(t, 1, got.Version)
			assert.Zero(t, e.ob.count())
		})
	}

	t.Run("exactly max size ok", func(t *testing.T) {
		e := newEnv(t)
		cv := e.newUpload(t, u1)
		e.storage.put(cv.ObjectKey, object{size: domain.MaxCVBytes, contentType: "application/pdf", head: "%PDF-"})
		out, err := e.svc.CompleteUpload(asCandidate(u1), cv.ID)
		require.NoError(t, err)
		assert.EqualValues(t, domain.MaxCVBytes, *out.SizeBytes)
		assert.Zero(t, e.storage.deletes)
		assert.Equal(t, 1, e.ob.count())
	})
}

func TestComplete_IdempotentWhenNotUploading(t *testing.T) {
	e := newEnv(t)
	first := e.uploaded(t, u1)
	stats := e.storage.stats

	second, err := e.svc.CompleteUpload(asCandidate(u1), first.ID)
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.EqualValues(t, 2, second.Version)
	assert.Equal(t, 1, e.ob.count())
	assert.Equal(t, stats, e.storage.stats, "Stat must not be called again")
}

func TestComplete_NonOwnerNotFound(t *testing.T) {
	e := newEnv(t)
	cv := e.newUpload(t, u1)
	e.storage.put(cv.ObjectKey, validPDF)

	_, err := e.svc.CompleteUpload(asCandidate(u2), cv.ID)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	_, err = e.svc.CompleteUpload(asCandidate(u1), uuid.New())
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	assert.Equal(t, domain.StatusUploading, e.cvs.m[cv.ID].Status)
	assert.Zero(t, e.storage.stats)
	assert.Zero(t, e.ob.count())
}

func TestCommand_RollsBackOnOutboxError(t *testing.T) {
	e := newEnv(t)
	cv := e.newUpload(t, u1)
	e.storage.put(cv.ObjectKey, validPDF)
	e.ob.err = errBoom

	_, err := e.svc.CompleteUpload(asCandidate(u1), cv.ID)
	assert.ErrorIs(t, err, errBoom)
	got := e.cvs.m[cv.ID]
	assert.Equal(t, domain.StatusUploading, got.Status)
	assert.EqualValues(t, 1, got.Version)
	assert.Zero(t, e.ob.count())
}
