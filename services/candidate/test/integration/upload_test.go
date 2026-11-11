//go:build integration

package integration

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/event/eventschema"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/storage"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

const samplePDF = "%PDF-1.4\n1 0 obj << /Type /Catalog >> endobj\ntrailer << /Root 1 0 R >>\n%%EOF\n"

// put uploads body to a presigned URL and returns the HTTP status.
func put(t *testing.T, url, contentType string, body []byte) int {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", contentType)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	return resp.StatusCode
}

func outboxCount(t *testing.T) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox`).Scan(&n))
	return n
}

// outboxCountFor counts the outbox rows of one CV, so subtests sharing the
// table do not depend on each other's order.
func outboxCountFor(t *testing.T, cvID uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE aggregate_id = $1`, cvID.String()).Scan(&n))
	return n
}

// rootStat stats an object with the MinIO root user (which may list the
// bucket, so a missing object is NoSuchKey).
func rootStat(t *testing.T, key string) error {
	t.Helper()
	_, err := root.StatObject(context.Background(), bucket, key, miniogo.StatObjectOptions{})
	return err
}

func newUpload(t *testing.T) (context.Context, app.UploadTicket) {
	t.Helper()
	ctx := asCandidate(uuid.New())
	tk, err := svc.CreateUploadURL(ctx, true, "Nguyen Van A - CV.pdf")
	require.NoError(t, err)
	return ctx, tk
}

func TestUpload_PresignPutComplete(t *testing.T) {
	truncate(t)
	ctx, tk := newUpload(t)
	require.Equal(t, http.StatusOK, put(t, tk.URL, "application/pdf", []byte(samplePDF)))

	cv, err := svc.CompleteUpload(ctx, tk.CVID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusUploaded, cv.Status)
	require.NotNil(t, cv.SizeBytes)
	assert.EqualValues(t, len(samplePDF), *cv.SizeBytes)
	assert.NotEmpty(t, cv.AttemptID)

	var aggregate string
	var payload []byte
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT aggregate_id, payload FROM outbox WHERE event_type = 'cv.uploaded'`).Scan(&aggregate, &payload))
	assert.Equal(t, tk.CVID.String(), aggregate)
	assert.NoError(t, eventschema.Validate("cv.uploaded", payload))
	assert.Contains(t, string(payload), cv.AttemptID)
}

func TestUpload_PutWithOtherContentTypeRejectedBySignature(t *testing.T) {
	truncate(t)
	_, tk := newUpload(t)
	assert.Equal(t, http.StatusForbidden, put(t, tk.URL, "text/plain", []byte(samplePDF)))
}

func TestComplete_RejectsAndDeletesObject(t *testing.T) {
	truncate(t)
	check := func(t *testing.T, ctx context.Context, tk app.UploadTicket) {
		t.Helper()
		_, err := svc.CompleteUpload(ctx, tk.CVID)
		require.ErrorIs(t, err, apperr.ErrValidation)
		cv, err := svc.GetCV(ctx, tk.CVID)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusUploading, cv.Status)
		assert.Equal(t, "NoSuchKey", miniogo.ToErrorResponse(rootStat(t, cv.ObjectKey)).Code)
		assert.Equal(t, 0, outboxCountFor(t, tk.CVID), "no cv.uploaded for a rejected upload")
	}

	t.Run("html renamed to pdf", func(t *testing.T) {
		ctx, tk := newUpload(t)
		require.Equal(t, http.StatusOK, put(t, tk.URL, "application/pdf", []byte("<html><body>hi</body></html>")))
		check(t, ctx, tk)
	})
	t.Run("larger than 5 MiB", func(t *testing.T) {
		ctx, tk := newUpload(t)
		body := append([]byte("%PDF-1.4\n"), bytes.Repeat([]byte{'a'}, int(domain.MaxCVBytes)+1-9)...)
		require.Len(t, body, int(domain.MaxCVBytes)+1)
		require.Equal(t, http.StatusOK, put(t, tk.URL, "application/pdf", body))
		check(t, ctx, tk)
	})
	t.Run("not uploaded", func(t *testing.T) {
		ctx, tk := newUpload(t)
		check(t, ctx, tk)
	})
	t.Run("empty object", func(t *testing.T) {
		ctx, tk := newUpload(t)
		require.Equal(t, http.StatusOK, put(t, tk.URL, "application/pdf", nil))
		check(t, ctx, tk)
	})
	t.Run("can PUT again after a rejection", func(t *testing.T) {
		ctx, tk := newUpload(t)
		require.Equal(t, http.StatusOK, put(t, tk.URL, "application/pdf", []byte("not a pdf")))
		_, err := svc.CompleteUpload(ctx, tk.CVID)
		require.ErrorIs(t, err, apperr.ErrValidation)
		require.Equal(t, http.StatusOK, put(t, tk.URL, "application/pdf", []byte(samplePDF)))
		cv, err := svc.CompleteUpload(ctx, tk.CVID)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusUploaded, cv.Status)
	})
}

func TestComplete_ConcurrentEmitsOnce(t *testing.T) {
	truncate(t)
	ctx, tk := newUpload(t)
	require.Equal(t, http.StatusOK, put(t, tk.URL, "application/pdf", []byte(samplePDF)))

	var wg sync.WaitGroup
	errs := make([]error, 5)
	cvs := make([]domain.CV, 5)
	for i := range 5 {
		wg.Go(func() { cvs[i], errs[i] = svc.CompleteUpload(ctx, tk.CVID) })
	}
	wg.Wait()
	for i := range 5 {
		require.NoError(t, errs[i])
		assert.Equal(t, domain.StatusUploaded, cvs[i].Status)
		assert.Equal(t, cvs[0].AttemptID, cvs[i].AttemptID)
	}
	assert.Equal(t, 1, outboxCount(t))
	cv, err := svc.GetCV(ctx, tk.CVID)
	require.NoError(t, err)
	assert.Equal(t, int64(2), cv.Version)
}

// The candidate-svc user has no s3:ListBucket, yet MinIO still answers
// NoSuchKey for a missing object, which the adapter reports as
// ErrObjectNotFound (E-R9).
func TestStorage_LeastPrivilegeMissingObjectIsNotFound(t *testing.T) {
	ctx := context.Background()
	key := "cvs/" + uuid.NewString() + "/" + uuid.NewString() + ".pdf"
	assert.Equal(t, "NoSuchKey", miniogo.ToErrorResponse(rootStat(t, key)).Code)

	// The policy is in effect: candidate-svc may not list the bucket.
	raw, err := miniogo.New(root.EndpointURL().Host, &miniogo.Options{
		Creds: credentials.NewStaticV4(svcAccessKey, svcSecretKey, ""), Region: region,
	})
	require.NoError(t, err)
	for obj := range raw.ListObjects(ctx, bucket, miniogo.ListObjectsOptions{}) {
		assert.Equal(t, "AccessDenied", miniogo.ToErrorResponse(obj.Err).Code)
		break
	}

	_, err = store.Stat(ctx, key)
	assert.ErrorIs(t, err, app.ErrObjectNotFound)
	_, err = store.ReadHead(ctx, key, 5)
	assert.ErrorIs(t, err, app.ErrObjectNotFound)
	assert.NoError(t, store.Delete(ctx, key))
}

// An object outside candidate-svc's policy yields AccessDenied, a real
// permission error: it must not read as "not found" and must not carry the
// key (E-R9, E-R8).
func TestStorage_AccessDeniedIsNotNotFound(t *testing.T) {
	ctx := context.Background()
	const other = "outside-policy"
	exists, err := root.BucketExists(ctx, other)
	require.NoError(t, err)
	if !exists {
		require.NoError(t, root.MakeBucket(ctx, other, miniogo.MakeBucketOptions{Region: region}))
	}
	key := "cvs/" + uuid.NewString() + "/denied-marker.pdf"
	_, err = root.PutObject(ctx, other, key, strings.NewReader(samplePDF), int64(len(samplePDF)),
		miniogo.PutObjectOptions{ContentType: "application/pdf"})
	require.NoError(t, err)

	ep := root.EndpointURL().Host
	denied, err := storage.New(storage.Config{Endpoint: ep, PublicEndpoint: "http://" + ep,
		AccessKey: svcAccessKey, SecretKey: svcSecretKey, Bucket: other, Region: region})
	require.NoError(t, err)
	for name, err := range map[string]error{
		"stat":     func() error { _, err := denied.Stat(ctx, key); return err }(),
		"readhead": func() error { _, err := denied.ReadHead(ctx, key, 5); return err }(),
	} {
		require.Error(t, err, name)
		assert.NotErrorIs(t, err, app.ErrObjectNotFound, name)
		assert.Contains(t, err.Error(), "AccessDenied", name)
		assert.NotContains(t, err.Error(), "denied-marker", name)
		assert.NotContains(t, err.Error(), ep, name)
	}
}

func TestStorage_ReadHeadAndDelete(t *testing.T) {
	ctx := context.Background()
	key := "cvs/" + uuid.NewString() + "/" + uuid.NewString() + ".pdf"
	u, err := store.PresignPut(ctx, key, "application/pdf", time.Minute)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, put(t, u, "application/pdf", []byte("%PD")))

	info, err := store.Stat(ctx, key)
	require.NoError(t, err)
	assert.EqualValues(t, 3, info.Size)
	assert.Equal(t, "application/pdf", info.ContentType)
	head, err := store.ReadHead(ctx, key, 5)
	require.NoError(t, err)
	assert.Equal(t, "%PD", string(head))

	require.NoError(t, store.Delete(ctx, key))
	_, err = store.Stat(ctx, key)
	assert.ErrorIs(t, err, app.ErrObjectNotFound)
}

// Storage errors must not echo the object key or a request URL (E-R8).
func TestStorage_ErrorsCarryNoKey(t *testing.T) {
	ctx := context.Background()
	key := "cvs/" + uuid.NewString() + "/secret-key-marker.pdf"
	ep := root.EndpointURL().Host

	bad, err := storage.New(storage.Config{Endpoint: ep, PublicEndpoint: "http://" + ep,
		AccessKey: svcAccessKey, SecretKey: "wrong-secret", Bucket: bucket, Region: region})
	require.NoError(t, err)
	_, err = bad.Stat(ctx, key)
	require.Error(t, err)
	assert.NotErrorIs(t, err, app.ErrObjectNotFound)
	assert.NotContains(t, err.Error(), "secret-key-marker")
	assert.NotContains(t, err.Error(), ep)

	down, err := storage.New(storage.Config{Endpoint: "127.0.0.1:1", PublicEndpoint: "http://127.0.0.1:1",
		AccessKey: svcAccessKey, SecretKey: svcSecretKey, Bucket: bucket, Region: region})
	require.NoError(t, err)
	// No deadline: minio-go gives up after its own connect retries (a few
	// seconds) and returns a *url.Error that names the URL.
	_, err = down.Stat(ctx, key)
	require.Error(t, err)
	assert.Equal(t, "storage stat: request failed", err.Error())
}
