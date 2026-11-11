package storage_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/storage"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
)

func TestPresignPut_NoNetworkAndSignsContentType(t *testing.T) {
	m, err := storage.New(storage.Config{
		Endpoint: "127.0.0.1:1", PublicEndpoint: "http://127.0.0.1:1",
		AccessKey: "k", SecretKey: "secret123", Bucket: "cvs", Region: "us-east-1",
	})
	require.NoError(t, err)
	u, err := m.PresignPut(context.Background(), "cvs/a/b.pdf", "application/pdf", 10*time.Minute)
	require.NoError(t, err) // no server listens on 127.0.0.1:1
	pu, err := url.Parse(u)
	require.NoError(t, err)
	assert.Equal(t, "http", pu.Scheme)
	assert.Equal(t, "127.0.0.1:1", pu.Host)
	assert.Equal(t, "/cvs/cvs/a/b.pdf", pu.Path)
	assert.Contains(t, pu.Query().Get("X-Amz-SignedHeaders"), "content-type")
	assert.Equal(t, "600", pu.Query().Get("X-Amz-Expires"))
}

func TestPresignPut_UsesPublicEndpoint(t *testing.T) {
	m, err := storage.New(storage.Config{
		Endpoint: "minio:9000", PublicEndpoint: "https://files.example.test",
		AccessKey: "k", SecretKey: "secret123", Bucket: "cvs", Region: "us-east-1",
	})
	require.NoError(t, err)
	u, err := m.PresignPut(context.Background(), "cvs/a/b.pdf", "application/pdf", time.Minute)
	require.NoError(t, err)
	pu, err := url.Parse(u)
	require.NoError(t, err)
	assert.Equal(t, "https", pu.Scheme)
	assert.Equal(t, "files.example.test", pu.Host)
}

func TestNew_RejectsBadConfig(t *testing.T) {
	base := storage.Config{
		Endpoint: "minio:9000", PublicEndpoint: "http://127.0.0.1:9000",
		AccessKey: "k", SecretKey: "secret123", Bucket: "cvs", Region: "us-east-1",
	}
	for name, mut := range map[string]func(*storage.Config){
		"no bucket":         func(c *storage.Config) { c.Bucket = "" },
		"no region":         func(c *storage.Config) { c.Region = "" },
		"public no scheme":  func(c *storage.Config) { c.PublicEndpoint = "127.0.0.1:9000" },
		"public bad scheme": func(c *storage.Config) { c.PublicEndpoint = "ftp://127.0.0.1:9000" },
		"public with path":  func(c *storage.Config) { c.PublicEndpoint = "http://127.0.0.1:9000/x" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base
			mut(&cfg)
			_, err := storage.New(cfg)
			require.Error(t, err)
		})
	}
}

// fakeS3 answers every request with status and, except for HEAD, an S3 XML
// error carrying code and markedKey; it records the last request.
type fakeS3 struct {
	status int
	code   string
	last   *http.Request
	srv    *httptest.Server
}

func newFakeS3(t *testing.T, status int, code string) (*fakeS3, *storage.MinIO) {
	t.Helper()
	f := &fakeS3{status: status, code: code}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.last = r
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(f.status)
		if r.Method != http.MethodHead && f.code != "" {
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>`+f.code+
				`</Code><Message>m</Message><Key>`+markedKey+`</Key></Error>`)
		}
	}))
	t.Cleanup(f.srv.Close)
	m, err := storage.New(storage.Config{Endpoint: f.host(), PublicEndpoint: f.srv.URL,
		AccessKey: "k", SecretKey: "secret123", Bucket: "cvs", Region: "us-east-1"})
	require.NoError(t, err)
	return f, m
}

func (f *fakeS3) host() string { return strings.TrimPrefix(f.srv.URL, "http://") }

const markedKey = "cvs/candidate-marker/cv-marker.pdf"

// assertNoLeak checks err carries neither the object key nor the endpoint (E-R8).
func assertNoLeak(t *testing.T, err error, f *fakeS3) {
	t.Helper()
	assert.NotContains(t, err.Error(), "marker")
	assert.NotContains(t, err.Error(), f.host())
}

// Only NoSuchKey means "not found"; AccessDenied (HEAD 403 carries no body)
// is a permission error and must surface as an internal error (E-R9).
func TestStat_MissingObjectIsNotFound(t *testing.T) {
	for name, tc := range map[string]struct {
		status   int
		notFound bool
	}{
		"access denied": {http.StatusForbidden, false},
		"no such key":   {http.StatusNotFound, true},
	} {
		t.Run(name, func(t *testing.T) {
			f, m := newFakeS3(t, tc.status, "")
			_, err := m.Stat(context.Background(), markedKey)
			require.Error(t, err)
			if tc.notFound {
				assert.ErrorIs(t, err, app.ErrObjectNotFound)
			} else {
				assert.NotErrorIs(t, err, app.ErrObjectNotFound)
			}
			assert.Equal(t, http.MethodHead, f.last.Method)
			assertNoLeak(t, err, f)
		})
	}
}

func TestReadHead_MissingObjectIsNotFound(t *testing.T) {
	for code, tc := range map[string]struct {
		status   int
		notFound bool
	}{
		"AccessDenied": {http.StatusForbidden, false},
		"NoSuchKey":    {http.StatusNotFound, true},
	} {
		t.Run(code, func(t *testing.T) {
			f, m := newFakeS3(t, tc.status, code)
			_, err := m.ReadHead(context.Background(), markedKey, 5)
			require.Error(t, err)
			if tc.notFound {
				assert.ErrorIs(t, err, app.ErrObjectNotFound)
			} else {
				assert.NotErrorIs(t, err, app.ErrObjectNotFound)
			}
			assert.Equal(t, "bytes=0-4", f.last.Header.Get("Range"))
			assertNoLeak(t, err, f)
		})
	}
}

func TestReadHead_EmptyObjectYieldsNoBytes(t *testing.T) {
	_, m := newFakeS3(t, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
	b, err := m.ReadHead(context.Background(), markedKey, 5)
	require.NoError(t, err)
	assert.Empty(t, b)
}

func TestErrors_OtherFailuresCarryNoKeyOrURL(t *testing.T) {
	f, m := newFakeS3(t, http.StatusBadRequest, "SignatureDoesNotMatch")
	ctx := context.Background()
	_, err := m.ReadHead(ctx, markedKey, 5)
	require.Error(t, err)
	assert.NotErrorIs(t, err, app.ErrObjectNotFound)
	assert.Contains(t, err.Error(), "SignatureDoesNotMatch")
	assertNoLeak(t, err, f)

	f.status, f.code = http.StatusForbidden, "AccessDenied"
	err = m.Delete(ctx, markedKey)
	require.Error(t, err)
	assert.NotErrorIs(t, err, app.ErrObjectNotFound, "delete never reports not-found")
	assertNoLeak(t, err, f)

	f.srv.Close() // connection refused: minio-go returns a *url.Error carrying the URL
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_, err = m.Stat(ctx, markedKey)
	require.Error(t, err)
	assertNoLeak(t, err, f)
}
