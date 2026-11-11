// Package storage implements app.ObjectStorage on MinIO (S3 API).
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
)

// Config configures MinIO. Endpoint is the in-network host:port used for
// server-side calls; PublicEndpoint is the URL (scheme://host[:port]) clients
// reach, used only to sign presigned URLs. Region must be set so presigning
// never looks the bucket location up over the network.
type Config struct {
	Endpoint             string
	PublicEndpoint       string
	AccessKey, SecretKey string
	Bucket, Region       string
	UseSSL               bool
}

// MinIO implements app.ObjectStorage. Returned errors never contain the
// object key or a request URL (E-R8).
type MinIO struct {
	internal *minio.Client
	public   *minio.Client
	bucket   string
}

// New builds the internal and public clients; it does not touch the network.
func New(cfg Config) (*MinIO, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("storage: empty bucket")
	}
	if cfg.Region == "" {
		return nil, errors.New("storage: empty region")
	}
	pub, err := url.Parse(cfg.PublicEndpoint)
	if err != nil || (pub.Scheme != "http" && pub.Scheme != "https") || pub.Host == "" ||
		(pub.Path != "" && pub.Path != "/") || pub.RawQuery != "" {
		return nil, errors.New("storage: public endpoint must be http(s)://host[:port]")
	}
	creds := credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, "")
	internal, err := minio.New(cfg.Endpoint, &minio.Options{Creds: creds, Secure: cfg.UseSSL, Region: cfg.Region})
	if err != nil {
		return nil, fmt.Errorf("storage: internal client: %w", err)
	}
	public, err := minio.New(pub.Host, &minio.Options{Creds: creds, Secure: pub.Scheme == "https", Region: cfg.Region})
	if err != nil {
		return nil, fmt.Errorf("storage: public client: %w", err)
	}
	return &MinIO{internal: internal, public: public, bucket: cfg.Bucket}, nil
}

// PresignPut returns a PUT URL on the public endpoint valid for ttl; the
// Content-Type header is part of the signature, so an upload with any other
// type is rejected by MinIO.
func (m *MinIO) PresignPut(ctx context.Context, key, contentType string, ttl time.Duration) (string, error) {
	u, err := m.public.PresignHeader(ctx, http.MethodPut, m.bucket, key, ttl, nil,
		http.Header{"Content-Type": {contentType}})
	if err != nil {
		return "", fmt.Errorf("storage presign: %s", classify(err))
	}
	return u.String(), nil
}

// Stat returns the object's size and content type, or app.ErrObjectNotFound.
func (m *MinIO) Stat(ctx context.Context, key string) (app.ObjectInfo, error) {
	info, err := m.internal.StatObject(ctx, m.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return app.ObjectInfo{}, wrap("stat", err)
	}
	return app.ObjectInfo{Size: info.Size, ContentType: info.ContentType}, nil
}

// ReadHead returns at most the first n bytes of the object; an empty object
// yields no bytes.
func (m *MinIO) ReadHead(ctx context.Context, key string, n int64) ([]byte, error) {
	if n <= 0 {
		return nil, nil
	}
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(0, n-1); err != nil {
		return nil, fmt.Errorf("storage read: invalid range")
	}
	obj, err := m.internal.GetObject(ctx, m.bucket, key, opts)
	if err != nil {
		return nil, wrap("read", err)
	}
	defer func() { _ = obj.Close() }()
	b, err := io.ReadAll(io.LimitReader(obj, n))
	if err != nil {
		if minio.ToErrorResponse(err).Code == "InvalidRange" { // empty object
			return nil, nil
		}
		return nil, wrap("read", err)
	}
	return b, nil
}

// Delete removes the object; deleting a missing object succeeds.
func (m *MinIO) Delete(ctx context.Context, key string) error {
	if err := m.internal.RemoveObject(ctx, m.bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("storage delete: %s", classify(err))
	}
	return nil
}

// wrap maps a missing object (NoSuchKey) on stat/read to
// app.ErrObjectNotFound. MinIO answers NoSuchKey even to a user without
// s3:ListBucket, so AccessDenied is a real permission problem and surfaces as
// an internal, key-free error (E-R9).
func wrap(op string, err error) error {
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return fmt.Errorf("storage %s: %w", op, app.ErrObjectNotFound)
	}
	return fmt.Errorf("storage %s: %s", op, classify(err))
}

// classify describes err without the request URL or object key that
// minio-go errors (and *url.Error) carry.
func classify(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "context canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline exceeded"
	}
	var resp minio.ErrorResponse
	if errors.As(err, &resp) && (resp.Code != "" || resp.StatusCode != 0) {
		return "s3 error " + resp.Code + " (status " + strconv.Itoa(resp.StatusCode) + ")"
	}
	return "request failed"
}
