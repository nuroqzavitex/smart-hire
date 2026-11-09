package grpcx_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	authv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/auth/v1"
	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/authz/authztest"
	"github.com/nuroqzavitex/smarthire/pkg/grpcx"
	"github.com/nuroqzavitex/smarthire/pkg/logger"
)

const registerMethod = "/smarthire.auth.v1.AuthService/Register"

type fakeAuth struct {
	authv1.UnimplementedAuthServiceServer
	register func(ctx context.Context, req *authv1.RegisterRequest) (*authv1.RegisterResponse, error)
	gotReqID string
}

func (f *fakeAuth) Register(ctx context.Context, req *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
	f.gotReqID = logger.RequestIDFrom(ctx)
	if f.register != nil {
		return f.register(ctx, req)
	}
	return &authv1.RegisterResponse{UserId: "new"}, nil
}

func (f *fakeAuth) GetMe(ctx context.Context, _ *authv1.GetMeRequest) (*authv1.User, error) {
	c, ok := authz.ClaimsFrom(ctx)
	if !ok {
		return nil, apperr.New(apperr.ErrUnauthenticated, "no claims")
	}
	return &authv1.User{Id: c.Subject}, nil
}

type testEnv struct {
	client authv1.AuthServiceClient
	conn   *grpc.ClientConn
	issuer *authztest.KeyIssuer
	fake   *fakeAuth
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func bufDial(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	conn, err := grpcx.Dial("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func newEnv(t *testing.T) *testEnv {
	t.Helper()
	return newEnvWithLog(t, discard{})
}

func newEnvWithLog(t *testing.T, w io.Writer) *testEnv {
	t.Helper()
	is := authztest.NewIssuer(t)
	v, err := authz.NewVerifier(context.Background(), is.JWKSURL(), authztest.Issuer, authztest.Audience)
	require.NoError(t, err)
	t.Cleanup(v.Close)

	srv, _ := grpcx.NewServer(grpcx.ServerConfig{
		Logger:        logger.New("test", w, 0),
		Verifier:      v,
		PublicMethods: []string{registerMethod},
	})
	fake := &fakeAuth{}
	authv1.RegisterAuthServiceServer(srv, fake)

	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn := bufDial(t, lis)
	return &testEnv{client: authv1.NewAuthServiceClient(conn), conn: conn, issuer: is, fake: fake}
}

func withBearer(tok string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+tok)
}

func validRegister() *authv1.RegisterRequest {
	return &authv1.RegisterRequest{Email: "a@b.co", Password: "longenough1", Role: authv1.Role_ROLE_CANDIDATE}
}

func TestAuthInterceptor_RejectsMissingTokenOnPrivate(t *testing.T) {
	e := newEnv(t)
	_, err := e.client.GetMe(context.Background(), &authv1.GetMeRequest{})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))

	_, err = e.client.GetMe(withBearer("forged"), &authv1.GetMeRequest{})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.NotContains(t, status.Convert(err).Message(), "forged")
}

func TestAuthInterceptor_SetsClaims(t *testing.T) {
	e := newEnv(t)
	u, err := e.client.GetMe(withBearer(e.issuer.Sign(t, "u42", authz.RoleCandidate, time.Minute)), &authv1.GetMeRequest{})
	require.NoError(t, err)
	assert.Equal(t, "u42", u.Id)
}

func TestAuthInterceptor_RejectsExpiredToken(t *testing.T) {
	e := newEnv(t)
	_, err := e.client.GetMe(withBearer(e.issuer.Sign(t, "u42", authz.RoleCandidate, -time.Minute)), &authv1.GetMeRequest{})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestPublicMethod_NoToken_OK(t *testing.T) {
	e := newEnv(t)
	u, err := e.client.Register(context.Background(), validRegister())
	require.NoError(t, err)
	assert.Equal(t, "new", u.UserId)
}

func TestPublicMethod_InvalidToken_PassesThrough(t *testing.T) {
	e := newEnv(t)
	_, err := e.client.Register(withBearer("forged"), validRegister())
	require.NoError(t, err)
}

func TestValidation_InvalidArgument(t *testing.T) {
	e := newEnv(t)
	req := validRegister()
	req.Email = "x"
	_, err := e.client.Register(context.Background(), req)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestErrorMapping(t *testing.T) {
	e := newEnv(t)
	e.fake.register = func(context.Context, *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
		return nil, apperr.New(apperr.ErrConflict, "dup")
	}
	_, err := e.client.Register(context.Background(), validRegister())
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
	assert.Equal(t, "dup", status.Convert(err).Message())
}

func TestRecovery(t *testing.T) {
	e := newEnv(t)
	e.fake.register = func(context.Context, *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
		panic("boom secret")
	}
	_, err := e.client.Register(context.Background(), validRegister())
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.NotContains(t, status.Convert(err).Message(), "boom")

	e.fake.register = nil
	_, err = e.client.Register(context.Background(), validRegister())
	require.NoError(t, err, "server must survive the panic")
}

func TestRequestID_PropagatedFromMetadata(t *testing.T) {
	e := newEnv(t)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-request-id", "req-123")
	_, err := e.client.Register(ctx, validRegister())
	require.NoError(t, err)
	assert.Equal(t, "req-123", e.fake.gotReqID)

	_, err = e.client.Register(context.Background(), validRegister())
	require.NoError(t, err)
	assert.NotEmpty(t, e.fake.gotReqID, "an id is generated when absent")
}

type failingVerifier struct{}

func (failingVerifier) Verify(string) (authz.Claims, error) {
	return authz.Claims{}, apperr.New(apperr.ErrUnauthenticated, "bad")
}

func TestHealth_PrivateByDefault(t *testing.T) {
	e := newEnv(t)
	hc := grpc_health_v1.NewHealthClient(e.conn)
	_, err := hc.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestHealth_PublicWhenListed(t *testing.T) {
	srv, hs := grpcx.NewServer(grpcx.ServerConfig{
		Logger:        logger.New("test", discard{}, 0),
		Verifier:      failingVerifier{},
		PublicMethods: []string{"/grpc.health.v1.Health/Check"},
	})
	require.NotNil(t, hs)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn := bufDial(t, lis)
	resp, err := grpc_health_v1.NewHealthClient(conn).Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.Status)
}

func slowRegister(ctx context.Context, _ *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
	select {
	case <-time.After(5 * time.Second):
	case <-ctx.Done():
	}
	return &authv1.RegisterResponse{}, nil
}

func TestDial_DefaultTimeout(t *testing.T) {
	e := newEnv(t)
	e.fake.register = slowRegister
	start := time.Now()
	_, err := e.client.Register(context.Background(), validRegister())
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	assert.InDelta(t, 3.0, time.Since(start).Seconds(), 1.0)
}

func TestDial_RespectsExistingDeadline(t *testing.T) {
	e := newEnv(t)
	e.fake.register = slowRegister
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := e.client.Register(ctx, validRegister())
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	assert.Less(t, time.Since(start), 2*time.Second)
}

func TestServe_GracefulOnCancel(t *testing.T) {
	srv, _ := grpcx.NewServer(grpcx.ServerConfig{Logger: logger.New("test", discard{}, 0), Verifier: failingVerifier{}})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- grpcx.Serve(ctx, srv, lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer ccancel()
	// Unauthenticated (health is private) proves the server answers.
	_, err = grpc_health_v1.NewHealthClient(conn).Check(cctx, &grpc_health_v1.HealthCheckRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	start := time.Now()
	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
		assert.Less(t, time.Since(start), time.Second)
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after cancel")
	}
}

func TestErrorMapping_LogsRootCauseOfInternal(t *testing.T) {
	var buf syncBuf
	e := newEnvWithLog(t, &buf)
	e.fake.register = func(context.Context, *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
		return nil, errors.New("db exploded")
	}
	_, err := e.client.Register(context.Background(), validRegister())
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Equal(t, "internal error", status.Convert(err).Message())
	assert.Contains(t, buf.String(), "db exploded")
	assert.Contains(t, buf.String(), registerMethod)
}
