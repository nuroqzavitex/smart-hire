package router_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	authv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/auth/v1"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/authz/authztest"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/config"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/ratelimit"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/router"
)

const origin = "https://app.example"

type fakeAuth struct {
	authv1.UnimplementedAuthServiceServer
	mu           sync.Mutex
	registerErr  error
	getMeCalls   int
	refreshCalls int
	getMeMD      metadata.MD
}

func (f *fakeAuth) Register(context.Context, *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.registerErr != nil {
		return nil, f.registerErr
	}
	return &authv1.RegisterResponse{UserId: "u-1"}, nil
}

func (f *fakeAuth) GetMe(ctx context.Context, _ *authv1.GetMeRequest) (*authv1.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getMeCalls++
	f.getMeMD, _ = metadata.FromIncomingContext(ctx)
	return &authv1.User{Id: "u-1", Email: "a@x.com"}, nil
}

func (f *fakeAuth) Login(context.Context, *authv1.LoginRequest) (*authv1.TokenPair, error) {
	return &authv1.TokenPair{AccessToken: "AT", ExpiresIn: 900, RefreshToken: "RT"}, nil
}

// allowAll is a redis.Scripter whose script always allows the request.
type allowAll struct{}

func (allowAll) reply(ctx context.Context) *redis.Cmd {
	c := redis.NewCmd(ctx)
	c.SetVal([]any{int64(1), int64(99), int64(0)})
	return c
}

func (a allowAll) Eval(ctx context.Context, _ string, _ []string, _ ...interface{}) *redis.Cmd {
	return a.reply(ctx)
}

func (a allowAll) EvalSha(ctx context.Context, _ string, _ []string, _ ...interface{}) *redis.Cmd {
	return a.reply(ctx)
}

func (a allowAll) EvalRO(ctx context.Context, _ string, _ []string, _ ...interface{}) *redis.Cmd {
	return a.reply(ctx)
}

func (a allowAll) EvalShaRO(ctx context.Context, _ string, _ []string, _ ...interface{}) *redis.Cmd {
	return a.reply(ctx)
}

func (allowAll) ScriptExists(ctx context.Context, _ ...string) *redis.BoolSliceCmd {
	return redis.NewBoolSliceCmd(ctx)
}

func (allowAll) ScriptLoad(ctx context.Context, _ string) *redis.StringCmd {
	return redis.NewStringCmd(ctx)
}

type env struct {
	h    http.Handler
	fake *fakeAuth
	is   *authztest.KeyIssuer
}

func setup(t *testing.T) *env {
	t.Helper()
	fake := &fakeAuth{}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	authv1.RegisterAuthServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	is := authztest.NewIssuer(t)
	v, err := authz.NewVerifier(t.Context(), is.JWKSURL(), authztest.Issuer, authztest.Audience)
	require.NoError(t, err)
	t.Cleanup(v.Close)

	h, err := router.New(t.Context(), router.Deps{
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		AuthConn: conn,
		Verifier: v,
		Limiter:  ratelimit.New(allowAll{}),
		Cfg: config.Config{
			CORSOrigins:  []string{origin},
			CookieSecure: true, RateLimitUserPerMin: 100, RateLimitLoginPerMin: 10,
		},
		Ready: func(context.Context) error { return nil },
	})
	require.NoError(t, err)
	return &env{h: h, fake: fake, is: is}
}

func (e *env) do(method, path, body string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	return rec
}

const registerBody = `{"email":"a@x.com","password":"password1","role":"ROLE_CANDIDATE"}`

var jsonHdr = map[string]string{"Content-Type": "application/json"}

func TestRouter_RegisterReturns201(t *testing.T) {
	e := setup(t)
	rec := e.do("POST", "/v1/auth/register", registerBody, jsonHdr)
	assert.Equal(t, 201, rec.Code, rec.Body.String())
	assert.JSONEq(t, `{"user_id":"u-1"}`, rec.Body.String())
}

func TestRouter_LoginSetsCookie(t *testing.T) {
	e := setup(t)
	rec := e.do("POST", "/v1/auth/login", `{"email":"a@x.com","password":"password1"}`, nil)
	assert.Equal(t, 200, rec.Code)
	assert.JSONEq(t, `{"access_token":"AT","expires_in":900}`, rec.Body.String())
	assert.Contains(t, strings.Join(rec.Header().Values("Set-Cookie"), "\n"), "sh_rt=RT")
}

func TestRouter_MeRequiresAuth(t *testing.T) {
	e := setup(t)
	rec := e.do("GET", "/v1/me", "", nil)
	assert.Equal(t, 401, rec.Code)
	assert.Contains(t, rec.Body.String(), `"UNAUTHENTICATED"`)
	assert.Equal(t, 0, e.fake.getMeCalls)
}

func TestRouter_InvalidTokenRejected(t *testing.T) {
	e := setup(t)
	rec := e.do("GET", "/v1/me", "", map[string]string{"Authorization": "Bearer garbage"})
	assert.Equal(t, 401, rec.Code)
	assert.Equal(t, 0, e.fake.getMeCalls)
}

func TestRouter_ForwardsAuthorizationAndRequestID(t *testing.T) {
	e := setup(t)
	tok := e.is.Sign(t, "u-1", authz.RoleCandidate, time.Minute)
	rec := e.do("GET", "/v1/me", "", map[string]string{
		"Authorization": "Bearer " + tok, "X-Request-ID": "r-1", "Cookie": "sh_rt=secret", "X-Evil": "1",
	})
	require.Equal(t, 200, rec.Code, rec.Body.String())
	assert.Equal(t, "r-1", rec.Header().Get("X-Request-ID"))
	md := e.fake.getMeMD
	assert.Equal(t, []string{"Bearer " + tok}, md.Get("authorization"))
	assert.Equal(t, []string{"r-1"}, md.Get("x-request-id"))
	assert.Empty(t, md.Get("cookie"))
	assert.Empty(t, md.Get("x-evil"))
	assert.Empty(t, md.Get("grpcgateway-cookie"))
}

func TestRouter_GRPCErrorUsesEnvelope(t *testing.T) {
	e := setup(t)
	e.fake.registerErr = status.Error(codes.AlreadyExists, "email taken")
	rec := e.do("POST", "/v1/auth/register", registerBody, jsonHdr)
	assert.Equal(t, 409, rec.Code)
	assert.Contains(t, rec.Body.String(), `"code":"ALREADY_EXISTS"`)
	assert.Contains(t, rec.Body.String(), `"request_id":"`)
}

func TestRouter_CORSPreflight(t *testing.T) {
	e := setup(t)
	rec := e.do("OPTIONS", "/v1/auth/refresh", "", map[string]string{
		"Origin": origin, "Access-Control-Request-Method": "POST",
		"Access-Control-Request-Headers": "content-type",
	})
	assert.Equal(t, "true", rec.Header().Get("Access-Control-Allow-Credentials"))
	assert.Equal(t, origin, rec.Header().Get("Access-Control-Allow-Origin"))
}

func (f *fakeAuth) Refresh(context.Context, *authv1.RefreshRequest) (*authv1.TokenPair, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refreshCalls++
	return &authv1.TokenPair{AccessToken: "AT2", ExpiresIn: 900, RefreshToken: "RT2"}, nil
}

func TestRouter_RefreshIgnoresInvalidAccessToken(t *testing.T) {
	e := setup(t)
	rec := e.do("POST", "/v1/auth/refresh", "", map[string]string{
		"Authorization": "Bearer garbage", "Origin": origin, "Cookie": "sh_rt=RT",
	})
	assert.Equal(t, 200, rec.Code, rec.Body.String())
	assert.Equal(t, 1, e.fake.refreshCalls)
	assert.JSONEq(t, `{"access_token":"AT2","expires_in":900}`, rec.Body.String())
}

func TestRouter_MeStillRejectsGarbageToken(t *testing.T) {
	e := setup(t)
	rec := e.do("GET", "/v1/me", "", map[string]string{"Authorization": "Bearer garbage"})
	assert.Equal(t, 401, rec.Code)
	assert.Equal(t, 0, e.fake.getMeCalls)
}

func TestRouter_Health(t *testing.T) {
	e := setup(t)
	assert.Equal(t, 200, e.do("GET", "/healthz", "", nil).Code)
	assert.Equal(t, 200, e.do("GET", "/readyz", "", nil).Code)
}
