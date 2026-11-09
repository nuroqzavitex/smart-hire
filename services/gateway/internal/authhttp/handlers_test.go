package authhttp_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	authv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/auth/v1"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/authhttp"
)

const allowedOrigin = "https://app.example"

type fakeClient struct {
	authv1.AuthServiceClient
	refreshErr   error
	loginCalls   int
	refreshCalls int
	logoutCalls  int
	lastLogout   string
	lastRefresh  string
}

func (f *fakeClient) Login(_ context.Context, in *authv1.LoginRequest, _ ...grpc.CallOption) (*authv1.TokenPair, error) {
	f.loginCalls++
	if in.GetEmail() != "a@x.com" {
		return nil, status.Error(codes.Unauthenticated, "bad credentials")
	}
	return &authv1.TokenPair{AccessToken: "AT", ExpiresIn: 900, RefreshToken: "RT"}, nil
}

func (f *fakeClient) Refresh(_ context.Context, in *authv1.RefreshRequest, _ ...grpc.CallOption) (*authv1.TokenPair, error) {
	f.refreshCalls++
	f.lastRefresh = in.GetRefreshToken()
	if f.refreshErr != nil {
		return nil, f.refreshErr
	}
	return &authv1.TokenPair{AccessToken: "AT2", ExpiresIn: 900, RefreshToken: "RT2"}, nil
}

func (f *fakeClient) Logout(_ context.Context, in *authv1.LogoutRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	f.logoutCalls++
	f.lastLogout = in.GetRefreshToken()
	return &emptypb.Empty{}, nil
}

func do(t *testing.T, fake *fakeClient, h func(*authhttp.Handlers) http.HandlerFunc, body string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	hs := authhttp.New(fake, authhttp.Config{CookieSecure: true, AllowedOrigins: []string{allowedOrigin}})
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/x", strings.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h(hs).ServeHTTP(rec, req)
	return rec
}

func login(hs *authhttp.Handlers) http.HandlerFunc   { return hs.Login }
func refresh(hs *authhttp.Handlers) http.HandlerFunc { return hs.Refresh }
func logout(hs *authhttp.Handlers) http.HandlerFunc  { return hs.Logout }

func cookie(rec *httptest.ResponseRecorder, name string) *http.Cookie {
	for _, c := range rec.Result().Cookies() {
		if c.Name == name {
			return c
		}
	}
	return nil
}

var jsonHdr = map[string]string{"Content-Type": "application/json"}

func TestLogin_RejectsNonJSONContentType(t *testing.T) {
	for _, ct := range []string{"text/plain", "application/x-www-form-urlencoded", "multipart/form-data; boundary=x", ""} {
		fake := &fakeClient{}
		rec := do(t, fake, login, `{"email":"a@x.com","password":"password1"}`, map[string]string{"Content-Type": ct})
		assert.Equal(t, 400, rec.Code, ct)
		assert.Contains(t, rec.Body.String(), `"INVALID_ARGUMENT"`, ct)
		assert.Equal(t, 0, fake.loginCalls, ct)
		assert.Nil(t, cookie(rec, "sh_rt"), ct)
	}
}

func TestLogin_AcceptsJSONWithCharset(t *testing.T) {
	rec := do(t, &fakeClient{}, login, `{"email":"a@x.com","password":"password1"}`,
		map[string]string{"Content-Type": "application/json; charset=utf-8"})
	assert.Equal(t, 200, rec.Code)
}

func TestLogin_RejectsBadOrigin(t *testing.T) {
	fake := &fakeClient{}
	rec := do(t, fake, login, `{"email":"a@x.com","password":"password1"}`,
		map[string]string{"Content-Type": "application/json", "Origin": "https://evil.example"})
	assert.Equal(t, 403, rec.Code)
	assert.Contains(t, rec.Body.String(), `"PERMISSION_DENIED"`)
	assert.Equal(t, 0, fake.loginCalls)
	assert.Nil(t, cookie(rec, "sh_rt"))
}

func TestLogin_AllowedOrNoOrigin(t *testing.T) {
	for _, hdr := range []map[string]string{
		{"Content-Type": "application/json"},
		{"Content-Type": "application/json", "Origin": allowedOrigin},
	} {
		rec := do(t, &fakeClient{}, login, `{"email":"a@x.com","password":"password1"}`, hdr)
		assert.Equal(t, 200, rec.Code, hdr)
	}
}

func TestLogin_SetsCookiesAndHidesRefreshToken(t *testing.T) {
	rec := do(t, &fakeClient{}, login, `{"email":"a@x.com","password":"password1"}`, jsonHdr)
	assert.Equal(t, 200, rec.Code)
	assert.JSONEq(t, `{"access_token":"AT","expires_in":900}`, rec.Body.String())
	assert.NotContains(t, rec.Body.String(), "RT")
	rt := cookie(rec, "sh_rt")
	require.NotNil(t, rt)
	assert.Equal(t, "RT", rt.Value)
	assert.True(t, rt.HttpOnly)
	assert.True(t, rt.Secure)
	assert.Equal(t, http.SameSiteStrictMode, rt.SameSite)
	assert.Equal(t, "/v1/auth", rt.Path)
	assert.Equal(t, 604800, rt.MaxAge)
	s := cookie(rec, "sh_session")
	require.NotNil(t, s)
	assert.Equal(t, "1", s.Value)
	assert.Equal(t, "/", s.Path)
	assert.True(t, s.HttpOnly)
	assert.Equal(t, http.SameSiteStrictMode, s.SameSite)
	assert.Equal(t, 604800, s.MaxAge)
}

func TestLogin_InsecureWhenConfigured(t *testing.T) {
	hs := authhttp.New(&fakeClient{}, authhttp.Config{CookieSecure: false})
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/login", strings.NewReader(`{"email":"a@x.com","password":"p"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	hs.Login(rec, req)
	assert.False(t, cookie(rec, "sh_rt").Secure)
}

func TestLogin_BadJSON(t *testing.T) {
	rec := do(t, &fakeClient{}, login, `{nope`, jsonHdr)
	assert.Equal(t, 400, rec.Code)
	assert.Contains(t, rec.Body.String(), `"INVALID_ARGUMENT"`)
	assert.Nil(t, cookie(rec, "sh_rt"))
}

func TestLogin_BodyTooLarge(t *testing.T) {
	rec := do(t, &fakeClient{}, login, `{"email":"`+strings.Repeat("a", 5000)+`"}`, jsonHdr)
	assert.Equal(t, 400, rec.Code)
}

func TestLogin_GRPCErrorMapped(t *testing.T) {
	rec := do(t, &fakeClient{}, login, `{"email":"zzz@x.com","password":"p"}`, jsonHdr)
	assert.Equal(t, 401, rec.Code)
	assert.Contains(t, rec.Body.String(), `"UNAUTHENTICATED"`)
	assert.Nil(t, cookie(rec, "sh_rt"))
}

func TestRefresh_RejectsBadOrigin(t *testing.T) {
	for _, hdr := range []map[string]string{
		{"Origin": "https://evil.example", "Cookie": "sh_rt=RT"},
		{"Cookie": "sh_rt=RT"},
	} {
		fake := &fakeClient{}
		rec := do(t, fake, refresh, "", hdr)
		assert.Equal(t, 403, rec.Code)
		assert.Contains(t, rec.Body.String(), `"PERMISSION_DENIED"`)
		assert.Equal(t, 0, fake.refreshCalls)
	}
}

func TestRefresh_MissingCookie(t *testing.T) {
	fake := &fakeClient{}
	rec := do(t, fake, refresh, "", map[string]string{"Origin": allowedOrigin})
	assert.Equal(t, 401, rec.Code)
	assert.Equal(t, 0, fake.refreshCalls)
}

func TestRefresh_Success_RotatesCookie(t *testing.T) {
	fake := &fakeClient{}
	rec := do(t, fake, refresh, "", map[string]string{"Origin": allowedOrigin, "Cookie": "sh_rt=RT"})
	assert.Equal(t, 200, rec.Code)
	assert.Equal(t, "RT", fake.lastRefresh)
	assert.JSONEq(t, `{"access_token":"AT2","expires_in":900}`, rec.Body.String())
	assert.Equal(t, "RT2", cookie(rec, "sh_rt").Value)
	assert.Equal(t, "1", cookie(rec, "sh_session").Value)
}

func TestRefresh_Unauthenticated_ClearsCookies(t *testing.T) {
	fake := &fakeClient{refreshErr: status.Error(codes.Unauthenticated, "revoked")}
	rec := do(t, fake, refresh, "", map[string]string{"Origin": allowedOrigin, "Cookie": "sh_rt=RT"})
	assert.Equal(t, 401, rec.Code)
	rt := cookie(rec, "sh_rt")
	require.NotNil(t, rt)
	assert.Less(t, rt.MaxAge, 0)
	s := cookie(rec, "sh_session")
	require.NotNil(t, s)
	assert.Less(t, s.MaxAge, 0)
}

func TestRefresh_OtherErrorKeepsCookies(t *testing.T) {
	fake := &fakeClient{refreshErr: status.Error(codes.Unavailable, "down")}
	rec := do(t, fake, refresh, "", map[string]string{"Origin": allowedOrigin, "Cookie": "sh_rt=RT"})
	assert.Equal(t, 503, rec.Code)
	assert.Nil(t, cookie(rec, "sh_rt"))
}

func TestLogout_AlwaysClears(t *testing.T) {
	fake := &fakeClient{}
	rec := do(t, fake, logout, "", map[string]string{"Origin": allowedOrigin})
	assert.Equal(t, 204, rec.Code)
	assert.Equal(t, 0, fake.logoutCalls)
	assert.Less(t, cookie(rec, "sh_rt").MaxAge, 0)
	assert.Less(t, cookie(rec, "sh_session").MaxAge, 0)

	rec = do(t, fake, logout, "", map[string]string{"Origin": allowedOrigin, "Cookie": "sh_rt=RT"})
	assert.Equal(t, 204, rec.Code)
	assert.Equal(t, 1, fake.logoutCalls)
	assert.Equal(t, "RT", fake.lastLogout)
	assert.Less(t, cookie(rec, "sh_rt").MaxAge, 0)
}

func TestLogout_RejectsBadOrigin(t *testing.T) {
	fake := &fakeClient{}
	rec := do(t, fake, logout, "", map[string]string{"Origin": "https://evil.example", "Cookie": "sh_rt=RT"})
	assert.Equal(t, 403, rec.Code)
	assert.Equal(t, 0, fake.logoutCalls)
}
