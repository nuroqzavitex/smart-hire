package wsproxy_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/coder/websocket"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/wsproxy"
)

const origin = "http://localhost:3001"

func tickets(t *testing.T) (*wsproxy.Tickets, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return wsproxy.NewTickets(rdb, 30*time.Second), mr
}

func TestTicket_OneUse(t *testing.T) {
	tk, mr := tickets(t)
	ctx := context.Background()
	ticket, err := tk.Issue(ctx, "user-1")
	require.NoError(t, err)
	assert.Len(t, ticket, 43, "32 random bytes, base64url without padding")
	assert.True(t, mr.Exists("gateway:ws:ticket:"+ticket))
	assert.Equal(t, 30*time.Second, mr.TTL("gateway:ws:ticket:"+ticket))

	user, err := tk.Redeem(ctx, ticket)
	require.NoError(t, err)
	assert.Equal(t, "user-1", user)
	_, err = tk.Redeem(ctx, ticket)
	assert.ErrorIs(t, err, wsproxy.ErrInvalidTicket)
}

func TestTicket_Expires(t *testing.T) {
	tk, mr := tickets(t)
	ticket, err := tk.Issue(context.Background(), "user-1")
	require.NoError(t, err)
	mr.FastForward(31 * time.Second)
	_, err = tk.Redeem(context.Background(), ticket)
	assert.ErrorIs(t, err, wsproxy.ErrInvalidTicket)
}

func TestTicketRoute_RequiresAuth(t *testing.T) {
	tk, _ := tickets(t)
	h := wsproxy.TicketHandler(tk, nil)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/ws/ticket", nil))
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/ws/ticket", nil)
	req = req.WithContext(authz.WithClaims(req.Context(), authz.Claims{Subject: "u-9", Role: authz.RoleCandidate}))
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var body struct {
		Ticket    string `json:"ticket"`
		ExpiresIn int    `json:"expires_in"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.NotEmpty(t, body.Ticket)
	assert.Equal(t, 30, body.ExpiresIn)
	user, err := tk.Redeem(context.Background(), body.Ticket)
	require.NoError(t, err)
	assert.Equal(t, "u-9", user)
}

// upstream is a fake notification /ws: it greets with the X-User-ID it saw,
// then sends "late" after delay.
func upstream(t *testing.T, delay time.Duration) *url.URL {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "" {
			http.Error(w, "query leaked", http.StatusBadRequest)
			return
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		ctx := c.CloseRead(r.Context())
		_ = c.Write(ctx, websocket.MessageText, []byte("hello "+r.Header.Get("X-User-ID")))
		select {
		case <-time.After(delay):
			_ = c.Write(ctx, websocket.MessageText, []byte("late"))
		case <-ctx.Done():
		}
		<-ctx.Done()
	}))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL + "/ws")
	require.NoError(t, err)
	return u
}

// gateway serves the proxy behind an http.Server with the given timeouts,
// like cmd/gateway does.
func gateway(t *testing.T, tk *wsproxy.Tickets, target *url.URL, timeout time.Duration) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &http.Server{
		Handler:           wsproxy.Handler(tk, target, []string{origin}, nil),
		ReadHeaderTimeout: time.Second,
		ReadTimeout:       timeout,
		WriteTimeout:      timeout,
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { _ = srv.Close() })
	return "ws://" + lis.Addr().String() + "/v1/ws"
}

func dial(t *testing.T, wsURL, ticket, org string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h := http.Header{}
	if org != "" {
		h.Set("Origin", org)
	}
	c, resp, err := websocket.Dial(ctx, wsURL+"?ticket="+url.QueryEscape(ticket), &websocket.DialOptions{HTTPHeader: h})
	if c != nil {
		t.Cleanup(func() { _ = c.CloseNow() })
	}
	return c, resp, err
}

func readText(t *testing.T, c *websocket.Conn, within time.Duration) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	_, data, err := c.Read(ctx)
	require.NoError(t, err)
	return string(data)
}

func TestWSProxy_ProxiesWithUserHeader(t *testing.T) {
	tk, _ := tickets(t)
	ws := gateway(t, tk, upstream(t, time.Hour), time.Minute)
	ticket, err := tk.Issue(context.Background(), "user-7")
	require.NoError(t, err)
	c, _, err := dial(t, ws, ticket, origin)
	require.NoError(t, err)
	assert.Equal(t, "hello user-7", readText(t, c, 5*time.Second))
}

func TestWSProxy_RejectsReusedOrMissingTicket(t *testing.T) {
	tk, _ := tickets(t)
	ws := gateway(t, tk, upstream(t, time.Hour), time.Minute)
	ticket, err := tk.Issue(context.Background(), "user-7")
	require.NoError(t, err)
	_, _, err = dial(t, ws, ticket, "")
	require.NoError(t, err, "no Origin (non-browser client) is allowed")
	_, resp, err := dial(t, ws, ticket, "")
	require.Error(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	_, resp, err = dial(t, ws, "", "")
	require.Error(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestWSProxy_RejectsForeignOrigin(t *testing.T) {
	tk, _ := tickets(t)
	ws := gateway(t, tk, upstream(t, time.Hour), time.Minute)
	ticket, err := tk.Issue(context.Background(), "user-7")
	require.NoError(t, err)
	_, resp, err := dial(t, ws, ticket, "https://evil.example")
	require.Error(t, err)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	_, err = tk.Redeem(context.Background(), ticket)
	require.NoError(t, err, "a refused origin does not burn the ticket")
}

func TestWSProxy_RedisDownIs503(t *testing.T) {
	tk, mr := tickets(t)
	ws := gateway(t, tk, upstream(t, time.Hour), time.Minute)
	mr.Close()
	_, resp, err := dial(t, ws, "anything", "")
	require.Error(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestWSProxy_OutlivesServerTimeouts(t *testing.T) {
	tk, _ := tickets(t)
	ws := gateway(t, tk, upstream(t, 600*time.Millisecond), 200*time.Millisecond)
	ticket, err := tk.Issue(context.Background(), "user-7")
	require.NoError(t, err)
	c, _, err := dial(t, ws, ticket, origin)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(readText(t, c, 5*time.Second), "hello"))
	assert.Equal(t, "late", readText(t, c, 5*time.Second), "message sent after Read/WriteTimeout still arrives")
}
