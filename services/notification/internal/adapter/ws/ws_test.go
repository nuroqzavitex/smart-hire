package ws_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/notification/internal/adapter/ws"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/domain"
)

var t0 = time.Date(2026, 11, 23, 9, 0, 0, 0, time.UTC)

func sample(user uuid.UUID) domain.Notification {
	return domain.Notification{ID: uuid.New(), UserID: user, Type: domain.TypeApplicationStatus, Title: "Hồ sơ của bạn",
		Body: "b", Data: map[string]string{"application_id": "a1"}, CreatedAt: t0}
}

func serve(t *testing.T, hub *ws.Hub) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(ws.Handler(hub, nil))
	t.Cleanup(srv.Close)
	return srv
}

func dial(t *testing.T, srv *httptest.Server, user string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h := http.Header{}
	if user != "" {
		h.Set("X-User-ID", user)
	}
	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/ws", &websocket.DialOptions{HTTPHeader: h})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.CloseNow() })
	return c
}

func read(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := c.Read(ctx)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(data, &m))
	return m
}

// waitConns waits until the hub has n connections for user.
func waitConns(t *testing.T, hub *ws.Hub, user uuid.UUID, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return hub.Connections(user) == n }, 5*time.Second, 10*time.Millisecond)
}

func TestWSHandler_StreamsPushedNotification(t *testing.T) {
	hub := ws.NewHub(4)
	srv := serve(t, hub)
	user := uuid.New()
	c := dial(t, srv, user.String())
	waitConns(t, hub, user, 1)

	n := sample(user)
	hub.Push(user, n)
	msg := read(t, c)
	assert.Equal(t, "notification.created", msg["type"])
	got := msg["notification"].(map[string]any)
	assert.Equal(t, n.ID.String(), got["id"])
	assert.Equal(t, "application_status", got["type"])
	assert.Equal(t, "Hồ sơ của bạn", got["title"])
	assert.Equal(t, map[string]any{"application_id": "a1"}, got["data"])
	assert.Equal(t, "2026-11-23T09:00:00Z", got["created_at"])
	assert.Nil(t, got["read_at"])
}

func TestHub_DeliversToAllConnectionsOfUserOnly(t *testing.T) {
	hub := ws.NewHub(4)
	srv := serve(t, hub)
	user, other := uuid.New(), uuid.New()
	a, b := dial(t, srv, user.String()), dial(t, srv, user.String())
	o := dial(t, srv, other.String())
	waitConns(t, hub, user, 2)
	waitConns(t, hub, other, 1)

	hub.Push(user, sample(user))
	assert.Equal(t, "notification.created", read(t, a)["type"])
	assert.Equal(t, "notification.created", read(t, b)["type"])
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, _, err := o.Read(ctx)
	assert.Error(t, err, "another user's connection gets nothing")
}

func TestHub_SlowClientDropped(t *testing.T) {
	hub := ws.NewHub(1)
	srv := serve(t, hub)
	user := uuid.New()
	dial(t, srv, user.String()) // never reads
	waitConns(t, hub, user, 1)

	start := time.Now()
	for range 200 {
		hub.Push(user, sample(user))
	}
	assert.Less(t, time.Since(start), time.Second, "Push never blocks the caller")
	waitConns(t, hub, user, 0)
}

func TestHub_PushWithoutConnectionsIsNoop(*testing.T) {
	hub := ws.NewHub(1)
	hub.Push(uuid.New(), sample(uuid.New()))
}

func TestWSHandler_RequiresUserHeader(t *testing.T) {
	srv := serve(t, ws.NewHub(1))
	for _, user := range []string{"", "not-a-uuid"} {
		req, err := http.NewRequest(http.MethodGet, srv.URL+"/ws", nil)
		require.NoError(t, err)
		if user != "" {
			req.Header.Set("X-User-ID", user)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "user %q", user)
	}
}

func TestHub_UnregistersOnClose(t *testing.T) {
	hub := ws.NewHub(4)
	srv := serve(t, hub)
	user := uuid.New()
	c := dial(t, srv, user.String())
	waitConns(t, hub, user, 1)
	require.NoError(t, c.Close(websocket.StatusNormalClosure, "bye"))
	waitConns(t, hub, user, 0)
}

func TestHub_DisconnectClosesOnlyThatUsersConnections(t *testing.T) {
	hub := ws.NewHub(4)
	srv := serve(t, hub)
	user, other := uuid.New(), uuid.New()
	a := dial(t, srv, user.String())
	dial(t, srv, user.String())
	o := dial(t, srv, other.String())
	waitConns(t, hub, user, 2)
	waitConns(t, hub, other, 1)

	hub.Disconnect(user)
	waitConns(t, hub, user, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, err := a.Read(ctx)
	assert.Error(t, err, "the socket is closed")
	assert.NotErrorIs(t, ctx.Err(), context.DeadlineExceeded, "closed by the server, not by the test timeout")

	hub.Push(other, sample(other))
	assert.Equal(t, "notification.created", read(t, o)["type"], "other users keep their socket")
	hub.Disconnect(uuid.New()) // unknown user: no-op
}
