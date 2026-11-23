package ws

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

// UserHeader carries the authenticated user id set by the gateway.
const UserHeader = "X-User-ID"

const (
	pingEvery    = 30 * time.Second
	writeTimeout = 10 * time.Second
)

// Handler serves GET /ws: one WebSocket per request, fed by hub. The origin
// was already checked by the gateway, which is the only caller.
func Handler(hub *Hub, log *slog.Logger) http.Handler {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", func(w http.ResponseWriter, r *http.Request) {
		user, err := uuid.Parse(r.Header.Get(UserHeader))
		if err != nil {
			http.Error(w, "missing or invalid "+UserHeader, http.StatusBadRequest)
			return
		}
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return // Accept already wrote the error response
		}
		serveConn(r.Context(), hub, user, c, log)
	})
	return mux
}

func serveConn(parent context.Context, hub *Hub, user uuid.UUID, c *websocket.Conn, log *slog.Logger) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	// CloseRead discards client frames and cancels ctx when the client goes.
	ctx = c.CloseRead(ctx)
	conn := hub.add(user, cancel)
	defer hub.remove(user, conn)
	log.Debug("websocket opened", "user_id", user)

	ping := time.NewTicker(pingEvery)
	defer ping.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = c.Close(websocket.StatusGoingAway, "")
			return
		case msg := <-conn.out:
			if err := write(ctx, c, msg); err != nil {
				_ = c.CloseNow()
				return
			}
		case <-ping.C:
			pctx, pcancel := context.WithTimeout(ctx, writeTimeout)
			err := c.Ping(pctx)
			pcancel()
			if err != nil {
				_ = c.CloseNow()
				return
			}
		}
	}
}

func write(ctx context.Context, c *websocket.Conn, msg []byte) error {
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return c.Write(ctx, websocket.MessageText, msg)
}
