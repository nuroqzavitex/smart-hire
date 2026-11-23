// Package ws pushes new notifications to the user's open WebSocket
// connections (ADR 0012). The gateway authenticates the connection with a
// one-use ticket and passes the user id in X-User-ID; this endpoint is only
// reachable inside the compose network.
package ws

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/services/notification/internal/domain"
)

// conn is one open WebSocket with its bounded outgoing queue.
type conn struct {
	out  chan []byte
	drop func()
}

// Hub keeps open connections per user in memory (one replica).
type Hub struct {
	mu    sync.Mutex
	queue int
	conns map[uuid.UUID]map[*conn]struct{}
}

// NewHub returns a hub buffering at most queue messages per connection.
func NewHub(queue int) *Hub {
	if queue < 1 {
		queue = 1
	}
	return &Hub{queue: queue, conns: map[uuid.UUID]map[*conn]struct{}{}}
}

// Push queues n on every connection of user without blocking: a connection
// whose queue is full is dropped (a slow client must not stall the
// consumer). Users without connections are skipped; they read via REST.
func (h *Hub) Push(user uuid.UUID, n domain.Notification) {
	msg, err := json.Marshal(message{Type: "notification.created", Notification: toJSON(n)})
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.conns[user] {
		select {
		case c.out <- msg:
		default:
			delete(h.conns[user], c)
			c.drop()
		}
	}
	if len(h.conns[user]) == 0 {
		delete(h.conns, user)
	}
}

// Disconnect closes every connection of user (account deletion, ADR 0014).
func (h *Hub) Disconnect(user uuid.UUID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.conns[user] {
		c.drop()
	}
	delete(h.conns, user)
}

// Connections reports how many connections user has open.
func (h *Hub) Connections(user uuid.UUID) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.conns[user])
}

func (h *Hub) add(user uuid.UUID, drop func()) *conn {
	c := &conn{out: make(chan []byte, h.queue), drop: drop}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conns[user] == nil {
		h.conns[user] = map[*conn]struct{}{}
	}
	h.conns[user][c] = struct{}{}
	return c
}

func (h *Hub) remove(user uuid.UUID, c *conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.conns[user], c)
	if len(h.conns[user]) == 0 {
		delete(h.conns, user)
	}
}

type message struct {
	Type         string           `json:"type"`
	Notification notificationJSON `json:"notification"`
}

// notificationJSON uses the REST field names (grpc-gateway, proto names).
type notificationJSON struct {
	ID        string            `json:"id"`
	Type      string            `json:"type"`
	Title     string            `json:"title"`
	Body      string            `json:"body"`
	Data      map[string]string `json:"data"`
	ReadAt    *string           `json:"read_at"`
	CreatedAt string            `json:"created_at"`
}

func toJSON(n domain.Notification) notificationJSON {
	out := notificationJSON{
		ID: n.ID.String(), Type: string(n.Type), Title: n.Title, Body: n.Body, Data: n.Data,
		CreatedAt: n.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
	if out.Data == nil {
		out.Data = map[string]string{}
	}
	if n.ReadAt != nil {
		s := n.ReadAt.UTC().Format(time.RFC3339Nano)
		out.ReadAt = &s
	}
	return out
}
