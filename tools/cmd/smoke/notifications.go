package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coder/websocket"
)

type smokeNotification struct {
	ID     string            `json:"id"`
	Type   string            `json:"type"`
	Title  string            `json:"title"`
	Data   map[string]string `json:"data"`
	ReadAt *string           `json:"read_at"`
}

type notificationList struct {
	Notifications []smokeNotification `json:"notifications"`
	UnreadCount   int                 `json:"unread_count"`
}

// welcomeSteps covers M4a (ADR 0012) for the freshly registered smoke user:
// an in-app welcome notification and the welcome email in Mailpit.
func (s *smoke) welcomeSteps() []namedStep {
	return []namedStep{
		{"notifications: welcome in-app", func() error {
			access, err := s.accessFor(s.email, smokePassword)
			if err != nil {
				return err
			}
			return poll(30*time.Second, func() error {
				var out notificationList
				if err := s.getJSON("/v1/notifications", access, http.StatusOK, &out); err != nil {
					return err
				}
				for _, n := range out.Notifications {
					if n.Type == "welcome" {
						return nil
					}
				}
				return fmt.Errorf("no welcome among %d notifications", len(out.Notifications))
			})
		}},
		{"notifications: welcome email in mailpit", func() error {
			return poll(30*time.Second, func() error {
				q := url.Values{"query": {`to:"` + s.email + `"`}}
				resp, err := s.client.Get(s.mailpit + "/api/v1/search?" + q.Encode())
				if err != nil {
					return err
				}
				defer func() { _ = resp.Body.Close() }()
				var out struct {
					MessagesCount int `json:"messages_count"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
					return fmt.Errorf("decode mailpit search: %w", err)
				}
				if out.MessagesCount < 1 {
					return fmt.Errorf("no email for the smoke user yet")
				}
				return nil
			})
		}},
	}
}

// openNotifications trades the access token for a WebSocket ticket and
// connects to /v1/ws; it returns the connection and the ticket it used.
func (s *smoke) openNotifications(access string) (*websocket.Conn, string, error) {
	r, err := s.do(http.MethodPost, "/v1/ws/ticket", nil, bearer(access))
	if err != nil {
		return nil, "", err
	}
	if err := expectStatus(r, http.StatusOK); err != nil {
		return nil, "", err
	}
	var t struct {
		Ticket string `json:"ticket"`
	}
	if err := json.Unmarshal(r.body, &t); err != nil {
		return nil, "", fmt.Errorf("decode ticket: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, s.wsURL()+"?ticket="+url.QueryEscape(t.Ticket), nil)
	if err != nil {
		return nil, "", fmt.Errorf("dial websocket: %w", err)
	}
	return c, t.Ticket, nil
}

func (s *smoke) wsURL() string {
	return "ws" + strings.TrimPrefix(s.base, "http") + "/v1/ws"
}

// reusedTicketRejected checks a redeemed ticket cannot open a second socket.
func (s *smoke) reusedTicketRejected(ticket string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, resp, err := websocket.Dial(ctx, s.wsURL()+"?ticket="+url.QueryEscape(ticket), nil)
	if err == nil {
		_ = c.CloseNow()
		return fmt.Errorf("a reused ticket opened a websocket")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		return fmt.Errorf("reused ticket: %w", err)
	}
	return nil
}

// awaitPushed reads the socket until a notification of type kind arrives.
func awaitPushed(c *websocket.Conn, kind string, within time.Duration) (smokeNotification, error) {
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			return smokeNotification{}, fmt.Errorf("no %s notification over websocket: %w", kind, err)
		}
		var msg struct {
			Type         string            `json:"type"`
			Notification smokeNotification `json:"notification"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			return smokeNotification{}, fmt.Errorf("decode websocket message: %w", err)
		}
		if msg.Type == "notification.created" && msg.Notification.Type == kind {
			return msg.Notification, nil
		}
	}
}

// markRead marks a notification read and checks read_at and the unread count.
func (s *smoke) markRead(access, id string) error {
	var before notificationList
	if err := s.getJSON("/v1/notifications?unread_only=true", access, http.StatusOK, &before); err != nil {
		return err
	}
	r, err := s.do(http.MethodPost, "/v1/notifications/"+url.PathEscape(id)+":read", map[string]any{}, bearer(access))
	if err != nil {
		return err
	}
	if err := expectStatus(r, http.StatusOK); err != nil {
		return err
	}
	var n smokeNotification
	if err := json.Unmarshal(r.body, &n); err != nil {
		return fmt.Errorf("decode notification: %w", err)
	}
	if n.ReadAt == nil || *n.ReadAt == "" {
		return fmt.Errorf("read_at not set")
	}
	var after notificationList
	if err := s.getJSON("/v1/notifications?unread_only=true", access, http.StatusOK, &after); err != nil {
		return err
	}
	if after.UnreadCount != before.UnreadCount-1 {
		return fmt.Errorf("unread %d -> %d", before.UnreadCount, after.UnreadCount)
	}
	return nil
}
