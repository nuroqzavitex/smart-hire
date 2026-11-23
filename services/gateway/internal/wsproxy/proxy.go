package wsproxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"slices"
	"time"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
)

// UserHeader carries the authenticated user id to the notification service.
const UserHeader = "X-User-ID"

const redisTimeout = 500 * time.Millisecond

func discard(log *slog.Logger) *slog.Logger {
	if log == nil {
		return slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return log
}

// TicketHandler serves POST /v1/ws/ticket for an authenticated caller.
func TicketHandler(t *Tickets, log *slog.Logger) http.Handler {
	log = discard(log)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, ok := authz.ClaimsFrom(r.Context())
		if !ok || c.Subject == "" {
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "authentication required")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), redisTimeout)
		defer cancel()
		ticket, err := t.Issue(ctx, c.Subject)
		if err != nil {
			log.ErrorContext(r.Context(), "websocket ticket not issued", "err", err)
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "websocket temporarily unavailable")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(struct {
			Ticket    string `json:"ticket"`
			ExpiresIn int    `json:"expires_in"`
		}{ticket, int(t.TTL().Seconds())})
	})
}

// Handler serves GET /v1/ws?ticket=…: it checks Origin (browsers) against
// origins, redeems the ticket and proxies the WebSocket to target with the
// user id in X-User-ID. The connection outlives the server's read/write
// timeouts: their deadlines are cleared before the upgrade.
func Handler(t *Tickets, target *url.URL, origins []string, log *slog.Logger) http.Handler {
	log = discard(log)
	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme, pr.Out.URL.Host, pr.Out.URL.Path = target.Scheme, target.Host, target.Path
			pr.Out.URL.RawQuery = "" // the ticket stays at the gateway
			pr.Out.Host = target.Host
			pr.Out.Header.Del("Authorization")
			pr.Out.Header.Del("Cookie")
			pr.Out.Header.Del("Origin") // checked here; the upstream sees a same-origin request
			pr.Out.Header.Set(UserHeader, pr.In.Header.Get(UserHeader))
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.WarnContext(r.Context(), "websocket upstream failed", "err", err)
			writeError(w, http.StatusBadGateway, "UNAVAILABLE", "notifications temporarily unavailable")
		},
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if o := r.Header.Get("Origin"); o != "" && !slices.Contains(origins, o) {
			writeError(w, http.StatusForbidden, "PERMISSION_DENIED", "origin not allowed")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), redisTimeout)
		user, err := t.Redeem(ctx, r.URL.Query().Get("ticket"))
		cancel()
		switch {
		case errors.Is(err, ErrInvalidTicket):
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "invalid or expired websocket ticket")
			return
		case err != nil:
			log.ErrorContext(r.Context(), "websocket ticket not redeemed", "err", err)
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "websocket temporarily unavailable")
			return
		}
		rc := http.NewResponseController(w)
		_ = rc.SetReadDeadline(time.Time{})
		_ = rc.SetWriteDeadline(time.Time{})
		r = r.Clone(r.Context())
		r.Header.Set(UserHeader, user)
		proxy.ServeHTTP(w, r)
	})
}

// writeError writes the gateway's error envelope (spec §13.1).
func writeError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": msg, "details": []any{}}})
}
