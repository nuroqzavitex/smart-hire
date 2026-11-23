// Package httpx serves the notification service's internal HTTP endpoints:
// health probes and the WebSocket endpoint the gateway proxies to.
package httpx

import (
	"context"
	"net/http"
	"time"
)

const readyTimeout = 2 * time.Second

// NewHandler serves /healthz, /readyz (503 when readyCheck fails) and hands
// /ws to ws.
func NewHandler(readyCheck func(ctx context.Context) error, ws http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
		defer cancel()
		if err := readyCheck(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("GET /ws", ws)
	return mux
}
