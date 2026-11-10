// Package httpx serves the job service's internal HTTP endpoints: health probes.
package httpx

import (
	"context"
	"net/http"
	"time"
)

const readyTimeout = 2 * time.Second

// NewHandler serves /healthz (liveness) and /readyz (readiness, 503 when
// readyCheck fails).
func NewHandler(readyCheck func(ctx context.Context) error) http.Handler {
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
	return mux
}
