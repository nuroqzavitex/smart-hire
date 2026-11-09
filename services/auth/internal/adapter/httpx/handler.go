// Package httpx serves the auth service's internal HTTP endpoints: JWKS and
// health probes.
package httpx

import (
	"context"
	"net/http"
	"time"

	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/crypto"
)

const readyTimeout = 2 * time.Second

// NewHandler serves /.well-known/jwks.json, /healthz (liveness) and /readyz
// (readiness, 503 when readyCheck fails).
func NewHandler(keys *crypto.KeySet, readyCheck func(ctx context.Context) error) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /.well-known/jwks.json", crypto.JWKSHandler(keys))
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
