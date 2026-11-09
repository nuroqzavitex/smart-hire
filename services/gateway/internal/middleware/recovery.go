package middleware

import (
	"log/slog"
	"net/http"
	"runtime/debug"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nuroqzavitex/smarthire/services/gateway/internal/httperr"
)

// Recovery turns a handler panic into a 500 error envelope and logs the panic.
func Recovery(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					if rec == http.ErrAbortHandler { //nolint:errorlint // sentinel panic value
						panic(rec)
					}
					log.ErrorContext(r.Context(), "panic recovered", "panic", rec, "stack", string(debug.Stack()))
					httperr.Write(w, r, status.Error(codes.Internal, "internal error"))
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
