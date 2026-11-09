// Package middleware holds the gateway's base HTTP middleware.
package middleware

import (
	"crypto/rand"
	"net/http"
	"regexp"

	"github.com/oklog/ulid/v2"

	"github.com/nuroqzavitex/smarthire/pkg/logger"
)

const requestIDHeader = "X-Request-ID"

var validRequestID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// RequestID accepts a well-formed inbound X-Request-ID or generates a ULID,
// echoes it in the response and stores it in the request context.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestIDHeader)
		if !validRequestID.MatchString(id) {
			id = ulid.MustNew(ulid.Now(), rand.Reader).String()
		}
		w.Header().Set(requestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(logger.WithRequestID(r.Context(), id)))
	})
}
