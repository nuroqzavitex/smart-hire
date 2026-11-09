package middleware

import (
	"context"
	"net"
	"net/http"
	"net/netip"
	"strings"
)

type clientIPKey struct{}

// ClientIPFrom returns the client IP resolved by ClientIP, or "" if unset.
func ClientIPFrom(ctx context.Context) string {
	s, _ := ctx.Value(clientIPKey{}).(string)
	return s
}

func isTrusted(trusted []netip.Prefix, a netip.Addr) bool {
	for _, p := range trusted {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// ClientIP resolves the client address. X-Forwarded-For is honoured only when
// the TCP peer is itself in trusted; the chain is then walked right to left
// and the first address not in trusted is the client. A malformed entry stops
// the walk (nothing to its left can be trusted) and the last trusted hop is used.
func ClientIP(trusted []netip.Prefix) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := resolveClientIP(trusted, r)
			ctx := context.WithValue(r.Context(), clientIPKey{}, ip)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func resolveClientIP(trusted []netip.Prefix, r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peer, err := netip.ParseAddr(host)
	if err != nil {
		return host
	}
	peer = peer.Unmap()
	if !isTrusted(trusted, peer) {
		return peer.String()
	}
	var hops []string
	for _, line := range r.Header.Values("X-Forwarded-For") {
		for _, h := range strings.Split(line, ",") {
			hops = append(hops, strings.TrimSpace(h))
		}
	}
	current := peer
	for i := len(hops) - 1; i >= 0; i-- {
		a, err := netip.ParseAddr(hops[i])
		if err != nil {
			return current.String()
		}
		a = a.Unmap()
		if !isTrusted(trusted, a) {
			return a.String()
		}
		current = a
	}
	return current.String()
}
