package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nuroqzavitex/smarthire/services/gateway/internal/middleware"
)

func ipVia(mw func(http.Handler) http.Handler, r *http.Request) string {
	var got string
	mw(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = middleware.ClientIPFrom(r.Context())
	})).ServeHTTP(httptest.NewRecorder(), r)
	return got
}

func newReq(remote, xff string) *http.Request {
	r := httptest.NewRequest("POST", "/", nil)
	r.RemoteAddr = remote
	if xff != "" {
		r.Header.Set("X-Forwarded-For", xff)
	}
	return r
}

func TestClientIP_IgnoresXFFFromUntrusted(t *testing.T) {
	r := newReq("203.0.113.5:1234", "1.2.3.4")
	assert.Equal(t, "203.0.113.5", ipVia(middleware.ClientIP(nil), r))
}

func TestClientIP_UsesXFFFromTrusted(t *testing.T) {
	r := newReq("10.0.0.2:1", "9.9.9.9, 1.2.3.4, 10.0.0.3")
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	assert.Equal(t, "1.2.3.4", ipVia(middleware.ClientIP(trusted), r))
}

func TestClientIP_TrustedPeerWithoutXFF(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	assert.Equal(t, "10.0.0.2", ipVia(middleware.ClientIP(trusted), newReq("10.0.0.2:1", "")))
}

func TestClientIP_AllXFFTrustedUsesLeftmostHop(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	// every hop is internal, so nothing is spoofable; the originating internal hop wins
	assert.Equal(t, "10.0.0.5", ipVia(middleware.ClientIP(trusted), newReq("10.0.0.2:1", "10.0.0.5, 10.0.0.3")))
}

func TestClientIP_GarbageXFFEntryStopsWalk(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	// rightmost entry is not an IP: do not trust anything to its left
	assert.Equal(t, "10.0.0.2", ipVia(middleware.ClientIP(trusted), newReq("10.0.0.2:1", "1.2.3.4, bogus")))
}

func TestClientIP_IPv6AndMappedPeer(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	assert.Equal(t, "2001:db8::1", ipVia(middleware.ClientIP(trusted), newReq("[::ffff:10.0.0.2]:1", "2001:db8::1")))
}

func TestClientIP_MultipleXFFHeaderLines(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	r := newReq("10.0.0.2:1", "")
	r.Header.Add("X-Forwarded-For", "9.9.9.9")
	r.Header.Add("X-Forwarded-For", "1.2.3.4")
	assert.Equal(t, "1.2.3.4", ipVia(middleware.ClientIP(trusted), r))
}
