// Package authhttp implements the hand-written login/refresh/logout routes
// that keep the refresh token in an HttpOnly cookie.
package authhttp

import "net/http"

const (
	refreshCookie = "sh_rt"
	sessionCookie = "sh_session"
	refreshPath   = "/v1/auth"
	cookieMaxAge  = 7 * 24 * 60 * 60 // seconds, matches the refresh token TTL
)

func (h *Handlers) setSession(w http.ResponseWriter, refreshToken string) {
	http.SetCookie(w, &http.Cookie{ //nolint:gosec // Secure is configurable for plain-http local dev
		Name: refreshCookie, Value: refreshToken, Path: refreshPath, MaxAge: cookieMaxAge,
		HttpOnly: true, Secure: h.cfg.CookieSecure, SameSite: http.SameSiteStrictMode,
	})
	http.SetCookie(w, &http.Cookie{ //nolint:gosec // Secure is configurable for plain-http local dev
		Name: sessionCookie, Value: "1", Path: "/", MaxAge: cookieMaxAge,
		HttpOnly: true, Secure: h.cfg.CookieSecure, SameSite: http.SameSiteStrictMode,
	})
}

func (h *Handlers) clearSession(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{ //nolint:gosec // Secure is configurable for plain-http local dev
		Name: refreshCookie, Path: refreshPath, MaxAge: -1,
		HttpOnly: true, Secure: h.cfg.CookieSecure, SameSite: http.SameSiteStrictMode,
	})
	http.SetCookie(w, &http.Cookie{ //nolint:gosec // Secure is configurable for plain-http local dev
		Name: sessionCookie, Path: "/", MaxAge: -1,
		HttpOnly: true, Secure: h.cfg.CookieSecure, SameSite: http.SameSiteStrictMode,
	})
}
