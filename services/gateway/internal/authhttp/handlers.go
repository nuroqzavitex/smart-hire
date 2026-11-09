package authhttp

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	authv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/auth/v1"
	"github.com/nuroqzavitex/smarthire/pkg/logger"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/httperr"
)

const maxBodyBytes = 4 << 10

// Config configures the handlers.
type Config struct {
	CookieSecure   bool
	AllowedOrigins []string
}

// Handlers serves the cookie-based auth routes.
type Handlers struct {
	client authv1.AuthServiceClient
	cfg    Config
}

// New returns Handlers calling client.
func New(client authv1.AuthServiceClient, cfg Config) *Handlers {
	return &Handlers{client: client, cfg: cfg}
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
}

// outCtx forwards the request ID to the auth service.
func outCtx(r *http.Request) context.Context {
	ctx := r.Context()
	if id := logger.RequestIDFrom(ctx); id != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", id)
	}
	return ctx
}

func writeTokens(w http.ResponseWriter, tp *authv1.TokenPair) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(tokenResponse{AccessToken: tp.GetAccessToken(), ExpiresIn: tp.GetExpiresIn()})
}

// originAllowed is the CSRF defence for cookie-authenticated routes: the
// Origin header must be present and listed.
func (h *Handlers) originAllowed(r *http.Request) bool {
	o := r.Header.Get("Origin")
	return o != "" && slices.Contains(h.cfg.AllowedOrigins, o)
}

func (h *Handlers) checkOrigin(w http.ResponseWriter, r *http.Request) bool {
	if h.originAllowed(r) {
		return true
	}
	httperr.Write(w, r, status.Error(codes.PermissionDenied, "origin not allowed"))
	return false
}

// Login exchanges credentials for an access token and sets the refresh cookie.
func (h *Handlers) Login(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		httperr.Write(w, r, status.Error(codes.InvalidArgument, "invalid request body"))
		return
	}
	tp, err := h.client.Login(outCtx(r), &authv1.LoginRequest{Email: req.Email, Password: req.Password})
	if err != nil {
		httperr.Write(w, r, err)
		return
	}
	h.setSession(w, tp.GetRefreshToken())
	writeTokens(w, tp)
}

// Refresh rotates the refresh cookie and returns a new access token.
func (h *Handlers) Refresh(w http.ResponseWriter, r *http.Request) {
	if !h.checkOrigin(w, r) {
		return
	}
	c, err := r.Cookie(refreshCookie)
	if err != nil || c.Value == "" {
		httperr.Write(w, r, status.Error(codes.Unauthenticated, "no session"))
		return
	}
	tp, err := h.client.Refresh(outCtx(r), &authv1.RefreshRequest{RefreshToken: c.Value})
	if err != nil {
		if status.Code(err) == codes.Unauthenticated {
			h.clearSession(w)
		}
		httperr.Write(w, r, err)
		return
	}
	h.setSession(w, tp.GetRefreshToken())
	writeTokens(w, tp)
}

// Logout revokes the refresh token (if any) and always clears both cookies.
func (h *Handlers) Logout(w http.ResponseWriter, r *http.Request) {
	if !h.checkOrigin(w, r) {
		return
	}
	if c, err := r.Cookie(refreshCookie); err == nil && c.Value != "" {
		// Best effort: the cookies are cleared regardless of the outcome.
		_, _ = h.client.Logout(outCtx(r), &authv1.LogoutRequest{RefreshToken: c.Value})
	}
	h.clearSession(w)
	w.WriteHeader(http.StatusNoContent)
}
