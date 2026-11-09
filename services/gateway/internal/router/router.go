// Package router assembles the gateway's HTTP handler.
package router

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/netip"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	authv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/auth/v1"
	"github.com/nuroqzavitex/smarthire/pkg/grpcx"
	"github.com/nuroqzavitex/smarthire/pkg/logger"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/authhttp"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/config"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/httperr"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/middleware"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/ratelimit"
)

// Deps are the router's collaborators.
type Deps struct {
	Log      *slog.Logger
	AuthConn *grpc.ClientConn
	Verifier grpcx.TokenVerifier
	Limiter  *ratelimit.Limiter
	Cfg      config.Config
	Ready    func(context.Context) error
}

// ParseTrustedProxies parses CIDRs (a bare IP is accepted as a single host).
func ParseTrustedProxies(cidrs []string) ([]netip.Prefix, error) {
	var out []netip.Prefix
	for _, s := range cidrs {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if p, err := netip.ParsePrefix(s); err == nil {
			out = append(out, p.Masked())
			continue
		}
		a, err := netip.ParseAddr(s)
		if err != nil {
			return nil, fmt.Errorf("invalid trusted proxy %q", s)
		}
		out = append(out, netip.PrefixFrom(a, a.BitLen()))
	}
	return out, nil
}

// incomingHeaderMatcher forwards only X-Request-ID. grpc-gateway itself always
// forwards Authorization as "authorization"; matching it here too would send
// the header twice.
func incomingHeaderMatcher(key string) (string, bool) {
	if strings.EqualFold(key, "x-request-id") {
		return "x-request-id", true
	}
	return "", false
}

// maxBodyBytes caps request bodies forwarded to the grpc-gateway mux.
const maxBodyBytes = 64 << 10

// dropHeader never forwards gRPC response metadata or trailers to clients
// (no Grpc-Metadata-* / Grpc-Trailer-* headers).
func dropHeader(string) (string, bool) { return "", false }

// stripGRPCTimeout removes the client's Grpc-Timeout header so callers cannot
// override grpcx.DefaultTimeout with their own deadline.
func stripGRPCTimeout(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Del("Grpc-Timeout")
		next.ServeHTTP(w, r)
	})
}

// New builds the gateway handler. Chain order (ruling R16):
// Recovery, RequestID, otelhttp, Logging, CORS, ClientIP, OptionalAuth,
// ratelimit, RejectInvalidAuth, routes (RejectInvalidAuth applies to every
// route except the three cookie auth routes, ruling R19).
func New(ctx context.Context, d Deps) (http.Handler, error) {
	trusted, err := ParseTrustedProxies(d.Cfg.TrustedProxies)
	if err != nil {
		return nil, err
	}

	mux := runtime.NewServeMux(
		runtime.WithErrorHandler(httperr.GatewayErrorHandler),
		runtime.WithIncomingHeaderMatcher(incomingHeaderMatcher),
		runtime.WithOutgoingHeaderMatcher(dropHeader),
		runtime.WithOutgoingTrailerMatcher(dropHeader),
		runtime.WithMetadata(func(ctx context.Context, _ *http.Request) metadata.MD {
			if id := logger.RequestIDFrom(ctx); id != "" {
				return metadata.Pairs("x-request-id", id)
			}
			return nil
		}),
		runtime.WithForwardResponseOption(func(_ context.Context, w http.ResponseWriter, m proto.Message) error {
			if _, ok := m.(*authv1.RegisterResponse); ok {
				w.WriteHeader(http.StatusCreated)
			}
			return nil
		}),
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
			MarshalOptions: protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true},
		}),
	)
	client := authv1.NewAuthServiceClient(d.AuthConn)
	if err := authv1.RegisterAuthServiceHandlerClient(ctx, mux, client); err != nil {
		return nil, fmt.Errorf("register auth gateway handler: %w", err)
	}

	api := stripGRPCTimeout(http.MaxBytesHandler(mux, maxBodyBytes))

	userRule := ratelimit.UserRule()
	userRule.Limit = d.Cfg.RateLimitUserPerMin
	loginRule := ratelimit.LoginRule()
	loginRule.Limit = d.Cfg.RateLimitLoginPerMin

	r := chi.NewRouter()
	r.Use(
		middleware.Recovery(d.Log),
		middleware.RequestID,
		func(next http.Handler) http.Handler { return otelhttp.NewHandler(next, "gateway") },
		middleware.Logging(d.Log),
		cors.Handler(cors.Options{
			AllowedOrigins:   d.Cfg.CORSOrigins,
			AllowedMethods:   []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions},
			AllowedHeaders:   []string{"Authorization", "Content-Type", "X-Request-ID"},
			ExposedHeaders:   []string{"X-Request-ID", "X-RateLimit-Limit", "X-RateLimit-Remaining", "Retry-After"},
			AllowCredentials: true,
			MaxAge:           600,
		}),
		middleware.ClientIP(trusted),
		middleware.OptionalAuth(d.Verifier),
		ratelimit.Middleware(d.Limiter, d.Log, userRule, loginRule),
	)

	// Ruling R19: the cookie routes ignore the auth-failed marker (they are
	// still rate-limited by IP above), so a SPA that sends its expired access
	// token with the refresh call can recover its session.
	h := authhttp.New(client, authhttp.Config{CookieSecure: d.Cfg.CookieSecure, AllowedOrigins: d.Cfg.CORSOrigins})
	r.Post("/v1/auth/login", h.Login)
	r.Post("/v1/auth/refresh", h.Refresh)
	r.Post("/v1/auth/logout", h.Logout)

	r.Group(func(r chi.Router) {
		r.Use(middleware.RejectInvalidAuth)
		r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
		r.Get("/readyz", func(w http.ResponseWriter, req *http.Request) {
			if d.Ready != nil {
				if err := d.Ready(req.Context()); err != nil {
					httperr.Write(w, req, status.Error(codes.Unavailable, "not ready"))
					return
				}
			}
			w.WriteHeader(http.StatusOK)
		})
		r.With(middleware.RequireAuth).Get("/v1/me", api.ServeHTTP)
		r.Handle("/v1/*", api)
	})
	return r, nil
}
