package grpcx

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/oklog/ulid/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/logger"
)

const (
	requestIDHeader = "x-request-id"
	maxRequestIDLen = 128
)

func recoveryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.ErrorContext(ctx, "panic recovered", "method", info.FullMethod, "panic", r, "stack", string(debug.Stack()))
				resp, err = nil, status.Error(codes.Internal, "internal error")
			}
		}()
		return h(ctx, req)
	}
}

func streamRecoveryInterceptor(log *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("panic recovered", "method", info.FullMethod, "panic", r, "stack", string(debug.Stack()))
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return h(srv, ss)
	}
}

// requestIDInterceptor reads x-request-id from metadata (or generates one) and
// stores it in the context and response headers.
func requestIDInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		id := ""
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if v := md.Get(requestIDHeader); len(v) > 0 && len(v[0]) <= maxRequestIDLen {
				id = v[0]
			}
		}
		if id == "" {
			id = ulid.Make().String()
		}
		_ = grpc.SetHeader(ctx, metadata.Pairs(requestIDHeader, id))
		return h(logger.WithRequestID(ctx, id), req)
	}
}

func loggingInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := h(ctx, req)
		code := status.Code(err)
		level := slog.LevelInfo
		if code == codes.Internal || code == codes.Unknown {
			level = slog.LevelError
		}
		log.Log(ctx, level, "grpc request",
			"method", info.FullMethod, "code", code.String(), "duration_ms", time.Since(start).Milliseconds())
		return resp, err
	}
}

// errorMappingInterceptor converts handler errors to gRPC status errors. The
// root cause of Internal/Unknown errors is logged here because the client only
// ever receives a generic message.
func errorMappingInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		resp, err := h(ctx, req)
		if err == nil {
			return resp, nil
		}
		mapped := apperr.ToGRPC(err)
		if c := status.Code(mapped); c == codes.Internal || c == codes.Unknown {
			log.ErrorContext(ctx, "handler error", "method", info.FullMethod, "err", err)
		}
		return nil, mapped
	}
}

type auth struct {
	verifier TokenVerifier
	public   map[string]struct{}
}

func newAuth(v TokenVerifier, publicMethods []string) *auth {
	pub := make(map[string]struct{}, len(publicMethods))
	for _, m := range publicMethods {
		pub[m] = struct{}{}
	}
	return &auth{verifier: v, public: pub}
}

var errUnauthenticated = status.Error(codes.Unauthenticated, "unauthenticated")

// authenticate returns a context carrying claims when a valid bearer token is
// present. Verifier error details are never exposed to the caller; on public
// methods any token problem simply yields an anonymous context.
func (a *auth) authenticate(ctx context.Context, method string) (context.Context, error) {
	_, public := a.public[method]
	fail := func() (context.Context, error) {
		if public {
			return ctx, nil
		}
		return ctx, errUnauthenticated
	}
	if a.verifier == nil {
		return fail()
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return fail()
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return fail()
	}
	raw, ok := authz.BearerToken(vals[0])
	if !ok {
		return fail()
	}
	claims, err := a.verifier.Verify(raw)
	if err != nil {
		return fail()
	}
	ctx = authz.WithClaims(ctx, claims)
	return logger.WithUserID(ctx, claims.Subject), nil
}

func (a *auth) unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	ctx, err := a.authenticate(ctx, info.FullMethod)
	if err != nil {
		return nil, err
	}
	return h(ctx, req)
}

func (a *auth) stream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	ctx, err := a.authenticate(ss.Context(), info.FullMethod)
	if err != nil {
		return err
	}
	return h(srv, &ctxStream{ServerStream: ss, ctx: ctx})
}

type ctxStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *ctxStream) Context() context.Context { return s.ctx }
