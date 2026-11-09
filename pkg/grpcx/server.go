// Package grpcx is the shared gRPC server/client toolkit: interceptor chain,
// health service, graceful serving and a client dial helper.
package grpcx

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"

	"buf.build/go/protovalidate"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
)

// StopTimeout bounds graceful shutdown before the server is forcibly stopped.
const StopTimeout = 30 * time.Second

// TokenVerifier validates a raw access token. *authz.Verifier satisfies it.
type TokenVerifier interface {
	Verify(raw string) (authz.Claims, error)
}

// ServerConfig configures NewServer.
type ServerConfig struct {
	Logger   *slog.Logger
	Verifier TokenVerifier
	// PublicMethods are full method names (e.g. "/pkg.Svc/Method") that do not
	// require a token. A valid token on them is still honoured.
	PublicMethods []string
}

// NewServer builds a gRPC server with the standard unary interceptor chain
// (recovery, request ID, logging, error mapping, auth, validation) and the
// standard health service registered.
func NewServer(cfg ServerConfig) (*grpc.Server, *health.Server) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	validator, err := protovalidate.New()
	if err != nil {
		// protovalidate.New only fails on invalid options; none are passed.
		panic("grpcx: create validator: " + err.Error())
	}
	a := newAuth(cfg.Verifier, cfg.PublicMethods)

	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			recoveryInterceptor(cfg.Logger),
			requestIDInterceptor(),
			loggingInterceptor(cfg.Logger),
			errorMappingInterceptor(cfg.Logger),
			a.unary,
			grpcvalidate.UnaryServerInterceptor(validator),
		),
		// Streams get the same recovery and auth guarantees so a streaming
		// method can never bypass authentication.
		grpc.ChainStreamInterceptor(
			streamRecoveryInterceptor(cfg.Logger),
			a.stream,
		),
	)
	hs := health.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, hs)
	return srv, hs
}

// Serve serves lis until ctx is cancelled, then stops gracefully, forcing a
// stop after StopTimeout. It returns nil on a clean shutdown.
func Serve(ctx context.Context, srv *grpc.Server, lis net.Listener) error {
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(lis) }()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	stopped := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(StopTimeout):
		srv.Stop()
		<-stopped
	}
	if err := <-errCh; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}
