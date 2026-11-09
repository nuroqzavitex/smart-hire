// Package bootstrap wires the auth service and runs it until shutdown.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/health/grpc_health_v1"

	authv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/auth/v1"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/grpcx"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
	"github.com/nuroqzavitex/smarthire/pkg/logger"
	"github.com/nuroqzavitex/smarthire/pkg/otel"
	"github.com/nuroqzavitex/smarthire/pkg/outbox"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/crypto"
	authgrpc "github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/grpc"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/httpx"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/app"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/config"
)

const (
	serviceName     = "auth"
	shutdownTimeout = 30 * time.Second
	verifierTimeout = 15 * time.Second
	readHeaderLimit = 5 * time.Second
)

// ParseLevel converts a LOG_LEVEL string to a slog level (default info).
func ParseLevel(s string) slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo
	}
	return l
}

// NewService assembles the use-case service over the postgres adapters.
func NewService(pool *pgxpool.Pool, hasher app.PasswordHasher, issuer app.TokenIssuer,
	refreshTTL time.Duration, log *slog.Logger,
) (*app.Service, error) {
	return app.NewService(app.Deps{
		Users:      postgres.NewUserRepo(pool),
		Tokens:     postgres.NewRefreshTokenRepo(pool),
		Outbox:     postgres.NewOutboxWriter(pool),
		Tx:         postgres.NewTxManager(pool),
		Hasher:     hasher,
		Issuer:     issuer,
		Now:        time.Now,
		RefreshTTL: refreshTTL,
		Logger:     log,
	})
}

// Run starts the service and blocks until ctx is cancelled and shutdown
// completes, or a server fails.
func Run(ctx context.Context, cfg config.Config) error {
	log := logger.New(serviceName, os.Stdout, ParseLevel(cfg.LogLevel))
	slog.SetDefault(log)

	shutdownOtel, err := otel.Setup(ctx, serviceName)
	if err != nil {
		return fmt.Errorf("otel setup: %w", err)
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := shutdownOtel(sctx); err != nil {
			log.Warn("otel shutdown", "err", err)
		}
	}()

	pool, err := postgres.NewPool(ctx, cfg.DBDSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer producer.Close() // runs before pool.Close; the relay is awaited earlier

	keys, err := crypto.LoadKeys(cfg.JWTPrivateKeyPath, cfg.JWTExtraPublicKeyPaths...)
	if err != nil {
		return err
	}
	issuer := crypto.NewIssuer(keys, cfg.JWTIssuer, cfg.JWTAudience, cfg.AccessTTL)
	svc, err := NewService(pool, crypto.NewHasher(cfg.Argon2Params()), issuer, cfg.RefreshTTL, log)
	if err != nil {
		return err
	}

	// The HTTP server (JWKS) must be listening before the self-verifier fetches it.
	httpSrv := &http.Server{
		Handler:           httpx.NewHandler(keys, pool.Ping),
		ReadHeaderTimeout: readHeaderLimit,
	}
	httpLis, err := net.Listen("tcp", ":"+strconv.Itoa(cfg.HTTPPort))
	if err != nil {
		return fmt.Errorf("listen http: %w", err)
	}
	httpErr := make(chan error, 1)
	go func() { httpErr <- httpSrv.Serve(httpLis) }()
	log.Info("http listening", "addr", httpLis.Addr().String())

	vctx, vcancel := context.WithTimeout(ctx, verifierTimeout)
	verifier, err := authz.NewVerifier(vctx, cfg.JWKSURL, cfg.JWTIssuer, cfg.JWTAudience)
	vcancel()
	if err != nil {
		_ = httpSrv.Close()
		return err
	}
	defer verifier.Close()

	grpcSrv, hs := grpcx.NewServer(grpcx.ServerConfig{
		Logger:        log,
		Verifier:      verifier,
		PublicMethods: append([]string{grpc_health_v1.Health_Check_FullMethodName}, authgrpc.PublicMethods...),
	})
	authv1.RegisterAuthServiceServer(grpcSrv, authgrpc.NewHandler(svc))
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	grpcLis, err := net.Listen("tcp", ":"+strconv.Itoa(cfg.GRPCPort))
	if err != nil {
		_ = httpSrv.Close()
		return fmt.Errorf("listen grpc: %w", err)
	}
	log.Info("grpc listening", "addr", grpcLis.Addr().String())

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	grpcErr := make(chan error, 1)
	go func() { grpcErr <- grpcx.Serve(runCtx, grpcSrv, grpcLis) }()

	relay := outbox.NewRelay(pool, producer, outbox.RelayOptions{Interval: cfg.OutboxInterval, Logger: log})
	relayDone := make(chan error, 1)
	go func() { relayDone <- relay.Run(runCtx) }()

	var runErr error
	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case runErr = <-grpcErr:
		grpcErr = nil
	case err := <-httpErr:
		if !errors.Is(err, http.ErrServerClosed) {
			runErr = fmt.Errorf("http server: %w", err)
		}
	}
	hs.Shutdown() // report NOT_SERVING while draining
	cancel()
	// Wait for the relay before the deferred producer and pool close.
	if err := <-relayDone; err != nil && runErr == nil {
		runErr = fmt.Errorf("outbox relay: %w", err)
	}
	if grpcErr != nil {
		if err := <-grpcErr; err != nil && runErr == nil {
			runErr = fmt.Errorf("grpc server: %w", err)
		}
	}
	sctx, scancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer scancel()
	if err := httpSrv.Shutdown(sctx); err != nil && runErr == nil {
		runErr = fmt.Errorf("http shutdown: %w", err)
	}
	return runErr
}
