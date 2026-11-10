// Command gateway runs the SmartHire HTTP gateway.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc/connectivity"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/grpcx"
	"github.com/nuroqzavitex/smarthire/pkg/logger"
	"github.com/nuroqzavitex/smarthire/pkg/otel"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/config"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/ratelimit"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/router"
)

const (
	serviceName     = "gateway"
	verifierRetry   = 30 * time.Second
	shutdownTimeout = 30 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gateway:", err)
		os.Exit(1)
	}
}

func parseLevel(s string) slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo
	}
	return l
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := logger.New(serviceName, os.Stdout, parseLevel(cfg.LogLevel))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownOtel, err := otel.Setup(ctx, serviceName)
	if err != nil {
		return fmt.Errorf("otel setup: %w", err)
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownOtel(sctx); err != nil {
			log.Warn("otel shutdown", "error", err)
		}
	}()

	conn, err := grpcx.Dial(cfg.AuthGRPCAddr)
	if err != nil {
		return fmt.Errorf("dial auth: %w", err)
	}
	defer func() { _ = conn.Close() }()

	verifier, err := authz.NewVerifierWithRetry(ctx, cfg.AuthJWKSURL, cfg.JWTIssuer, cfg.JWTAudience, verifierRetry, log)
	if err != nil {
		return err
	}
	defer verifier.Close()

	rdb := ratelimit.NewRedisClient(cfg.RedisAddr)
	defer func() { _ = rdb.Close() }()

	handler, err := router.New(ctx, router.Deps{
		Log:      log,
		AuthConn: conn,
		Verifier: verifier,
		Limiter:  ratelimit.New(rdb),
		Cfg:      cfg,
		Ready: func(context.Context) error {
			if conn.GetState() == connectivity.Shutdown {
				return errors.New("auth connection closed")
			}
			return nil
		},
	})
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              net.JoinHostPort("", strconv.Itoa(cfg.HTTPPort)),
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Info("gateway listening", "addr", srv.Addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
	}
	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}
