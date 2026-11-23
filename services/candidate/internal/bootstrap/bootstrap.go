// Package bootstrap wires the candidate service and runs it until shutdown.
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
	"sync/atomic"
	"time"

	"google.golang.org/grpc/health/grpc_health_v1"

	candidatev1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/candidate/v1"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/event/eventschema"
	"github.com/nuroqzavitex/smarthire/pkg/grpcx"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
	"github.com/nuroqzavitex/smarthire/pkg/logger"
	"github.com/nuroqzavitex/smarthire/pkg/otel"
	"github.com/nuroqzavitex/smarthire/pkg/outbox"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/aiclient"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/consumer"
	candgrpc "github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/grpc"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/httpx"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/jobclient"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/schema"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/storage"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/config"
)

const (
	serviceName     = "candidate"
	shutdownTimeout = 30 * time.Second
	verifierMaxWait = 30 * time.Second
	readHeaderLimit = 5 * time.Second
)

var errNotServing = errors.New("grpc server not serving")

// ParseLevel converts a LOG_LEVEL string to a slog level (default info).
func ParseLevel(s string) slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo
	}
	return l
}

// readiness reports ready only while the gRPC server is serving and the
// database answers. The HTTP probe server starts first (liveness works while
// the JWKS is still being fetched), so the DB alone must not make it ready.
func readiness(serving *atomic.Bool, ping func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		if !serving.Load() {
			return errNotServing
		}
		return ping(ctx)
	}
}

// Run starts the service and blocks until ctx is cancelled and shutdown
// completes, or a server, the relay or the result consumer fails.
func Run(ctx context.Context, cfg config.Config) error {
	log := logger.New(serviceName, os.Stdout, ParseLevel(cfg.LogLevel))
	slog.SetDefault(log)

	// The result consumer validates events against contracts/events at
	// runtime; fail fast instead of dead-lettering every event.
	if _, err := eventschema.Dir(); err != nil {
		return fmt.Errorf("event schemas: %w", err)
	}

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

	store, err := storage.New(storage.Config{
		Endpoint:       cfg.S3Endpoint,
		PublicEndpoint: cfg.S3PublicEndpoint,
		AccessKey:      cfg.S3AccessKey,
		SecretKey:      cfg.S3SecretKey,
		Bucket:         cfg.S3Bucket,
		Region:         cfg.S3Region,
		UseSSL:         cfg.S3UseSSL,
	})
	if err != nil {
		return err
	}

	// Lazy connections: AI or job being down only fails recommendations.
	aiConn, err := grpcx.Dial(cfg.AIAddr)
	if err != nil {
		return fmt.Errorf("dial ai: %w", err)
	}
	defer func() { _ = aiConn.Close() }()
	jobConn, err := grpcx.Dial(cfg.JobAddr)
	if err != nil {
		return fmt.Errorf("dial job: %w", err)
	}
	defer func() { _ = jobConn.Close() }()

	svc := app.NewService(app.Deps{
		Candidates:   postgres.NewCandidateRepo(pool),
		CVs:          postgres.NewCVRepo(pool),
		Storage:      store,
		Outbox:       postgres.NewOutboxWriter(pool),
		Idem:         postgres.NewIdempotency(pool),
		Tx:           postgres.NewTxManager(pool),
		Schema:       schema.Validator{},
		UploadTTL:    cfg.UploadURLTTL,
		Logger:       log,
		Recommender:  aiclient.New(aiConn, cfg.AIBreakerFailures, cfg.AIBreakerOpen, cfg.AITimeout),
		JobDirectory: jobclient.New(jobConn, cfg.JobTimeout),
		Quota:        postgres.NewQuotaRepo(pool),
		DailyQuota:   cfg.DailyApplicationQuota,
	})

	results, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: cfg.KafkaBrokers,
		Group:   consumer.Group,
		Topics:  consumer.Topics,
		Handler: consumer.NewHandler(svc),
		Logger:  log,
	})
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	defer results.Close() // runs before producer and pool close; Run is awaited earlier

	var serving atomic.Bool
	httpSrv := &http.Server{
		Handler:           httpx.NewHandler(readiness(&serving, pool.Ping)),
		ReadHeaderTimeout: readHeaderLimit,
	}
	httpLis, err := net.Listen("tcp", ":"+strconv.Itoa(cfg.HTTPPort))
	if err != nil {
		return fmt.Errorf("listen http: %w", err)
	}
	httpErr := make(chan error, 1)
	go func() { httpErr <- httpSrv.Serve(httpLis) }()
	log.Info("http listening", "addr", httpLis.Addr().String())

	verifier, err := authz.NewVerifierWithRetry(ctx, cfg.JWKSURL, cfg.JWTIssuer, cfg.JWTAudience, verifierMaxWait, log)
	if err != nil {
		_ = httpSrv.Close()
		return err
	}
	defer verifier.Close()

	grpcSrv, hs := grpcx.NewServer(grpcx.ServerConfig{
		Logger:        log,
		Verifier:      verifier,
		PublicMethods: append([]string{grpc_health_v1.Health_Check_FullMethodName}, candgrpc.PublicMethods...),
	})
	candidatev1.RegisterCandidateServiceServer(grpcSrv, candgrpc.NewHandler(svc))
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
	// The listener is bound and Serve is accepting from it: from here a gRPC
	// call through the gateway can succeed, so /readyz may report ready.
	serving.Store(true)

	// outbox.pending for the backlog alert (ADR 0013).
	if reg, err := outbox.RegisterPendingGauge(otel.Meter(serviceName), pool); err != nil {
		log.Warn("outbox gauge not registered", "err", err)
	} else {
		defer func() { _ = reg.Unregister() }()
	}
	relay := outbox.NewRelay(pool, producer, outbox.RelayOptions{Interval: cfg.OutboxInterval, Logger: log})
	relayDone := make(chan error, 1)
	go func() { relayDone <- relay.Run(runCtx) }()

	consumerDone := make(chan error, 1)
	go func() { consumerDone <- results.Run(runCtx) }()

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
	case err := <-consumerDone:
		consumerDone = nil
		if err == nil {
			err = errors.New("stopped unexpectedly")
		}
		runErr = fmt.Errorf("cv result consumer: %w", err)
		log.Error("cv result consumer stopped, shutting down", "err", err)
	}
	serving.Store(false)
	hs.Shutdown() // report NOT_SERVING while draining
	cancel()
	// Wait for the relay, the consumer and the gRPC server before the
	// deferred consumer, producer and pool close.
	if err := <-relayDone; err != nil && runErr == nil {
		runErr = fmt.Errorf("outbox relay: %w", err)
	}
	if consumerDone != nil {
		if err := <-consumerDone; err != nil && runErr == nil {
			runErr = fmt.Errorf("cv result consumer: %w", err)
		}
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
