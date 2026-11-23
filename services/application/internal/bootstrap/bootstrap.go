// Package bootstrap wires the application service and runs it until
// shutdown: gRPC API, submit saga worker, outbox relay and job-event
// consumer in one process.
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

	aiv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/ai/v1"
	applicationv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/application/v1"
	candidatev1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/candidate/v1"
	jobv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/job/v1"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/grpcx"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
	"github.com/nuroqzavitex/smarthire/pkg/logger"
	"github.com/nuroqzavitex/smarthire/pkg/otel"
	"github.com/nuroqzavitex/smarthire/pkg/outbox"
	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/aiclient"
	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/consumer"
	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/cvclient"
	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/dialx"
	appgrpc "github.com/nuroqzavitex/smarthire/services/application/internal/adapter/grpc"
	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/httpx"
	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/jobclient"
	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/quotaclient"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
	"github.com/nuroqzavitex/smarthire/services/application/internal/config"
)

const (
	serviceName     = "application"
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

func readiness(serving *atomic.Bool, ping func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		if !serving.Load() {
			return errNotServing
		}
		return ping(ctx)
	}
}

// Run starts the service and blocks until ctx is cancelled and shutdown
// completes, or a server, the relay, the worker or the consumer fails.
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
	defer producer.Close()

	// Lazy connections with the retry policy (spec §14.2): a dependency
	// being down only delays sagas or fails submits with 503.
	jobConn, err := grpcx.Dial(cfg.JobAddr, dialx.RetryPolicy(jobv1.JobService_ServiceDesc.ServiceName))
	if err != nil {
		return fmt.Errorf("dial job: %w", err)
	}
	defer func() { _ = jobConn.Close() }()
	candConn, err := grpcx.Dial(cfg.CandidateAddr, dialx.RetryPolicy(candidatev1.CandidateService_ServiceDesc.ServiceName))
	if err != nil {
		return fmt.Errorf("dial candidate: %w", err)
	}
	defer func() { _ = candConn.Close() }()
	aiConn, err := grpcx.Dial(cfg.AIAddr, dialx.RetryPolicy(aiv1.AIService_ServiceDesc.ServiceName))
	if err != nil {
		return fmt.Errorf("dial ai: %w", err)
	}
	defer func() { _ = aiConn.Close() }()

	var worker *app.Worker
	svc := app.NewService(app.Deps{
		Applications:        postgres.NewApplicationRepo(pool),
		Sagas:               postgres.NewSagaRepo(pool),
		Outbox:              postgres.NewOutboxWriter(pool),
		Idem:                postgres.NewIdempotency(pool),
		ClosedJobs:          postgres.NewClosedJobRepo(pool),
		Tx:                  postgres.NewTxManager(pool),
		Jobs:                jobclient.New(jobConn, cfg.CallTimeout),
		CVs:                 cvclient.New(candConn, cfg.CallTimeout),
		Quota:               quotaclient.New(candConn, cfg.CallTimeout),
		Scorer:              aiclient.New(aiConn, cfg.AIBreakerFailures, cfg.AIBreakerOpen, cfg.AITimeout, log),
		Logger:              log,
		Wake:                func() { worker.Wake() },
		SagaDeadline:        cfg.SagaDeadline,
		SagaLease:           cfg.SagaLease,
		SagaBatch:           cfg.SagaBatch,
		ScoreNotFoundWindow: cfg.ScoreNotFoundWindow,
	})
	worker = app.NewWorker(svc, cfg.SagaPoll, log)

	jobEvents, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: cfg.KafkaBrokers,
		Group:   consumer.Group,
		Topics:  consumer.Topics,
		Handler: consumer.NewHandler(svc),
		Logger:  log,
	})
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	defer jobEvents.Close()

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
		PublicMethods: append([]string{grpc_health_v1.Health_Check_FullMethodName}, appgrpc.PublicMethods...),
	})
	applicationv1.RegisterApplicationServiceServer(grpcSrv, appgrpc.NewHandler(svc))
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
	serving.Store(true)

	if reg, err := app.RegisterSagaGauge(otel.Meter(serviceName), postgres.NewSagaRepo(pool)); err != nil {
		log.Warn("saga gauge not registered", "err", err)
	} else {
		defer func() { _ = reg.Unregister() }()
	}
	// outbox.pending for the backlog alert (ADR 0013).
	if reg, err := outbox.RegisterPendingGauge(otel.Meter(serviceName), pool); err != nil {
		log.Warn("outbox gauge not registered", "err", err)
	} else {
		defer func() { _ = reg.Unregister() }()
	}
	relay := outbox.NewRelay(pool, producer, outbox.RelayOptions{Interval: cfg.OutboxInterval, Logger: log})
	relayDone := make(chan error, 1)
	go func() { relayDone <- relay.Run(runCtx) }()

	workerDone := make(chan error, 1)
	go func() { workerDone <- worker.Run(runCtx) }()

	consumerDone := make(chan error, 1)
	go func() { consumerDone <- jobEvents.Run(runCtx) }()

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
		runErr = fmt.Errorf("job event consumer: %w", err)
		log.Error("job event consumer stopped, shutting down", "err", err)
	}
	serving.Store(false)
	hs.Shutdown()
	cancel()
	// Wait for the background loops and the gRPC server before the deferred
	// consumer, producer and pool close.
	if err := <-relayDone; err != nil && runErr == nil {
		runErr = fmt.Errorf("outbox relay: %w", err)
	}
	if err := <-workerDone; err != nil && runErr == nil {
		runErr = fmt.Errorf("saga worker: %w", err)
	}
	if consumerDone != nil {
		if err := <-consumerDone; err != nil && runErr == nil {
			runErr = fmt.Errorf("job event consumer: %w", err)
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
