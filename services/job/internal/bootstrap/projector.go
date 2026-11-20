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

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nuroqzavitex/smarthire/pkg/event/eventschema"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
	"github.com/nuroqzavitex/smarthire/pkg/logger"
	"github.com/nuroqzavitex/smarthire/pkg/otel"
	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/httpx"
	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/opensearch"
	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/job/internal/config"
	"github.com/nuroqzavitex/smarthire/services/job/internal/projection"
)

const (
	projectorName  = "job-projector"
	projectorGroup = "job-projector"
	ensureBackoff  = 2 * time.Second
	// An OpenSearch outage blocks the partition (retried with this backoff)
	// instead of dead-lettering events; each store call is bounded.
	retryBase        = 200 * time.Millisecond
	retryMax         = 10 * time.Second
	storeCallTimeout = 10 * time.Second
)

// projectorTopics are the job events the read model follows.
var projectorTopics = []string{"job.created", "job.updated", "job.closed", "job.deleted"}

var errNotReady = errors.New("read model not ready")

// RunProjector consumes job.* events into the OpenSearch read model until ctx
// is cancelled. /readyz turns ready once the index and aliases exist.
func RunProjector(ctx context.Context, cfg config.ProjectorConfig) error {
	log := logger.New(projectorName, os.Stdout, ParseLevel(cfg.LogLevel))
	slog.SetDefault(log)
	// Events are validated against contracts/events at runtime; fail fast
	// instead of dead-lettering every event.
	if _, err := eventschema.Dir(); err != nil {
		return fmt.Errorf("event schemas: %w", err)
	}
	shutdownOtel, err := otel.Setup(ctx, projectorName)
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
	client, err := opensearch.New(cfg.OpenSearchURL)
	if err != nil {
		return err
	}

	var ready atomic.Bool
	httpSrv := &http.Server{
		Handler: httpx.NewHandler(func(context.Context) error {
			if !ready.Load() {
				return errNotReady
			}
			return nil
		}),
		ReadHeaderTimeout: readHeaderLimit,
	}
	httpLis, err := net.Listen("tcp", ":"+strconv.Itoa(cfg.HTTPPort))
	if err != nil {
		return fmt.Errorf("listen http: %w", err)
	}
	httpErr := make(chan error, 1)
	go func() { httpErr <- httpSrv.Serve(httpLis) }()
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = httpSrv.Shutdown(sctx)
	}()
	log.Info("http listening", "addr", httpLis.Addr().String())

	if err := ensureIndex(ctx, client, log); err != nil {
		return err
	}
	ready.Store(true)

	p := &projection.Projector{
		Store: client, Validate: eventschema.Validate, AliasTTL: cfg.AliasTTL,
		IndexGone: opensearch.IndexGone, Transient: opensearch.Transient,
		RetryBase: retryBase, RetryMax: retryMax, CallTimeout: storeCallTimeout, Log: log,
	}
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: cfg.KafkaBrokers, Group: projectorGroup, Topics: projectorTopics,
		Handler: p.Handle, Logger: log,
	})
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	defer consumer.Close()
	log.Info("projecting job events", "topics", projectorTopics)

	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(ctx) }()
	select {
	case err := <-runErr:
		if err != nil && ctx.Err() == nil {
			return fmt.Errorf("consumer: %w", err)
		}
		return nil
	case err := <-httpErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		return <-runErr
	}
}

// ensureIndex retries EnsureIndex until it succeeds or ctx ends, so the
// projector can start before OpenSearch is healthy.
func ensureIndex(ctx context.Context, c *opensearch.Client, log *slog.Logger) error {
	for {
		err := c.EnsureIndex(ctx)
		if err == nil {
			return nil
		}
		log.Warn("read model index not ready, retrying", "error_type", fmt.Sprintf("%T", err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(ensureBackoff):
		}
	}
}

// RunRebuild rebuilds the read model from Postgres and returns the new index.
func RunRebuild(ctx context.Context, cfg config.ProjectorConfig, keepOld bool) (string, error) {
	pool, client, err := projectorDeps(ctx, cfg)
	if err != nil {
		return "", err
	}
	defer pool.Close()
	if err := client.EnsureIndex(ctx); err != nil {
		return "", err
	}
	return projection.Rebuild(ctx, postgres.NewJobRepo(pool), client,
		projection.RebuildOptions{AliasTTL: cfg.AliasTTL, KeepOld: keepOld})
}

// RunReconcile compares open jobs in Postgres with the read model.
func RunReconcile(ctx context.Context, cfg config.ProjectorConfig) (projection.Report, error) {
	pool, client, err := projectorDeps(ctx, cfg)
	if err != nil {
		return projection.Report{}, err
	}
	defer pool.Close()
	return projection.Reconcile(ctx, postgres.NewJobRepo(pool), client)
}

func projectorDeps(ctx context.Context, cfg config.ProjectorConfig) (*pgxpool.Pool, *opensearch.Client, error) {
	pool, err := postgres.NewPool(ctx, cfg.DBDSN)
	if err != nil {
		return nil, nil, err
	}
	client, err := opensearch.New(cfg.OpenSearchURL)
	if err != nil {
		pool.Close()
		return nil, nil, err
	}
	return pool, client, nil
}
