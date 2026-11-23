//go:build integration

// Package integration runs the notification service on the real Postgres
// adapters; the mailer and WebSocket pusher are in-process fakes.
package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/nuroqzavitex/smarthire/services/notification/internal/adapter/postgres"
)

const migrationsDir = "../../migrations"

var (
	pool *pgxpool.Pool
	dsn  string
)

func newMigrate() (*migrate.Migrate, error) {
	abs, err := filepath.Abs(migrationsDir)
	if err != nil {
		return nil, err
	}
	src, err := iofs.New(os.DirFS(abs), ".")
	if err != nil {
		return nil, err
	}
	return migrate.NewWithSourceInstance("iofs", src, strings.Replace(dsn, "postgres://", "pgx5://", 1))
}

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithPassword("su"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(90*time.Second)))
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres:", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(ctr) }()
	if err := setup(ctx, ctr); err != nil {
		fmt.Fprintln(os.Stderr, "setup:", err)
		return 1
	}
	defer pool.Close()
	return m.Run()
}

// setup creates notification_svc as the non-superuser owner of
// notification_db (like deploy/docker) and migrates as that role.
func setup(ctx context.Context, ctr *tcpostgres.PostgresContainer) error {
	adminDSN, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return err
	}
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return err
	}
	for _, stmt := range []string{
		`CREATE ROLE notification_svc LOGIN PASSWORD 'pw' NOSUPERUSER NOCREATEDB NOCREATEROLE`,
		`CREATE DATABASE notification_db OWNER notification_svc`,
	} {
		if _, err := admin.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	_ = admin.Close(ctx)
	host, err := ctr.Host(ctx)
	if err != nil {
		return err
	}
	port, err := ctr.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return err
	}
	dsn = fmt.Sprintf("postgres://notification_svc:pw@%s:%s/notification_db?sslmode=disable", host, port.Port())
	mg, err := newMigrate()
	if err != nil {
		return fmt.Errorf("migrate init: %w", err)
	}
	if err := mg.Up(); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}
	_, _ = mg.Close()
	pool, err = postgres.NewPool(ctx, dsn)
	return err
}
