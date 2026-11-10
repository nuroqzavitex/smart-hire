//go:build integration

// Package integration runs the job service against a real Postgres.
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

	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/postgres"
)

var (
	pool *pgxpool.Pool
	dsn  string
)

const migrationsDir = "../../migrations"

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

// run starts Postgres as the default superuser, then creates job_svc as a
// plain (non-superuser) owner of job_db, like deploy/docker does, and runs
// migrations as that role so trusted-extension behavior is exercised.
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
	adminDSN, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintln(os.Stderr, "dsn:", err)
		return 1
	}
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, "admin connect:", err)
		return 1
	}
	for _, stmt := range []string{
		`CREATE ROLE job_svc LOGIN PASSWORD 'pw' NOSUPERUSER NOCREATEDB NOCREATEROLE`,
		`CREATE DATABASE job_db OWNER job_svc`,
	} {
		if _, err := admin.Exec(ctx, stmt); err != nil {
			fmt.Fprintln(os.Stderr, stmt, err)
			return 1
		}
	}
	_ = admin.Close(ctx)
	host, err := ctr.Host(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "host:", err)
		return 1
	}
	port, err := ctr.MappedPort(ctx, "5432/tcp")
	if err != nil {
		fmt.Fprintln(os.Stderr, "port:", err)
		return 1
	}
	dsn = fmt.Sprintf("postgres://job_svc:pw@%s:%s/job_db?sslmode=disable", host, port.Port())
	mg, err := newMigrate()
	if err != nil {
		fmt.Fprintln(os.Stderr, "migrate init:", err)
		return 1
	}
	if err := mg.Up(); err != nil {
		fmt.Fprintln(os.Stderr, "migrate up:", err)
		return 1
	}
	_, _ = mg.Close()
	pool, err = postgres.NewPool(ctx, dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pool:", err)
		return 1
	}
	defer pool.Close()
	return m.Run()
}

func truncate(t *testing.T) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `TRUNCATE jobs, outbox, processed_events`); err != nil {
		t.Fatal(err)
	}
}
