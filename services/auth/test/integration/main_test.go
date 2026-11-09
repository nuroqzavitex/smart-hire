//go:build integration

// Package integration runs the auth service against a real Postgres.
package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/app"
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

func run(m *testing.M) int {
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("auth_db"), tcpostgres.WithUsername("auth_svc"), tcpostgres.WithPassword("pw"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(90*time.Second)))
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres:", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(ctr) }()
	dsn, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintln(os.Stderr, "dsn:", err)
		return 1
	}
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
	if _, err := pool.Exec(context.Background(), `TRUNCATE users, refresh_tokens, outbox CASCADE`); err != nil {
		t.Fatal(err)
	}
}

func newService(t *testing.T, outbox app.OutboxWriter) *app.Service {
	t.Helper()
	return newServiceWith(t, postgres.NewRefreshTokenRepo(pool), outbox)
}

func newServiceWithTokens(t *testing.T, tokens app.RefreshTokenRepo) *app.Service {
	t.Helper()
	return newServiceWith(t, tokens, postgres.NewOutboxWriter(pool))
}

func newServiceWith(t *testing.T, tokens app.RefreshTokenRepo, outbox app.OutboxWriter) *app.Service {
	t.Helper()
	svc, err := app.NewService(app.Deps{
		Users:      postgres.NewUserRepo(pool),
		Tokens:     tokens,
		Outbox:     outbox,
		Tx:         postgres.NewTxManager(pool),
		Hasher:     fastHasher{},
		Issuer:     fakeIssuer{},
		Now:        time.Now,
		RefreshTTL: 7 * 24 * time.Hour,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}
