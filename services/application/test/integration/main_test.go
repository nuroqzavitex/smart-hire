//go:build integration

// Package integration runs the application service's use cases and saga on
// the real Postgres adapters; the job, candidate and AI services are
// in-process fakes.
package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
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

// setup creates application_svc as the non-superuser owner of
// application_db (like deploy/docker) and migrates as that role.
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
		`CREATE ROLE application_svc LOGIN PASSWORD 'pw' NOSUPERUSER NOCREATEDB NOCREATEROLE`,
		`CREATE DATABASE application_db OWNER application_svc`,
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
	dsn = fmt.Sprintf("postgres://application_svc:pw@%s:%s/application_db?sslmode=disable", host, port.Port())
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

// reset empties every table: Claim sees all due sagas, so tests must not
// leak sagas into each other.
func reset(t *testing.T) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`TRUNCATE saga_instances, application_history, applications, outbox, processed_events, closed_jobs`)
	require.NoError(t, err)
}

// clock is a settable, goroutine-safe time source.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type fakeJobs struct{ recruiter uuid.UUID }

func (f fakeJobs) GetJob(_ context.Context, id uuid.UUID) (app.JobInfo, error) {
	return app.JobInfo{ID: id, RecruiterID: f.recruiter, Open: true}, nil
}

type fakeCVs struct{}

func (fakeCVs) GetCV(_ context.Context, id uuid.UUID) (app.CVInfo, error) {
	return app.CVInfo{ID: id, Parsed: true}, nil
}

type fakeQuota struct {
	mu         sync.Mutex
	reserveErr error
	reserved   []uuid.UUID
	released   []uuid.UUID
}

func (f *fakeQuota) Reserve(_ context.Context, appID, _ uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reserved = append(f.reserved, appID)
	return f.reserveErr
}

func (f *fakeQuota) Release(_ context.Context, appID uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released = append(f.released, appID)
	return nil
}

func (f *fakeQuota) releasedIDs() []uuid.UUID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uuid.UUID(nil), f.released...)
}

type fakeScorer struct{}

func (fakeScorer) Score(context.Context, uuid.UUID, uuid.UUID) (app.Score, error) {
	return app.Score{Value: 72.25, Breakdown: []byte(`{"matched_skills":["Go"]}`), ModelVersion: "fake-hash@768"}, nil
}

type env struct {
	svc       *app.Service
	clock     *clock
	quota     *fakeQuota
	recruiter uuid.UUID
	apps      *postgres.ApplicationRepo
	sagas     *postgres.SagaRepo
}

func newEnv(t *testing.T) *env {
	t.Helper()
	reset(t)
	e := &env{
		clock:     &clock{t: time.Now().UTC().Truncate(time.Microsecond)},
		quota:     &fakeQuota{},
		recruiter: uuid.New(),
		apps:      postgres.NewApplicationRepo(pool),
		sagas:     postgres.NewSagaRepo(pool),
	}
	e.svc = app.NewService(app.Deps{
		Applications: e.apps,
		Sagas:        e.sagas,
		Outbox:       postgres.NewOutboxWriter(pool),
		Idem:         postgres.NewIdempotency(pool),
		ClosedJobs:   postgres.NewClosedJobRepo(pool),
		Tx:           postgres.NewTxManager(pool),
		Jobs:         fakeJobs{recruiter: e.recruiter},
		CVs:          fakeCVs{},
		Quota:        e.quota,
		Scorer:       fakeScorer{},
		Purger:       postgres.NewPurgeRepo(pool),
		Now:          e.clock.Now,
		SagaDeadline: time.Minute,
		SagaLease:    30 * time.Second,
		SagaBatch:    10,
	})
	return e
}

func asCandidate(id uuid.UUID) context.Context {
	return authz.WithClaims(context.Background(), authz.Claims{Subject: id.String(), Role: authz.RoleCandidate})
}

func (e *env) submit(t *testing.T) uuid.UUID {
	t.Helper()
	cand := uuid.New()
	a, err := e.svc.Submit(asCandidate(cand), app.SubmitInput{JobID: uuid.New(), CVID: uuid.New(), IdempotencyKey: "k"})
	require.NoError(t, err)
	return a.ID
}

func count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(), query, args...).Scan(&n))
	return n
}
