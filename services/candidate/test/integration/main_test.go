//go:build integration

// Package integration runs the candidate service on real adapters against
// Postgres, MinIO (the locally built smarthire/minio image, E-R4) and Redpanda.
package integration

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/schema"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/storage"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
)

const (
	migrationsDir = "../../migrations"
	// Built locally by `make minio-image` (deploy/docker/minio/Dockerfile).
	minioImage    = "smarthire/minio:RELEASE.2025-04-22T22-12-26Z"
	redpandaImage = "docker.redpanda.com/redpandadata/redpanda:v24.2.7"
	policyFile    = "../../../../deploy/docker/init/minio/candidate-svc.json"
	bucket        = "cvs"
	rootUser      = "rootadmin"
	rootPassword  = "rootsecret123"
	svcAccessKey  = "candidate-svc"
	svcSecretKey  = "candidate-svc-secret"
	region        = "us-east-1"
)

var (
	pool   *pgxpool.Pool
	dsn    string
	svc    *app.Service
	store  *storage.MinIO // candidate-svc credentials (least privilege)
	root   *miniogo.Client
	broker string
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
	cleanups, err := setup(ctx)
	defer func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}()
	if err != nil {
		fmt.Fprintln(os.Stderr, "setup:", err)
		return 1
	}
	return m.Run()
}

func setup(ctx context.Context) ([]func(), error) {
	var cleanups []func()
	if err := setupPostgres(ctx, &cleanups); err != nil {
		return cleanups, err
	}
	endpoint, err := setupMinIO(ctx, &cleanups)
	if err != nil {
		return cleanups, err
	}
	if err := setupRedpanda(ctx, &cleanups); err != nil {
		return cleanups, err
	}
	store, err = storage.New(storage.Config{
		Endpoint: endpoint, PublicEndpoint: "http://" + endpoint,
		AccessKey: svcAccessKey, SecretKey: svcSecretKey, Bucket: bucket, Region: region,
	})
	if err != nil {
		return cleanups, err
	}
	svc = app.NewService(app.Deps{
		Candidates: postgres.NewCandidateRepo(pool),
		CVs:        postgres.NewCVRepo(pool),
		Storage:    store,
		Outbox:     postgres.NewOutboxWriter(pool),
		Idem:       postgres.NewIdempotency(pool),
		Tx:         postgres.NewTxManager(pool),
		Schema:     schema.Validator{},
		Quota:      postgres.NewQuotaRepo(pool),
	})
	return cleanups, nil
}

// setupPostgres starts Postgres, creates candidate_svc as the non-superuser
// owner of candidate_db (like deploy/docker) and migrates as that role.
func setupPostgres(ctx context.Context, cleanups *[]func()) error {
	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithPassword("su"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(90*time.Second)))
	if err != nil {
		return fmt.Errorf("start postgres: %w", err)
	}
	*cleanups = append(*cleanups, func() { _ = testcontainers.TerminateContainer(ctr) })
	adminDSN, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return err
	}
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return err
	}
	for _, stmt := range []string{
		`CREATE ROLE candidate_svc LOGIN PASSWORD 'pw' NOSUPERUSER NOCREATEDB NOCREATEROLE`,
		`CREATE DATABASE candidate_db OWNER candidate_svc`,
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
	dsn = fmt.Sprintf("postgres://candidate_svc:pw@%s:%s/candidate_db?sslmode=disable", host, port.Port())
	mg, err := newMigrate()
	if err != nil {
		return fmt.Errorf("migrate init: %w", err)
	}
	if err := mg.Up(); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}
	_, _ = mg.Close()
	pool, err = postgres.NewPool(ctx, dsn)
	if err != nil {
		return err
	}
	*cleanups = append(*cleanups, pool.Close)
	return nil
}

// setupMinIO starts the locally built MinIO image, creates the private cvs
// bucket and the candidate-svc user with the repo's policy file (no
// s3:ListBucket), and returns the mapped host:port.
func setupMinIO(ctx context.Context, cleanups *[]func()) (string, error) {
	policy, err := filepath.Abs(policyFile)
	if err != nil {
		return "", err
	}
	ctr, err := tcminio.Run(ctx, minioImage,
		tcminio.WithUsername(rootUser), tcminio.WithPassword(rootPassword),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			HostFilePath: policy, ContainerFilePath: "/init/candidate-svc.json", FileMode: 0o644,
		}))
	if err != nil {
		return "", fmt.Errorf("start minio: %w", err)
	}
	*cleanups = append(*cleanups, func() { _ = testcontainers.TerminateContainer(ctr) })
	for _, cmd := range [][]string{
		{"mc", "alias", "set", "local", "http://127.0.0.1:9000", rootUser, rootPassword},
		{"mc", "mb", "local/" + bucket},
		{"mc", "anonymous", "set", "none", "local/" + bucket},
		{"mc", "admin", "policy", "create", "local", "candidate-svc", "/init/candidate-svc.json"},
		{"mc", "admin", "user", "add", "local", svcAccessKey, svcSecretKey},
		{"mc", "admin", "policy", "attach", "local", "candidate-svc", "--user", svcAccessKey},
	} {
		code, out, err := ctr.Exec(ctx, cmd)
		if err != nil {
			return "", fmt.Errorf("%s: %w", strings.Join(cmd[:3], " "), err)
		}
		if code != 0 {
			b, _ := io.ReadAll(out)
			return "", fmt.Errorf("%s: exit %d: %s", strings.Join(cmd[:3], " "), code, b)
		}
	}
	endpoint, err := ctr.ConnectionString(ctx)
	if err != nil {
		return "", err
	}
	root, err = miniogo.New(endpoint, &miniogo.Options{
		Creds: credentials.NewStaticV4(rootUser, rootPassword, ""), Region: region,
	})
	if err != nil {
		return "", err
	}
	return endpoint, nil
}

func setupRedpanda(ctx context.Context, cleanups *[]func()) error {
	ctr, err := redpanda.Run(ctx, redpandaImage)
	if err != nil {
		return fmt.Errorf("start redpanda: %w", err)
	}
	*cleanups = append(*cleanups, func() { _ = testcontainers.TerminateContainer(ctr) })
	broker, err = ctr.KafkaSeedBroker(ctx)
	return err
}

// createTopics creates topics with 3 partitions and replication factor 1.
func createTopics(ctx context.Context, topics ...string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		return err
	}
	defer cl.Close()
	resp, err := kadm.NewClient(cl).CreateTopics(ctx, 3, 1, nil, topics...)
	if err != nil {
		return err
	}
	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("create topic %s: %w", r.Topic, r.Err)
		}
	}
	return nil
}

func truncate(t *testing.T) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `TRUNCATE candidates, cvs, outbox, processed_events, daily_quota, quota_reservations`); err != nil {
		t.Fatal(err)
	}
}

// asCandidate returns a context authenticated as candidate uid.
func asCandidate(uid uuid.UUID) context.Context {
	return authz.WithClaims(context.Background(), authz.Claims{Subject: uid.String(), Role: authz.RoleCandidate})
}
