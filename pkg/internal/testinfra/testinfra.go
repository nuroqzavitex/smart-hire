// Package testinfra starts throwaway infrastructure containers for integration tests.
package testinfra

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const redpandaImage = "docker.redpanda.com/redpandadata/redpanda:v24.2.7"

// Redpanda starts a single-node Redpanda with topic auto-creation disabled.
func Redpanda(ctx context.Context) (string, func(), error) {
	ctr, err := redpanda.Run(ctx, redpandaImage)
	if err != nil {
		return "", nil, fmt.Errorf("start redpanda: %w", err)
	}
	terminate := func() { _ = testcontainers.TerminateContainer(ctr) }
	broker, err := ctr.KafkaSeedBroker(ctx)
	if err != nil {
		terminate()
		return "", nil, fmt.Errorf("redpanda broker: %w", err)
	}
	if err := ensureNoAutoCreate(ctx, broker); err != nil {
		terminate()
		return "", nil, err
	}
	return broker, terminate, nil
}

func ensureNoAutoCreate(ctx context.Context, broker string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	cfgs, err := adm.DescribeBrokerConfigs(ctx)
	if err != nil {
		return fmt.Errorf("describe broker configs: %w", err)
	}
	for _, rc := range cfgs {
		for _, c := range rc.Configs {
			if c.Key == "auto_create_topics_enabled" && c.Value != nil && *c.Value == "false" {
				return nil
			}
		}
	}
	off := "false"
	resp, err := adm.AlterBrokerConfigs(ctx, []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "auto_create_topics_enabled", Value: &off}})
	if err != nil {
		return fmt.Errorf("disable auto create: %w", err)
	}
	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("disable auto create: %w", r.Err)
		}
	}
	return nil
}

// CreateTopics creates topics with 3 partitions and replication factor 1.
func CreateTopics(ctx context.Context, broker string, topics ...string) error {
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

// Postgres starts postgres:16-alpine and returns its DSN.
func Postgres(ctx context.Context) (string, func(), error) {
	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("test_db"), tcpostgres.WithUsername("test_svc"), tcpostgres.WithPassword("pw"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(90*time.Second)))
	if err != nil {
		return "", nil, fmt.Errorf("start postgres: %w", err)
	}
	terminate := func() { _ = testcontainers.TerminateContainer(ctr) }
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminate()
		return "", nil, fmt.Errorf("postgres dsn: %w", err)
	}
	return dsn, terminate, nil
}
