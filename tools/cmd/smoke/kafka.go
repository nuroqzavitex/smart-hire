package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// waitForKey reads topic from the beginning (no consumer group) until it sees
// a record with the given key, or fails after 10s.
func waitForKey(ctx context.Context, broker, topic, key string) error {
	return waitForKeyCount(ctx, broker, topic, key, 1)
}

// waitForKeyCount reads topic from the beginning until it saw want records
// with the given key, or fails after 10s.
func waitForKeyCount(ctx context.Context, broker, topic, key string, want int) error {
	if key == "" {
		return errors.New("empty key")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer cl.Close()

	seen := 0
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return fmt.Errorf("%d of %d records with key %s on %s within 10s", seen, want, key, topic)
		}
		var ferr error
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				ferr = fmt.Errorf("fetch %s[%d]: %w", t, p, err)
			}
		})
		if ferr != nil {
			return ferr
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if string(r.Key) == key {
				seen++
			}
		})
		if seen >= want {
			return nil
		}
	}
}
