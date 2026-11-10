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

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return fmt.Errorf("key %s not found on %s within 10s", key, topic)
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
		found := false
		fetches.EachRecord(func(r *kgo.Record) {
			if string(r.Key) == key {
				found = true
			}
		})
		if found {
			return nil
		}
	}
}
