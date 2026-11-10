package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nuroqzavitex/smarthire/pkg/event"
)

func TestBackoff(t *testing.T) {
	b, m := 200*time.Millisecond, 2*time.Second
	assert.Equal(t, 200*time.Millisecond, backoff(1, b, m))
	assert.Equal(t, 400*time.Millisecond, backoff(2, b, m))
	assert.Equal(t, 1600*time.Millisecond, backoff(4, b, m))
	assert.Equal(t, 2*time.Second, backoff(5, b, m))
	assert.Equal(t, 2*time.Second, backoff(40, b, m))
}

func TestPermanent(t *testing.T) {
	e := Permanent(errors.New("bad"))
	assert.True(t, IsPermanent(fmt.Errorf("wrap: %w", e)))
	assert.False(t, IsPermanent(errors.New("x")))
	assert.Equal(t, "bad", e.Error())
}

func TestDLQHeaders_TruncatesError(t *testing.T) {
	h := dlqHeaders(&kgo.Record{Topic: "job.created", Partition: 2, Offset: 7}, errors.New(strings.Repeat("é", 600)), 3)
	assert.Equal(t, 512, utf8.RuneCountInString(h["dlq-error"]))
	assert.Equal(t, "3", h["dlq-attempts"])
	assert.Equal(t, "job.created", h["dlq-original-topic"])
	assert.Equal(t, "2", h["dlq-original-partition"])
	assert.Equal(t, "7", h["dlq-original-offset"])
}

func TestDLQTopic(t *testing.T) { assert.Equal(t, "a.dlq", DLQTopic("a")) }

func TestNewConsumer_ValidatesConfig(t *testing.T) {
	h := func(context.Context, event.Envelope, *kgo.Record) error { return nil }
	for name, cfg := range map[string]ConsumerConfig{
		"brokers": {Group: "g", Topics: []string{"t"}, Handler: h},
		"group":   {Brokers: []string{"b:1"}, Topics: []string{"t"}, Handler: h},
		"topics":  {Brokers: []string{"b:1"}, Group: "g", Handler: h},
	} {
		_, err := NewConsumer(cfg)
		assert.ErrorContains(t, err, "empty "+name, name)
	}
}

func TestDLQRecordHeaders_ReplacesOldDLQHeaders(t *testing.T) {
	orig := []kgo.RecordHeader{
		{Key: "traceparent", Value: []byte("tp")},
		{Key: "dlq-error", Value: []byte("old")},
		{Key: "dlq-attempts", Value: []byte("9")},
		{Key: "x", Value: []byte("1")},
	}
	got := dlqRecordHeaders(orig, &kgo.Record{Topic: "cv.parsed", Partition: 1, Offset: 4}, errors.New("new"), 2)
	want := []kgo.RecordHeader{
		{Key: "traceparent", Value: []byte("tp")},
		{Key: "x", Value: []byte("1")},
		{Key: "dlq-attempts", Value: []byte("2")},
		{Key: "dlq-error", Value: []byte("new")},
		{Key: "dlq-original-offset", Value: []byte("4")},
		{Key: "dlq-original-partition", Value: []byte("1")},
		{Key: "dlq-original-topic", Value: []byte("cv.parsed")},
	}
	assert.Equal(t, want, got, "exactly one dlq-error/dlq-attempts, originals first, 7 headers")
	assert.Len(t, orig, 4, "input slice untouched")
	assert.Equal(t, "dlq-error", orig[1].Key)
}
