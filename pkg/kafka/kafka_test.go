package kafka

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
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
