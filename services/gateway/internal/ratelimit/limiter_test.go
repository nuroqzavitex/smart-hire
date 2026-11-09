package ratelimit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/gateway/internal/ratelimit"
)

type fakeScripter struct {
	res any
	err error
}

func (f fakeScripter) reply(ctx context.Context) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	if f.err != nil {
		cmd.SetErr(f.err)
	} else {
		cmd.SetVal(f.res)
	}
	return cmd
}

func (f fakeScripter) Eval(ctx context.Context, _ string, _ []string, _ ...interface{}) *redis.Cmd {
	return f.reply(ctx)
}

func (f fakeScripter) EvalSha(ctx context.Context, _ string, _ []string, _ ...interface{}) *redis.Cmd {
	return f.reply(ctx)
}

func (f fakeScripter) EvalRO(ctx context.Context, _ string, _ []string, _ ...interface{}) *redis.Cmd {
	return f.reply(ctx)
}

func (f fakeScripter) EvalShaRO(ctx context.Context, _ string, _ []string, _ ...interface{}) *redis.Cmd {
	return f.reply(ctx)
}

func (f fakeScripter) ScriptExists(ctx context.Context, _ ...string) *redis.BoolSliceCmd {
	return redis.NewBoolSliceCmd(ctx)
}

func (f fakeScripter) ScriptLoad(ctx context.Context, _ string) *redis.StringCmd {
	return redis.NewStringCmd(ctx)
}

func TestLimiter_ParsesScriptReply(t *testing.T) {
	l := ratelimit.New(fakeScripter{res: []any{int64(0), int64(0), int64(6000)}})
	r, err := l.Allow(t.Context(), "k", 10, time.Minute)
	require.NoError(t, err)
	assert.False(t, r.Allowed)
	assert.Equal(t, 10, r.Limit)
	assert.Equal(t, 6*time.Second, r.RetryAfter)

	l = ratelimit.New(fakeScripter{res: []any{int64(1), int64(7), int64(0)}})
	r, err = l.Allow(t.Context(), "k", 10, time.Minute)
	require.NoError(t, err)
	assert.True(t, r.Allowed)
	assert.Equal(t, 7, r.Remaining)
}

type blockingScripter struct{ fakeScripter }

func (blockingScripter) block(ctx context.Context) *redis.Cmd {
	<-ctx.Done()
	cmd := redis.NewCmd(ctx)
	cmd.SetErr(ctx.Err())
	return cmd
}

func (b blockingScripter) EvalSha(ctx context.Context, _ string, _ []string, _ ...interface{}) *redis.Cmd {
	return b.block(ctx)
}

func (b blockingScripter) Eval(ctx context.Context, _ string, _ []string, _ ...interface{}) *redis.Cmd {
	return b.block(ctx)
}

func TestLimiter_AllowIsTimeBounded(t *testing.T) {
	l := ratelimit.New(blockingScripter{})
	start := time.Now()
	_, err := l.Allow(t.Context(), "k", 10, time.Minute)
	require.Error(t, err)
	assert.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestLimiter_ErrorAndMalformed(t *testing.T) {
	_, err := ratelimit.New(fakeScripter{err: errors.New("boom")}).Allow(t.Context(), "k", 1, time.Minute)
	require.Error(t, err)
	_, err = ratelimit.New(fakeScripter{res: "nope"}).Allow(t.Context(), "k", 1, time.Minute)
	require.Error(t, err)
	_, err = ratelimit.New(fakeScripter{res: []any{int64(1)}}).Allow(t.Context(), "k", 1, time.Minute)
	require.Error(t, err)
}
