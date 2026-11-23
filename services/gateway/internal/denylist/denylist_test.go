package denylist_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/gateway/internal/denylist"
)

func TestDenylist_AddContainsTTL(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	d := denylist.New(rdb, time.Hour)
	ctx := context.Background()

	ok, err := d.Contains(ctx, "u-1")
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, d.Add(ctx, "u-1"))
	ok, err = d.Contains(ctx, "u-1")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, time.Hour, mr.TTL("gateway:denylist:user:u-1"))

	mr.FastForward(time.Hour + time.Second)
	ok, err = d.Contains(ctx, "u-1")
	require.NoError(t, err)
	assert.False(t, ok, "expires after the TTL")
}

func TestDenylist_RedisErrors(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	d := denylist.New(rdb, time.Hour)
	mr.SetError("down")
	assert.Error(t, d.Add(context.Background(), "u-1"))
	_, err := d.Contains(context.Background(), "u-1")
	assert.Error(t, err)
}
