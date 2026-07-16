package cache

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

// requireRedis skips the test if no Redis instance is reachable, so `go
// test ./...` degrades gracefully (skip, not fail) without a local Redis -
// same convention as lib/jobs' requireRedis.
func requireRedis(t *testing.T) *RedisCache {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	c := NewRedisCache([]string{addr})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := c.client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not reachable at %s, skipping integration test: %v", addr, err)
	}
	return c
}

// uniqueKey returns a cache key unique to this test/call, so concurrent
// tests never collide on the same Redis key.
func uniqueKey(base string) string {
	return base + "-" + uuid.NewString()
}

// cleanupKey removes a cache entry once the test using it is done.
func cleanupKey(t *testing.T, c *RedisCache, key string) {
	t.Helper()
	t.Cleanup(func() {
		c.client.Del(context.Background(), keyPrefix+key)
	})
}
