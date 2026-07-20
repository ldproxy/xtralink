package jobs

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

// requireRedis skips the test if no Redis instance is reachable, so `go
// test ./...` degrades gracefully (skip, not fail) without a local Redis -
// e.g. `docker compose up -d redis` not running.
func requireRedis(t *testing.T) *RedisBackend {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	b := NewRedisBackend([]string{addr}, "")
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := b.client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not reachable at %s, skipping integration test: %v", addr, err)
	}
	return b
}

// uniqueType returns a partial job type unique to this test/call, so
// concurrent tests never share a priority/queue key - both are keyed by
// partial job type in Redis, so two tests using e.g. "worker" would steal
// each other's partial jobs.
func uniqueType(base string) string {
	return base + "-" + uuid.NewString()
}

// cleanupJob removes a Job document (and its finalize-lock key) once the
// test using it is done.
func cleanupJob(t *testing.T, b *RedisBackend, id string) {
	t.Helper()
	t.Cleanup(func() {
		b.client.Del(context.Background(), b.keyJob+id, b.keyFinalized+id)
	})
}

// cleanupPartialJob removes a standalone PartialJob document and any trace
// of it in the taken list once the test using it is done (Done()/Error()
// normally do this as part of the real lifecycle; tests that call Take()
// without following through need it done explicitly).
func cleanupPartialJob(t *testing.T, b *RedisBackend, id string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		b.client.LRem(ctx, b.keyTaken, 0, id)
		b.client.Del(ctx, b.keyPartial+id)
	})
}
