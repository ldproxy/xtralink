// Package lock implements a simple Redis-backed distributed lock with a
// heartbeat/lease, used by app/workflows to prevent the same workflow from
// running twice concurrently. Deliberately its own package, not part of
// lib/jobs: locking isn't a job-queue concern, even though it reuses the
// same Redis instance for convenience.
package lock

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Locker claims exclusive ownership of an id for as long as the caller
// holds it. Acquire never blocks waiting for a held lock - it reports
// ok=false immediately if someone else already has it.
type Locker interface {
	// Acquire tries to claim id exclusively. release must be called exactly
	// once when the caller is done, regardless of ok - it is a no-op if
	// ok is false.
	Acquire(ctx context.Context, id string) (release func(), ok bool, err error)
}

const keyPrefix = "xtrasync:locks:"

// ttl/interval: the lease is renewed every interval for as long as the
// holder keeps calling nothing (a background goroutine does it), so it
// outlives a single ttl window - but if the holding process crashes, the
// lease simply expires within one ttl instead of blocking every future
// attempt forever (unlike a single, long, one-shot TTL would).
const (
	ttl      = 30 * time.Second
	interval = 10 * time.Second
)

type RedisLocker struct {
	client redis.UniversalClient
}

// NewRedisLocker builds a client for nodes (single Redis/Valkey node, or a
// cluster if more than one) - the same nodes list app.NewAppContext also
// passes to jobs.NewRedisBackend, since both share one Redis instance.
func NewRedisLocker(nodes []string) *RedisLocker {
	return &RedisLocker{client: redis.NewUniversalClient(&redis.UniversalOptions{Addrs: nodes})}
}

func (r *RedisLocker) Acquire(ctx context.Context, id string) (func(), bool, error) {
	key := keyPrefix + id

	claimed, err := r.client.SetNX(ctx, key, "1", ttl).Result()
	if err != nil {
		return nil, false, fmt.Errorf("could not acquire lock %q: %w", id, err)
	}
	if !claimed {
		return func() {}, false, nil
	}

	leaseCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.client.Expire(context.Background(), key, ttl)
			case <-leaseCtx.Done():
				return
			}
		}
	}()

	release := func() {
		cancel()
		<-done
		r.client.Del(context.Background(), key)
	}
	return release, true, nil
}

// NoopLocker always successfully acquires - for tests and any environment
// where cross-process exclusivity doesn't matter (a real jobs.Backend
// already needs Redis, but a fakeBackend-based test doesn't).
type NoopLocker struct{}

func (NoopLocker) Acquire(ctx context.Context, id string) (func(), bool, error) {
	return func() {}, true, nil
}
