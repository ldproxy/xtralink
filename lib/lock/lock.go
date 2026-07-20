// Package lock implements a simple Redis-backed distributed lock with a
// heartbeat/lease, used by app/workflows to prevent the same workflow from
// running twice concurrently. Deliberately its own package, not part of
// lib/jobs: locking isn't a job-queue concern, even though it reuses the
// same Redis instance for convenience.
package lock

import (
	"context"
	"fmt"
	"os"
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

	// keyPrefix is scoped by cluster (s. NewRedisLocker), so only instances
	// sharing the same configuration - and thus the same cluster - contend
	// for the same lock.
	keyPrefix string
}

// NewRedisLocker builds a client for nodes (single Redis/Valkey node, or a
// cluster if more than one) - the same nodes list app.NewAppContext also
// passes to jobs.NewRedisBackend, since both share one Redis instance.
//
// cluster scopes every key this locker uses, so only instances sharing the
// same configuration contend for the same lock; if empty, it falls back to
// the hostname (best-effort - left empty if even that fails).
func NewRedisLocker(nodes []string, cluster string) *RedisLocker {
	if cluster == "" {
		if host, err := os.Hostname(); err == nil {
			cluster = host
		}
	}

	prefix := "xtrasync:locks:"
	if cluster != "" {
		prefix += cluster + ":"
	}

	return &RedisLocker{
		client:    redis.NewUniversalClient(&redis.UniversalOptions{Addrs: nodes}),
		keyPrefix: prefix,
	}
}

func (r *RedisLocker) Acquire(ctx context.Context, id string) (func(), bool, error) {
	key := r.keyPrefix + id

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
