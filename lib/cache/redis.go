package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// keyPrefix follows the same xtrasync:<domain>:* convention as lib/jobs
// (xtrasync:jobs:*) and lib/lock (xtrasync:locks:*).
const keyPrefix = "xtrasync:cache:"

// RedisCache implements Cache against Redis/Valkey - the shared-storage
// counterpart to today's per-process, local-disk caches in lib/drivers
// (e.g. the OCI driver's layer-digest cache under os.TempDir(), s.
// ociCacheHasLayerDigest): those don't survive across separate xtralink
// instances behind a load balancer, this does. Each entry is a Redis Hash
// with "value"/"validator" fields (not RedisJSON - these are raw blobs, not
// JSON documents, s. package doc), TTL via Redis' native per-key expiry
// instead of xtraplatform-cache's manual TTL-file bookkeeping.
type RedisCache struct {
	client redis.UniversalClient
}

// NewRedisCache connects lazily, same as jobs.NewRedisBackend/
// lock.NewRedisLocker - a single entry connects to one node, more than one
// switches to cluster mode.
func NewRedisCache(nodes []string) *RedisCache {
	return &RedisCache{client: redis.NewUniversalClient(&redis.UniversalOptions{Addrs: nodes})}
}

func (c *RedisCache) Get(key string) (Entry, bool, error) {
	ctx := context.Background()
	vals, err := c.client.HMGet(ctx, keyPrefix+key, "value", "validator").Result()
	if err != nil {
		return Entry{}, false, err
	}
	// HMGET returns nil for every field on a missing key, and Redis Hash
	// fields are never empty-but-absent for a key that exists (we always
	// write both fields together in Put) - checking the first field is
	// enough to tell "missing/expired" apart from "present".
	value, ok := vals[0].(string)
	if !ok {
		return Entry{}, false, nil
	}
	validator, _ := vals[1].(string)
	return Entry{Value: []byte(value), Validator: validator}, true, nil
}

func (c *RedisCache) Put(key string, entry Entry, ttl time.Duration) error {
	ctx := context.Background()
	fullKey := keyPrefix + key
	if err := c.client.HSet(ctx, fullKey, "value", entry.Value, "validator", entry.Validator).Err(); err != nil {
		return err
	}
	if ttl <= 0 {
		return nil
	}
	// PExpire (millisecond precision), not Expire (whole seconds only) -
	// go-redis's Expire truncates any duration under 1s up to 1s, which
	// would silently turn a caller's short TTL into a much longer one.
	return c.client.PExpire(ctx, fullKey, ttl).Err()
}

func (c *RedisCache) Delete(key string) error {
	return c.client.Del(context.Background(), keyPrefix+key).Err()
}
