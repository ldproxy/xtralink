// Package cache provides a small, Redis-backed blob cache - the horizontal-
// scaling counterpart to today's per-process, local-disk caches in
// lib/drivers (e.g. the OCI driver's layer-digest cache, s. RedisCache's doc
// comment). Modeled after xtraplatform-cache's Cache/CacheDriver split, but
// collapsed into a single interface: Go doesn't need Java's separate
// variadic-key/prefix-wrapping public layer on top of a single-key driver
// SPI, one interface covers both.
package cache

import "time"

// Entry is a cached value plus an optional Validator (e.g. a digest/ETag) a
// caller can compare against its own current expected value to decide
// whether the cached Value is still fresh - mirrors xtraplatform-cache's
// hasValid(validator, key), but as one combined read instead of a separate
// has/get pair. Validator is empty for a plain blob cache entry that never
// needs freshness checks.
type Entry struct {
	Value     []byte
	Validator string
}

// Cache is a simple key/blob store: Get returns (Entry{}, false, nil) for a
// missing or expired key, never an error - a cache miss is a normal outcome,
// not a failure. Put with ttl==0 stores the entry with no expiry.
type Cache interface {
	Get(key string) (Entry, bool, error)
	Put(key string, entry Entry, ttl time.Duration) error
	Delete(key string) error
}

// NoopCache always misses and discards every Put - used when no Redis is
// configured (s. lock.NoopLocker for the same reasoning): callers that want
// to use a Cache can do so unconditionally, without special-casing "is a
// real cache configured" everywhere.
type NoopCache struct{}

func (NoopCache) Get(key string) (Entry, bool, error)                  { return Entry{}, false, nil }
func (NoopCache) Put(key string, entry Entry, ttl time.Duration) error { return nil }
func (NoopCache) Delete(key string) error                              { return nil }
