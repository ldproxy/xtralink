package cache

import (
	"context"
	"testing"
	"time"
)

func TestRedisCache_PutAndGet(t *testing.T) {
	c := requireRedis(t)
	key := uniqueKey("put-get")
	cleanupKey(t, c, key)

	if err := c.Put(key, Entry{Value: []byte("hello"), Validator: "v1"}, 0); err != nil {
		t.Fatalf("Put: %v", err)
	}

	entry, ok, err := c.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatal("expected a cache hit")
	}
	if string(entry.Value) != "hello" || entry.Validator != "v1" {
		t.Errorf("entry = %+v, want Value=hello Validator=v1", entry)
	}
}

func TestRedisCache_GetReturnsMissForUnknownKey(t *testing.T) {
	c := requireRedis(t)

	entry, ok, err := c.Get(uniqueKey("missing"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Errorf("expected a miss, got %+v", entry)
	}
}

func TestRedisCache_PutWithoutValidatorLeavesItEmpty(t *testing.T) {
	c := requireRedis(t)
	key := uniqueKey("no-validator")
	cleanupKey(t, c, key)

	if err := c.Put(key, Entry{Value: []byte("blob")}, 0); err != nil {
		t.Fatalf("Put: %v", err)
	}

	entry, ok, err := c.Get(key)
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if entry.Validator != "" {
		t.Errorf("Validator = %q, want empty", entry.Validator)
	}
}

func TestRedisCache_PutOverwritesExistingEntry(t *testing.T) {
	c := requireRedis(t)
	key := uniqueKey("overwrite")
	cleanupKey(t, c, key)

	if err := c.Put(key, Entry{Value: []byte("old"), Validator: "v1"}, 0); err != nil {
		t.Fatalf("Put (first): %v", err)
	}
	if err := c.Put(key, Entry{Value: []byte("new"), Validator: "v2"}, 0); err != nil {
		t.Fatalf("Put (second): %v", err)
	}

	entry, ok, err := c.Get(key)
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if string(entry.Value) != "new" || entry.Validator != "v2" {
		t.Errorf("entry = %+v, want Value=new Validator=v2", entry)
	}
}

func TestRedisCache_Delete(t *testing.T) {
	c := requireRedis(t)
	key := uniqueKey("delete")
	cleanupKey(t, c, key)

	if err := c.Put(key, Entry{Value: []byte("gone-soon")}, 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := c.Delete(key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, ok, err := c.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Error("expected a miss after Delete")
	}
}

func TestRedisCache_TTLExpiresEntry(t *testing.T) {
	c := requireRedis(t)
	key := uniqueKey("ttl")
	cleanupKey(t, c, key)

	if err := c.Put(key, Entry{Value: []byte("short-lived")}, 50*time.Millisecond); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if _, ok, err := c.Get(key); err != nil || !ok {
		t.Fatalf("expected a hit right after Put: ok=%v err=%v", ok, err)
	}

	time.Sleep(150 * time.Millisecond)

	_, ok, err := c.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Error("expected the entry to have expired")
	}
}

func TestRedisCache_ZeroTTLNeverExpires(t *testing.T) {
	c := requireRedis(t)
	key := uniqueKey("no-ttl")
	cleanupKey(t, c, key)

	if err := c.Put(key, Entry{Value: []byte("forever")}, 0); err != nil {
		t.Fatalf("Put: %v", err)
	}

	ttl, err := c.client.TTL(context.Background(), keyPrefix+key).Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl != -1 {
		t.Errorf("TTL = %v, want -1 (no expiry)", ttl)
	}
}
