package lock

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
)

func requireRedis(t *testing.T) *RedisLocker {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	l := NewRedisLocker(addr)
	if err := l.client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis not reachable at %s, skipping: %v", addr, err)
	}
	return l
}

func TestRedisLocker_SecondAcquireFailsWhileHeld(t *testing.T) {
	l := requireRedis(t)
	id := "test-" + uuid.NewString()

	release1, ok1, err := l.Acquire(context.Background(), id)
	if err != nil {
		t.Fatalf("Acquire (1st): %v", err)
	}
	if !ok1 {
		t.Fatal("expected the 1st Acquire to succeed")
	}
	defer release1()

	release2, ok2, err := l.Acquire(context.Background(), id)
	if err != nil {
		t.Fatalf("Acquire (2nd): %v", err)
	}
	if ok2 {
		t.Error("expected the 2nd Acquire of the same id to fail while the 1st is held")
	}
	release2() // no-op per contract, but must not panic
}

func TestRedisLocker_DifferentIdsBothSucceed(t *testing.T) {
	l := requireRedis(t)
	idA := "test-" + uuid.NewString()
	idB := "test-" + uuid.NewString()

	releaseA, okA, err := l.Acquire(context.Background(), idA)
	if err != nil {
		t.Fatalf("Acquire (a): %v", err)
	}
	if !okA {
		t.Fatal("expected Acquire(a) to succeed")
	}
	defer releaseA()

	releaseB, okB, err := l.Acquire(context.Background(), idB)
	if err != nil {
		t.Fatalf("Acquire (b): %v", err)
	}
	if !okB {
		t.Error("expected Acquire(b) to succeed even while a is held - different ids must not block each other")
	}
	defer releaseB()
}

func TestRedisLocker_ReacquireAfterRelease(t *testing.T) {
	l := requireRedis(t)
	id := "test-" + uuid.NewString()

	release1, ok1, err := l.Acquire(context.Background(), id)
	if err != nil {
		t.Fatalf("Acquire (1st): %v", err)
	}
	if !ok1 {
		t.Fatal("expected the 1st Acquire to succeed")
	}
	release1()

	release2, ok2, err := l.Acquire(context.Background(), id)
	if err != nil {
		t.Fatalf("Acquire (2nd, after release): %v", err)
	}
	if !ok2 {
		t.Error("expected Acquire to succeed again once the 1st lease was released")
	}
	defer release2()
}

func TestNoopLocker_AlwaysAcquires(t *testing.T) {
	var l NoopLocker
	release, ok, err := l.Acquire(context.Background(), "anything")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !ok {
		t.Error("expected NoopLocker to always acquire")
	}
	release() // must not panic
}
