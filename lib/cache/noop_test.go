package cache

import "testing"

func TestNoopCache_AlwaysMisses(t *testing.T) {
	var c NoopCache

	if err := c.Put("key", Entry{Value: []byte("value")}, 0); err != nil {
		t.Fatalf("Put: %v", err)
	}

	entry, ok, err := c.Get("key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Errorf("expected a miss, got %+v", entry)
	}
}

func TestNoopCache_DeleteIsNoop(t *testing.T) {
	var c NoopCache
	if err := c.Delete("key"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}
