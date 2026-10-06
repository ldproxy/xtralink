package app

import (
	"testing"

	"github.com/ldproxy/xtralink/lib/jobs"
	"github.com/ldproxy/xtralink/lib/lock"
	"github.com/rs/zerolog"
)

func TestNewJobsBackend_DefaultsToMemory(t *testing.T) {
	if _, ok := newJobsBackend(&Settings{}, zerolog.Nop()).(*jobs.MemoryBackend); !ok {
		t.Errorf("expected a *jobs.MemoryBackend for a zero-value Settings")
	}
	if _, ok := newJobsBackend(nil, zerolog.Nop()).(*jobs.MemoryBackend); !ok {
		t.Errorf("expected a *jobs.MemoryBackend for nil Settings")
	}
}

func TestNewJobsBackend_LocalQueueUsesMemory(t *testing.T) {
	settings := &Settings{General: GeneralConfig{Queue: "local"}}
	if _, ok := newJobsBackend(settings, zerolog.Nop()).(*jobs.MemoryBackend); !ok {
		t.Errorf("expected a *jobs.MemoryBackend for queue=local")
	}
}

func TestNewJobsBackend_RedisQueueUsesRedisBackend(t *testing.T) {
	settings := &Settings{General: GeneralConfig{Queue: "REDIS", Redis: []string{"localhost:6379"}}}
	if _, ok := newJobsBackend(settings, zerolog.Nop()).(*jobs.RedisBackend); !ok {
		t.Errorf("expected a *jobs.RedisBackend for queue=REDIS (case-insensitive)")
	}
}

func TestNewLocker_NoRedisNodesUsesNoop(t *testing.T) {
	if _, ok := newLocker(&Settings{}).(lock.NoopLocker); !ok {
		t.Errorf("expected a lock.NoopLocker when no settings.redis are configured")
	}
	if _, ok := newLocker(nil).(lock.NoopLocker); !ok {
		t.Errorf("expected a lock.NoopLocker for nil Settings")
	}
}

func TestNewLocker_RedisNodesUsesRedisLocker(t *testing.T) {
	settings := &Settings{General: GeneralConfig{Redis: []string{"localhost:6379"}}}
	if _, ok := newLocker(settings).(*lock.RedisLocker); !ok {
		t.Errorf("expected a *lock.RedisLocker when settings.redis is configured")
	}
}
