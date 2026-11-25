package jobs

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ldproxy/xtralink/model"
)

// assertPushWakesRunner pushes work while the Runner is idle in a poll sleep
// far longer than the test allows, so only the push event can wake it.
func assertPushWakesRunner(t *testing.T, b Backend, cleanup func(id string)) {
	t.Helper()
	jobType := uniqueType("wake")

	var processed int32
	r := NewRunner(b, "test")
	r.PollInterval = time.Minute
	r.HousekeepingInterval = 0
	r.Register(&JobProcessor{Kind: jobType, Priority: 1000, Process: func(*model.PartialJob, *model.Job, Backend) model.JobResult {
		atomic.AddInt32(&processed, 1)
		return model.Success()
	}})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() { cancel(); <-done }()

	time.Sleep(100 * time.Millisecond) // let the Runner find nothing and go to sleep
	partialJob := NewPartialJob(uuid.NewString(), jobType, 1000, "")
	cleanup(partialJob.Id)
	if err := b.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&processed) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if atomic.LoadInt32(&processed) == 0 {
		t.Error("the push did not wake the Runner before its one-minute poll")
	}
}

func TestMemoryBackend_PushWakesRunner(t *testing.T) {
	assertPushWakesRunner(t, NewMemoryBackend(), func(string) {})
}

func TestRedisBackend_PushWakesRunner(t *testing.T) {
	b := requireRedis(t)
	assertPushWakesRunner(t, b, func(id string) { cleanupPartialJob(t, b, id) })
}

// Two RedisBackends stand in for two replicas sharing one queue: a Cancel
// on one has to reach a listener on the other.
func TestRedisBackend_CancelReachesOtherProcess(t *testing.T) {
	canceller := requireRedis(t)
	listener := requireRedis(t)

	cancelled := make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := listener.Listen(ctx, Notifications{OnCancel: func(jobID string) { cancelled <- jobID }}); err != nil {
		t.Fatalf("Listen: %v", err)
	}

	job := NewJob(uuid.NewString(), uniqueType("cancel-remote"), 1000, "", nil)
	cleanupJob(t, canceller, job.Id)
	if err := canceller.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	if ok, err := canceller.Cancel(job.Id); err != nil || !ok {
		t.Fatalf("Cancel = %v, %v", ok, err)
	}

	select {
	case got := <-cancelled:
		if got != job.Id {
			t.Errorf("notified %s, want %s", got, job.Id)
		}
	case <-time.After(2 * time.Second):
		t.Error("the other process was never notified of the cancel")
	}
}

func TestMemoryBackend_ListenStopsWithContext(t *testing.T) {
	b := NewMemoryBackend()
	var notified int32
	ctx, cancel := context.WithCancel(context.Background())
	if err := b.Listen(ctx, Notifications{OnWork: func() { atomic.AddInt32(&notified, 1) }}); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	cancel()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		remaining := len(b.subscribers)
		b.mu.Unlock()
		if remaining == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Error("subscriber still registered after its context was cancelled")
}
