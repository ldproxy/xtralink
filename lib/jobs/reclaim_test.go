package jobs

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ldproxy/xtralink/model"
)

// forBothBackends runs check against MemoryBackend and, if reachable, Redis.
func forBothBackends(t *testing.T, check func(t *testing.T, b Backend)) {
	t.Run("memory", func(t *testing.T) { check(t, NewMemoryBackend()) })
	t.Run("redis", func(t *testing.T) { check(t, requireRedis(t)) })
}

// Two re-queues of one taken PartialJob (an orphan sweep and a host
// re-push, say) must queue it once, or two executors run it at once.
func TestUntake_QueuesATakenPartialJobOnlyOnce(t *testing.T) {
	forBothBackends(t, func(t *testing.T, b Backend) {
		kind := uniqueType("untake-twice")
		if err := b.PushPartialJob(NewPartialJob(uuid.NewString(), kind, 1, ""), false); err != nil {
			t.Fatalf("PushPartialJob: %v", err)
		}
		taken, _ := b.Take(kind, "a")

		_ = b.PushPartialJob(taken, true)
		_ = b.PushPartialJob(taken, true)

		first, _ := b.Take(kind, "a")
		second, _ := b.Take(kind, "b")
		if first == nil || second != nil {
			t.Errorf("expected exactly one take after two re-queues, got %v and %v", first, second)
		}
		if first != nil {
			_ = b.Done(first.Id)
		}
	})
}

// An orphan sweep acts on a GetTaken snapshot; if the PartialJob finishes
// in between, the re-queue must not bring it back.
func TestUntake_DoesNotResurrectAFinishedPartialJob(t *testing.T) {
	forBothBackends(t, func(t *testing.T, b Backend) {
		kind := uniqueType("untake-finished")
		if err := b.PushPartialJob(NewPartialJob(uuid.NewString(), kind, 1, ""), false); err != nil {
			t.Fatalf("PushPartialJob: %v", err)
		}
		taken, _ := b.Take(kind, "a")
		snapshot := *taken

		_ = b.Done(taken.Id)
		_ = b.PushPartialJob(&snapshot, true)

		if again, _ := b.Take(kind, "b"); again != nil {
			t.Errorf("finished partial job %s is takeable again", again.Id)
		}
	})
}

// Progress reported by an executor that lost its PartialJob to a reclaim
// must not count, or the Job finishes with parts that never ran.
func TestUpdatePartialJob_IgnoresAPartialJobNoLongerTaken(t *testing.T) {
	forBothBackends(t, func(t *testing.T, b Backend) {
		kind := uniqueType("stale-update")
		job := NewJob(uuid.NewString(), uniqueType("job"), 1, "", nil)
		job.Partials = []string{kind, kind}
		if rb, ok := b.(*RedisBackend); ok {
			cleanupJob(t, rb, job.Id)
		}
		if err := b.PushJob(job); err != nil {
			t.Fatalf("PushJob: %v", err)
		}

		reclaimed, _ := b.Take(kind, "a")
		_ = b.StartJob(job.Id)
		_ = b.PushPartialJob(reclaimed, true) // another Runner's orphan sweep

		_ = b.UpdatePartialJob(reclaimed.Id, 1) // the original executor finishing late
		_ = b.Done(reclaimed.Id)

		got, _ := b.GetJob(job.Id)
		if got.Progress.Current != 0 || got.FinishedAt > 0 {
			t.Errorf("stale progress counted: current=%d finishedAt=%d", got.Progress.Current, got.FinishedAt)
		}
		for {
			pj, _ := b.Take(kind, "cleanup")
			if pj == nil {
				break
			}
			_ = b.Done(pj.Id)
		}
	})
}

// A processor that runs longer than the orphan timeout without reporting
// progress must not be reclaimed by another Runner while its own Runner's
// heartbeat keeps it fresh. The sweeping Runner cannot take anything
// itself (Concurrency 0), so the one run is the worker's.
func TestRunner_HeartbeatKeepsALongRunningPartialJobFromBeingReclaimed(t *testing.T) {
	forBothBackends(t, func(t *testing.T, b Backend) {
		kind := uniqueType("heartbeat")

		var runs atomic.Int32
		release := make(chan struct{})
		worker := NewRunner(b, "worker")
		worker.PollInterval = 5 * time.Millisecond
		worker.HeartbeatInterval = 10 * time.Millisecond
		worker.HousekeepingInterval = 0
		worker.Register(&JobProcessor{Kind: kind, Priority: 1, Process: func(*model.PartialJob, *model.Job, Backend) model.JobResult {
			runs.Add(1)
			<-release
			return model.Success()
		}})

		sweeper := NewRunner(b, "sweeper")
		sweeper.Concurrency = 0
		sweeper.HeartbeatInterval = 0
		sweeper.HousekeepingInterval = 10 * time.Millisecond
		sweeper.Register(&JobProcessor{Kind: kind, Priority: 1, OrphanTimeout: 50 * time.Millisecond, Process: func(*model.PartialJob, *model.Job, Backend) model.JobResult {
			t.Error("the sweeper must not run anything")
			return model.Success()
		}})

		ctx, cancel := context.WithCancel(context.Background())
		stopped := make(chan struct{}, 2)
		for _, r := range []*Runner{worker, sweeper} {
			go func(r *Runner) { _ = r.Run(ctx); stopped <- struct{}{} }(r)
		}
		partialJob := NewPartialJob(uuid.NewString(), kind, 1, "")
		if err := b.PushPartialJob(partialJob, false); err != nil {
			t.Fatalf("PushPartialJob: %v", err)
		}

		time.Sleep(300 * time.Millisecond) // six orphan timeouts
		queued, _ := b.GetOpen(kind)
		close(release)
		cancel()
		<-stopped
		<-stopped

		if got := runs.Load(); got != 1 {
			t.Errorf("partial job run %d times by the worker, want 1", got)
		}
		if len(queued) != 0 {
			t.Errorf("partial job was re-queued while the worker ran it")
		}
	})
}
