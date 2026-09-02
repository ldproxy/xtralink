package jobs

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ldproxy/xtralink/model"
)

// runRunner starts r.Run in a goroutine, waits for waitFor to return true (or
// timeout), then cancels and waits for Run to actually return.
func runRunnerUntil(t *testing.T, r *Runner, timeout time.Duration, waitFor func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if waitFor() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
}

func TestRunner_DispatchesToRegisteredProcessor(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("dispatch")

	partialJob := NewPartialJob(uuid.NewString(), jobType, 1000, "")
	cleanupPartialJob(t, b, partialJob.Id)
	if err := b.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	var processed int32
	r := NewRunner(b, "test")
	r.PollInterval = 20 * time.Millisecond
	r.Register(&JobProcessor{Kind: jobType, Priority: 1000, Process: func(*model.PartialJob, *model.Job, Backend) model.JobResult {
		atomic.AddInt32(&processed, 1)
		return model.Success()
	}})

	runRunnerUntil(t, r, 2*time.Second, func() bool { return atomic.LoadInt32(&processed) > 0 })

	if atomic.LoadInt32(&processed) != 1 {
		t.Errorf("expected the partial job to be processed exactly once, got %d", processed)
	}
	if got, _ := b.getPartialJob(context.Background(), partialJob.Id); got != nil {
		t.Error("expected partial job to be deleted (Done()) after successful processing")
	}
}

func TestRunner_TriesHigherPriorityProcessorFirst(t *testing.T) {
	b := requireRedis(t)
	base := uniqueType("prio")
	lowType := base + "-low"
	highType := base + "-high"

	lowJob := NewPartialJob(uuid.NewString(), lowType, 1000, "")
	highJob := NewPartialJob(uuid.NewString(), highType, 1000, "")
	cleanupPartialJob(t, b, lowJob.Id)
	cleanupPartialJob(t, b, highJob.Id)
	if err := b.PushPartialJob(lowJob, false); err != nil {
		t.Fatalf("PushPartialJob(low): %v", err)
	}
	if err := b.PushPartialJob(highJob, false); err != nil {
		t.Fatalf("PushPartialJob(high): %v", err)
	}

	var mu sync.Mutex
	var order []string

	record := func(name string) func(*model.PartialJob, *model.Job, Backend) model.JobResult {
		return func(*model.PartialJob, *model.Job, Backend) model.JobResult {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			time.Sleep(150 * time.Millisecond) // keep the single concurrency slot busy
			return model.Success()
		}
	}

	r := NewRunner(b, "test")
	r.Concurrency = 1 // force strictly one-at-a-time so dispatch order is observable
	r.PollInterval = 10 * time.Millisecond
	r.Register(&JobProcessor{Kind: lowType, Priority: 100, Process: record("low")})
	r.Register(&JobProcessor{Kind: highType, Priority: 900, Process: record("high")})

	runRunnerUntil(t, r, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 2
	})

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "high" || order[1] != "low" {
		t.Errorf("expected [high low] dispatch order, got %v", order)
	}
}

func TestRunner_ConcurrencyLimitsParallelExecution(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("conc-limit")

	const jobCount = 6
	const concurrency = 2

	for i := 0; i < jobCount; i++ {
		partialJob := NewPartialJob(uuid.NewString(), jobType, 1000, "")
		cleanupPartialJob(t, b, partialJob.Id)
		if err := b.PushPartialJob(partialJob, false); err != nil {
			t.Fatalf("PushPartialJob: %v", err)
		}
	}

	var current, maxObserved int32
	var completed int32

	r := NewRunner(b, "test")
	r.Concurrency = concurrency
	r.PollInterval = 10 * time.Millisecond
	r.Register(&JobProcessor{Kind: jobType, Priority: 1000, Process: func(*model.PartialJob, *model.Job, Backend) model.JobResult {
		n := atomic.AddInt32(&current, 1)
		for {
			max := atomic.LoadInt32(&maxObserved)
			if n <= max || atomic.CompareAndSwapInt32(&maxObserved, max, n) {
				break
			}
		}
		time.Sleep(80 * time.Millisecond)
		atomic.AddInt32(&current, -1)
		atomic.AddInt32(&completed, 1)
		return model.Success()
	}})

	runRunnerUntil(t, r, 5*time.Second, func() bool { return atomic.LoadInt32(&completed) == jobCount })

	if got := atomic.LoadInt32(&completed); got != jobCount {
		t.Fatalf("expected all %d partial jobs to complete, got %d", jobCount, got)
	}
	if max := atomic.LoadInt32(&maxObserved); max > concurrency {
		t.Errorf("observed %d partial jobs running at once, want at most %d", max, concurrency)
	}
}

func TestRunner_StartsJobOnFirstNonSetupPartialJob(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("start-job")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	cleanupJob(t, b, job.Id)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	partialJob := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, job.Id)
	cleanupPartialJob(t, b, partialJob.Id)
	if err := b.InitJob(job.Id, 1, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	if err := b.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	var processed int32
	r := NewRunner(b, "test")
	r.PollInterval = 10 * time.Millisecond
	r.Register(&JobProcessor{Kind: jobType + ":worker", Priority: 1000, Process: func(p *model.PartialJob, j *model.Job, backend Backend) model.JobResult {
		_ = backend.UpdatePartialJob(p.Id, 1)
		atomic.AddInt32(&processed, 1)
		return model.Success()
	}})

	runRunnerUntil(t, r, 2*time.Second, func() bool { return atomic.LoadInt32(&processed) > 0 })

	got, err := b.GetJob(job.Id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if !got.IsStarted() {
		t.Error("expected Job to be started once its first non-setup partial job was taken")
	}
}

func TestRunner_OnHoldRetriesAfterInterval(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("onhold")

	partialJob := NewPartialJob(uuid.NewString(), jobType, 1000, "")
	cleanupPartialJob(t, b, partialJob.Id)
	if err := b.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	var attempts int32
	r := NewRunner(b, "test")
	r.OnHoldRetryInterval = 100 * time.Millisecond
	r.PollInterval = 10 * time.Millisecond
	r.Register(&JobProcessor{Kind: jobType, Priority: 1000, Process: func(*model.PartialJob, *model.Job, Backend) model.JobResult {
		if atomic.AddInt32(&attempts, 1) == 1 {
			return model.OnHold()
		}
		return model.Success()
	}})

	runRunnerUntil(t, r, 3*time.Second, func() bool { return atomic.LoadInt32(&attempts) >= 2 })

	if got := atomic.LoadInt32(&attempts); got != 2 {
		t.Errorf("expected exactly 2 attempts (initial OnHold + retry), got %d", got)
	}
	if got, _ := b.getPartialJob(context.Background(), partialJob.Id); got != nil {
		t.Error("expected partial job to be deleted (Done()) once the retried attempt succeeded")
	}
}

// The tests below drive MemoryBackend rather than Redis: they exercise
// Runner logic (panic containment, housekeeping sweeps) that is identical
// for either backend, so there is no reason to make them skip when no Redis
// is around.

func TestRunner_RecoversFromPanickingProcessor(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("panic")

	for i := 0; i < 2; i++ {
		if err := b.PushPartialJob(NewPartialJob(uuid.NewString(), jobType, 1000, ""), false); err != nil {
			t.Fatalf("PushPartialJob: %v", err)
		}
	}

	var attempts int32
	r := NewRunner(b, "test")
	r.Concurrency = 1 // one at a time, so exactly the first attempt panics
	r.PollInterval = 10 * time.Millisecond
	r.Register(&JobProcessor{Kind: jobType, Priority: 1000, Process: func(*model.PartialJob, *model.Job, Backend) model.JobResult {
		if atomic.AddInt32(&attempts, 1) == 1 {
			panic("processor exploded")
		}
		return model.Success()
	}})

	runRunnerUntil(t, r, 2*time.Second, func() bool { return atomic.LoadInt32(&attempts) >= 2 })

	if got := atomic.LoadInt32(&attempts); got != 2 {
		t.Fatalf("expected the Runner to survive the panic and attempt both partial jobs, got %d attempts", got)
	}

	failed, err := b.GetFailed()
	if err != nil {
		t.Fatalf("GetFailed: %v", err)
	}
	if len(failed) != 1 {
		t.Fatalf("expected exactly 1 failed partial job, got %d", len(failed))
	}
	if !strings.Contains(strings.Join(failed[0].Errors, " "), "panic") {
		t.Errorf("expected the panic to be recorded as the failure reason, got %v", failed[0].Errors)
	}
}

func TestRunner_ReclaimsOrphanedPartialJob(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("orphan")

	if err := b.PushPartialJob(NewPartialJob(uuid.NewString(), jobType, 1000, ""), false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	// Taken and then never touched again: what a crashed executor leaves
	// behind.
	taken, err := b.Take(jobType, "dead-executor")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}

	var processed int32
	r := NewRunner(b, "test")
	r.PollInterval = 10 * time.Millisecond
	r.HousekeepingInterval = 10 * time.Millisecond
	r.Register(&JobProcessor{Kind: jobType, Priority: 1000, OrphanTimeout: 30 * time.Millisecond,
		Process: func(*model.PartialJob, *model.Job, Backend) model.JobResult {
			atomic.AddInt32(&processed, 1)
			return model.Success()
		}})

	runRunnerUntil(t, r, 2*time.Second, func() bool { return atomic.LoadInt32(&processed) > 0 })

	if got := atomic.LoadInt32(&processed); got != 1 {
		t.Errorf("expected the orphaned partial job to be reclaimed and processed once, got %d", got)
	}
}

func TestRunner_DoesNotReclaimItsOwnInFlightPartialJob(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("inflight")

	if err := b.PushPartialJob(NewPartialJob(uuid.NewString(), jobType, 1000, ""), false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	var started, finished int32
	r := NewRunner(b, "test")
	r.PollInterval = 10 * time.Millisecond
	r.HousekeepingInterval = 10 * time.Millisecond
	// Deliberately far shorter than the processor runs for: without the
	// in-flight guard every sweep would re-queue work that is still running.
	r.Register(&JobProcessor{Kind: jobType, Priority: 1000, OrphanTimeout: 20 * time.Millisecond,
		Process: func(*model.PartialJob, *model.Job, Backend) model.JobResult {
			atomic.AddInt32(&started, 1)
			time.Sleep(200 * time.Millisecond)
			atomic.AddInt32(&finished, 1)
			return model.Success()
		}})

	runRunnerUntil(t, r, 3*time.Second, func() bool { return atomic.LoadInt32(&finished) > 0 })

	if got := atomic.LoadInt32(&started); got != 1 {
		t.Errorf("expected the in-flight partial job to be dispatched exactly once, got %d", got)
	}
}

func TestRunner_RemovesLongFinishedJob(t *testing.T) {
	b := NewMemoryBackend()
	job, r := finishingJobRunner(t, b, nil)
	r.JobRetention = time.Millisecond

	runRunnerUntil(t, r, 2*time.Second, func() bool {
		got, _ := b.GetJob(job.Id)
		return got == nil
	})

	if got, _ := b.GetJob(job.Id); got != nil {
		t.Errorf("expected the finished Job to be removed once JobRetention passed, got %+v", got)
	}
}

func TestRunner_KeepsFinishedJobWhenRetentionIsNegative(t *testing.T) {
	b := NewMemoryBackend()
	job, r := finishingJobRunner(t, b, nil)
	r.JobRetention = -1

	assertJobSurvivesSweeps(t, b, r, job.Id)
}

// finishingJobRunner pushes a Job with a single worker PartialJob and
// returns a Runner whose processor completes it, so the cleanup sweep has a
// finished Job to act on. Housekeeping runs fast; the caller sets
// JobRetention. ttlSeconds, when non-nil, is the Job's own TTL override.
func finishingJobRunner(t *testing.T, b *MemoryBackend, ttlSeconds *int) (*model.Job, *Runner) {
	t.Helper()

	jobType := uniqueType("retention")
	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.TtlSeconds = ttlSeconds
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	if err := b.InitJob(job.Id, 1, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	if err := b.PushPartialJob(NewPartialJob(uuid.NewString(), jobType+":worker", 1000, job.Id), false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	r := NewRunner(b, "test")
	r.PollInterval = 10 * time.Millisecond
	r.HousekeepingInterval = 10 * time.Millisecond
	r.Register(&JobProcessor{Kind: jobType + ":worker", Priority: 1000,
		Process: func(p *model.PartialJob, _ *model.Job, backend Backend) model.JobResult {
			if err := backend.UpdatePartialJob(p.Id, 1); err != nil {
				return model.Error(err.Error())
			}
			return model.Success()
		}})

	return job, r
}

func TestRunner_JobTtlShortensTheDefaultRetention(t *testing.T) {
	b := NewMemoryBackend()
	ttl := 1
	job, r := finishingJobRunner(t, b, &ttl)
	r.JobRetention = time.Hour

	runRunnerUntil(t, r, 3*time.Second, func() bool {
		got, _ := b.GetJob(job.Id)
		return got == nil
	})

	if got, _ := b.GetJob(job.Id); got != nil {
		t.Errorf("expected the Job's own TTL to retire it well before the 1h default, got %+v", got)
	}
}

func TestRunner_JobTtlExtendsPastTheDefaultRetention(t *testing.T) {
	b := NewMemoryBackend()
	ttl := 3600
	job, r := finishingJobRunner(t, b, &ttl)
	r.JobRetention = time.Millisecond

	assertJobSurvivesSweeps(t, b, r, job.Id)
}

func TestRunner_JobTtlAppliesWhenDefaultKeepsIndefinitely(t *testing.T) {
	b := NewMemoryBackend()
	ttl := 1
	job, r := finishingJobRunner(t, b, &ttl)
	r.JobRetention = -1

	runRunnerUntil(t, r, 3*time.Second, func() bool {
		got, _ := b.GetJob(job.Id)
		return got == nil
	})

	if got, _ := b.GetJob(job.Id); got != nil {
		t.Errorf("expected an explicit TTL to apply even when the default keeps Jobs forever, got %+v", got)
	}
}

func TestRunner_ZeroJobTtlRemovesTheJobAtOnce(t *testing.T) {
	b := NewMemoryBackend()
	ttl := 0
	job, r := finishingJobRunner(t, b, &ttl)
	r.JobRetention = time.Hour

	runRunnerUntil(t, r, 2*time.Second, func() bool {
		got, _ := b.GetJob(job.Id)
		return got == nil
	})

	if got, _ := b.GetJob(job.Id); got != nil {
		t.Errorf("expected a zero TTL to retire the Job on the first sweep after it finished, got %+v", got)
	}
}

func TestRunner_NegativeJobTtlKeepsTheJob(t *testing.T) {
	b := NewMemoryBackend()
	ttl := -1
	job, r := finishingJobRunner(t, b, &ttl)
	r.JobRetention = time.Millisecond

	assertJobSurvivesSweeps(t, b, r, job.Id)
}

// assertJobSurvivesSweeps runs r well past the point where jobID finished,
// so plenty of housekeeping sweeps see the Job, and asserts it is still
// there afterwards.
func assertJobSurvivesSweeps(t *testing.T, b *MemoryBackend, r *Runner, jobID string) {
	t.Helper()

	deadline := time.Now().Add(150 * time.Millisecond)
	runRunnerUntil(t, r, 2*time.Second, func() bool {
		got, _ := b.GetJob(jobID)
		return got == nil || (got.FinishedAt > 0 && time.Now().After(deadline))
	})

	got, _ := b.GetJob(jobID)
	if got == nil {
		t.Fatal("expected the finished Job to be kept, but it was removed")
	}
	if got.FinishedAt <= 0 {
		t.Errorf("expected the Job to have finished, got %+v", got)
	}
}
