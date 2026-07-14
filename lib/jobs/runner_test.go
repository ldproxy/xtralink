package jobs

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

// funcProcessor lets tests supply Process() as a plain closure instead of
// declaring a new named type per test.
type funcProcessor struct {
	jobType  string
	priority int
	process  func(partialJob *PartialJob, job *Job, backend Backend) JobResult
}

func (p *funcProcessor) JobType() string { return p.jobType }
func (p *funcProcessor) Priority() int   { return p.priority }
func (p *funcProcessor) Process(partialJob *PartialJob, job *Job, backend Backend) JobResult {
	return p.process(partialJob, job, backend)
}

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
	cleanupPartialJob(t, b, partialJob.ID)
	if err := b.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	var processed int32
	r := NewRunner(b, "test")
	r.PollInterval = 20 * time.Millisecond
	r.Register(&funcProcessor{jobType: jobType, priority: 1000, process: func(*PartialJob, *Job, Backend) JobResult {
		atomic.AddInt32(&processed, 1)
		return Success()
	}})

	runRunnerUntil(t, r, 2*time.Second, func() bool { return atomic.LoadInt32(&processed) > 0 })

	if atomic.LoadInt32(&processed) != 1 {
		t.Errorf("expected the partial job to be processed exactly once, got %d", processed)
	}
	if got, _ := b.getPartialJob(context.Background(), partialJob.ID); got != nil {
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
	cleanupPartialJob(t, b, lowJob.ID)
	cleanupPartialJob(t, b, highJob.ID)
	if err := b.PushPartialJob(lowJob, false); err != nil {
		t.Fatalf("PushPartialJob(low): %v", err)
	}
	if err := b.PushPartialJob(highJob, false); err != nil {
		t.Fatalf("PushPartialJob(high): %v", err)
	}

	var mu sync.Mutex
	var order []string

	record := func(name string) func(*PartialJob, *Job, Backend) JobResult {
		return func(*PartialJob, *Job, Backend) JobResult {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			time.Sleep(150 * time.Millisecond) // keep the single concurrency slot busy
			return Success()
		}
	}

	r := NewRunner(b, "test")
	r.Concurrency = 1 // force strictly one-at-a-time so dispatch order is observable
	r.PollInterval = 10 * time.Millisecond
	r.Register(&funcProcessor{jobType: lowType, priority: 100, process: record("low")})
	r.Register(&funcProcessor{jobType: highType, priority: 900, process: record("high")})

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
		cleanupPartialJob(t, b, partialJob.ID)
		if err := b.PushPartialJob(partialJob, false); err != nil {
			t.Fatalf("PushPartialJob: %v", err)
		}
	}

	var current, maxObserved int32
	var completed int32

	r := NewRunner(b, "test")
	r.Concurrency = concurrency
	r.PollInterval = 10 * time.Millisecond
	r.Register(&funcProcessor{jobType: jobType, priority: 1000, process: func(*PartialJob, *Job, Backend) JobResult {
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
		return Success()
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
	cleanupJob(t, b, job.ID)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	partialJob := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, job.ID)
	partialJob.Total = 1
	cleanupPartialJob(t, b, partialJob.ID)
	if err := b.InitJob(job.ID, 1, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	if err := b.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	var processed int32
	r := NewRunner(b, "test")
	r.PollInterval = 10 * time.Millisecond
	r.Register(&funcProcessor{jobType: jobType + ":worker", priority: 1000, process: func(p *PartialJob, j *Job, backend Backend) JobResult {
		_ = backend.UpdatePartialJob(p.ID, 1)
		_ = backend.UpdateJob(j.ID, 1, nil)
		atomic.AddInt32(&processed, 1)
		return Success()
	}})

	runRunnerUntil(t, r, 2*time.Second, func() bool { return atomic.LoadInt32(&processed) > 0 })

	got, err := b.GetJob(job.ID)
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
	cleanupPartialJob(t, b, partialJob.ID)
	if err := b.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	var attempts int32
	r := NewRunner(b, "test")
	r.OnHoldRetryInterval = 100 * time.Millisecond
	r.PollInterval = 10 * time.Millisecond
	r.Register(&funcProcessor{jobType: jobType, priority: 1000, process: func(*PartialJob, *Job, Backend) JobResult {
		if atomic.AddInt32(&attempts, 1) == 1 {
			return OnHold()
		}
		return Success()
	}})

	runRunnerUntil(t, r, 3*time.Second, func() bool { return atomic.LoadInt32(&attempts) >= 2 })

	if got := atomic.LoadInt32(&attempts); got != 2 {
		t.Errorf("expected exactly 2 attempts (initial OnHold + retry), got %d", got)
	}
	if got, _ := b.getPartialJob(context.Background(), partialJob.ID); got != nil {
		t.Error("expected partial job to be deleted (Done()) once the retried attempt succeeded")
	}
}
