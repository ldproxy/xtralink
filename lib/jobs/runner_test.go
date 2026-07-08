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
	process  func(job *Job, jobSet *JobSet, backend Backend) JobResult
}

func (p *funcProcessor) JobType() string { return p.jobType }
func (p *funcProcessor) Priority() int   { return p.priority }
func (p *funcProcessor) Process(job *Job, jobSet *JobSet, backend Backend) JobResult {
	return p.process(job, jobSet, backend)
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

	job := NewJob(uuid.NewString(), jobType, 1000, "")
	cleanupJob(t, b, job.ID)
	if err := b.PushJob(job, false); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	var processed int32
	r := NewRunner(b, "test")
	r.PollInterval = 20 * time.Millisecond
	r.Register(&funcProcessor{jobType: jobType, priority: 1000, process: func(*Job, *JobSet, Backend) JobResult {
		atomic.AddInt32(&processed, 1)
		return Success()
	}})

	runRunnerUntil(t, r, 2*time.Second, func() bool { return atomic.LoadInt32(&processed) > 0 })

	if atomic.LoadInt32(&processed) != 1 {
		t.Errorf("expected the job to be processed exactly once, got %d", processed)
	}
	if got, _ := b.getJob(context.Background(), job.ID); got != nil {
		t.Error("expected job to be deleted (Done()) after successful processing")
	}
}

func TestRunner_TriesHigherPriorityProcessorFirst(t *testing.T) {
	b := requireRedis(t)
	base := uniqueType("prio")
	lowType := base + "-low"
	highType := base + "-high"

	lowJob := NewJob(uuid.NewString(), lowType, 1000, "")
	highJob := NewJob(uuid.NewString(), highType, 1000, "")
	cleanupJob(t, b, lowJob.ID)
	cleanupJob(t, b, highJob.ID)
	if err := b.PushJob(lowJob, false); err != nil {
		t.Fatalf("PushJob(low): %v", err)
	}
	if err := b.PushJob(highJob, false); err != nil {
		t.Fatalf("PushJob(high): %v", err)
	}

	var mu sync.Mutex
	var order []string

	record := func(name string) func(*Job, *JobSet, Backend) JobResult {
		return func(*Job, *JobSet, Backend) JobResult {
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
		job := NewJob(uuid.NewString(), jobType, 1000, "")
		cleanupJob(t, b, job.ID)
		if err := b.PushJob(job, false); err != nil {
			t.Fatalf("PushJob: %v", err)
		}
	}

	var current, maxObserved int32
	var completed int32

	r := NewRunner(b, "test")
	r.Concurrency = concurrency
	r.PollInterval = 10 * time.Millisecond
	r.Register(&funcProcessor{jobType: jobType, priority: 1000, process: func(*Job, *JobSet, Backend) JobResult {
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
		t.Fatalf("expected all %d jobs to complete, got %d", jobCount, got)
	}
	if max := atomic.LoadInt32(&maxObserved); max > concurrency {
		t.Errorf("observed %d jobs running at once, want at most %d", max, concurrency)
	}
}

func TestRunner_StartsJobSetOnFirstNonSetupJob(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("start-jobset")

	js := NewJobSet(uuid.NewString(), jobType, 1000, "", "", nil)
	cleanupJobSet(t, b, js.ID)
	if err := b.PushJobSet(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}
	job := NewJob(uuid.NewString(), jobType+":worker", 1000, js.ID)
	job.Total = 1
	cleanupJob(t, b, job.ID)
	if err := b.InitJobSet(js.ID, 1, nil); err != nil {
		t.Fatalf("InitJobSet: %v", err)
	}
	if err := b.PushJob(job, false); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	var processed int32
	r := NewRunner(b, "test")
	r.PollInterval = 10 * time.Millisecond
	r.Register(&funcProcessor{jobType: jobType + ":worker", priority: 1000, process: func(j *Job, jobSet *JobSet, backend Backend) JobResult {
		_ = backend.UpdateJob(j.ID, 1)
		_ = backend.UpdateJobSet(jobSet.ID, 1, nil)
		atomic.AddInt32(&processed, 1)
		return Success()
	}})

	runRunnerUntil(t, r, 2*time.Second, func() bool { return atomic.LoadInt32(&processed) > 0 })

	got, err := b.GetSet(js.ID)
	if err != nil {
		t.Fatalf("GetSet: %v", err)
	}
	if !got.IsStarted() {
		t.Error("expected JobSet to be started once its first non-setup job was taken")
	}
}

func TestRunner_OnHoldRetriesAfterInterval(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("onhold")

	job := NewJob(uuid.NewString(), jobType, 1000, "")
	cleanupJob(t, b, job.ID)
	if err := b.PushJob(job, false); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	var attempts int32
	r := NewRunner(b, "test")
	r.OnHoldRetryInterval = 100 * time.Millisecond
	r.PollInterval = 10 * time.Millisecond
	r.Register(&funcProcessor{jobType: jobType, priority: 1000, process: func(*Job, *JobSet, Backend) JobResult {
		if atomic.AddInt32(&attempts, 1) == 1 {
			return OnHold()
		}
		return Success()
	}})

	runRunnerUntil(t, r, 3*time.Second, func() bool { return atomic.LoadInt32(&attempts) >= 2 })

	if got := atomic.LoadInt32(&attempts); got != 2 {
		t.Errorf("expected exactly 2 attempts (initial OnHold + retry), got %d", got)
	}
	if got, _ := b.getJob(context.Background(), job.ID); got != nil {
		t.Error("expected job to be deleted (Done()) once the retried attempt succeeded")
	}
}
