package jobs

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ldproxy/xtralink/model"
)

// runUntil runs r until waitFor holds or timeout passes, failing the test on
// timeout, and returns once Run has returned.
func runUntil(t *testing.T, r *Runner, timeout time.Duration, what string, waitFor func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = r.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	deadline := time.Now().Add(timeout)
	for !waitFor() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func fastRunner(b Backend) *Runner {
	r := NewRunner(b, uuid.NewString())
	r.PollInterval = 5 * time.Millisecond
	r.HousekeepingInterval = 0
	return r
}

func succeed(*model.PartialJob, *model.Job, Backend) model.JobResult { return model.Success() }

// complete reports the PartialJob's one unit of work, without which its Job
// never reaches current == total and never finishes.
func complete(partialJob *model.PartialJob, _ *model.Job, backend Backend) model.JobResult {
	_ = backend.UpdatePartialJob(partialJob.Id, 1)
	return model.Success()
}

// Through a Runner, which starts a Job on its first non-setup PartialJob:
// the cleanup of a Job whose setup failed must not start it, or the Job
// counts as having run and its follow-ups are pushed after all.
func TestRunner_FailedSetupDeclinesFollowUps(t *testing.T) {
	forBothBackends(t, func(t *testing.T, b Backend) {
		kind, worker, followUpWorker := uniqueType("job"), uniqueType("worker"), uniqueType("follow-up")
		job := NewJobFromConfiguration(model.JobConfiguration{
			Kind: kind, Priority: 1, Partials: []string{worker}, Setup: true, Cleanup: true,
			FollowUps: []model.JobConfiguration{{Kind: kind + "-next", Priority: 1, Partials: []string{followUpWorker}}},
		})

		var followUpRuns atomic.Int32
		r := fastRunner(b)
		r.Register(&JobProcessor{Kind: SetupKind(kind), Priority: 1, Process: func(*model.PartialJob, *model.Job, Backend) model.JobResult {
			return model.Error("setup broke")
		}})
		r.Register(&JobProcessor{Kind: worker, Priority: 1, Process: complete})
		r.Register(&JobProcessor{Kind: CleanupKind(kind), Priority: 1, Process: succeed})
		r.Register(&JobProcessor{Kind: followUpWorker, Priority: 1, Process: func(*model.PartialJob, *model.Job, Backend) model.JobResult {
			followUpRuns.Add(1)
			return model.Success()
		}})

		if err := b.PushJob(&job); err != nil {
			t.Fatalf("PushJob: %v", err)
		}
		runUntil(t, r, 3*time.Second, "the job to settle", func() bool {
			got, _ := b.GetJob(job.Id)
			return got != nil && IsSettled(got)
		})

		got, _ := b.GetJob(job.Id)
		if got.Status != model.StatusFAILED || got.IsStarted() {
			t.Errorf("job status=%s started=%v, want FAILED and never started", got.Status, got.IsStarted())
		}
		if queued, _ := b.GetOpen(followUpWorker); len(queued) > 0 || followUpRuns.Load() > 0 {
			t.Errorf("follow-up pushed after a failed setup: queued=%d ran=%d", len(queued), followUpRuns.Load())
		}
	})
}

// The retention sweep removes a Job only once it is settled: with its
// cleanup still running, even a zero retention has to keep it.
func TestRunner_RetentionWaitsForTheCleanup(t *testing.T) {
	forBothBackends(t, func(t *testing.T, b Backend) {
		kind, worker := uniqueType("job"), uniqueType("worker")
		zero := 0
		job := NewJobFromConfiguration(model.JobConfiguration{Kind: kind, Priority: 1, Partials: []string{worker}, Cleanup: true, TtlSeconds: &zero})

		cleanupStarted := make(chan struct{})
		release := make(chan struct{})
		r := fastRunner(b)
		r.HousekeepingInterval = 10 * time.Millisecond
		r.Register(&JobProcessor{Kind: worker, Priority: 1, Process: complete})
		r.Register(&JobProcessor{Kind: CleanupKind(kind), Priority: 1, Process: func(*model.PartialJob, *model.Job, Backend) model.JobResult {
			close(cleanupStarted)
			<-release
			return model.Success()
		}})

		if err := b.PushJob(&job); err != nil {
			t.Fatalf("PushJob: %v", err)
		}
		var keptWhileCleanupRan, released bool
		runUntil(t, r, 3*time.Second, "the job to be removed", func() bool {
			select {
			case <-cleanupStarted:
				if !released {
					time.Sleep(60 * time.Millisecond) // several sweeps
					got, _ := b.GetJob(job.Id)
					keptWhileCleanupRan = got != nil
					close(release)
					released = true
				}
			default:
			}
			got, _ := b.GetJob(job.Id)
			return released && got == nil
		})

		if !keptWhileCleanupRan {
			t.Error("job removed while its cleanup was still running")
		}
	})
}

// Two executors taking from one queue at once must never get the same
// PartialJob, and between them must get every one.
func TestTake_ConcurrentExecutorsClaimEachPartialJobOnce(t *testing.T) {
	forBothBackends(t, func(t *testing.T, b Backend) {
		kind := uniqueType("contended")
		const count = 40
		for range count {
			if err := b.PushPartialJob(NewPartialJob(uuid.NewString(), kind, 1, ""), false); err != nil {
				t.Fatalf("PushPartialJob: %v", err)
			}
		}

		var mu sync.Mutex
		claims := map[string]int{}
		var wg sync.WaitGroup
		for executor := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					pj, err := b.Take(kind, uuid.NewString()[:4]+string(rune('a'+executor)))
					if err != nil {
						t.Errorf("Take: %v", err)
						return
					}
					if pj == nil {
						return
					}
					mu.Lock()
					claims[pj.Id]++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()

		if len(claims) != count {
			t.Errorf("claimed %d distinct partial jobs, want %d", len(claims), count)
		}
		for id, n := range claims {
			if n > 1 {
				t.Errorf("partial job %s claimed %d times", id, n)
			}
		}
	})
}

// Two orphan sweeps acting on the same snapshot, with a take in between:
// the second re-queue belongs to a take that is over and must not queue the
// PartialJob a second time.
func TestUntake_IgnoresASnapshotOfAnEarlierTake(t *testing.T) {
	forBothBackends(t, func(t *testing.T, b Backend) {
		kind := uniqueType("overlap")
		if err := b.PushPartialJob(NewPartialJob(uuid.NewString(), kind, 1, ""), false); err != nil {
			t.Fatalf("PushPartialJob: %v", err)
		}
		first, _ := b.Take(kind, "dead")
		snapshot := *first

		_ = b.PushPartialJob(&snapshot, true) // first sweep
		second, _ := b.Take(kind, "live")
		_ = b.PushPartialJob(&snapshot, true) // second sweep, same snapshot

		if third, _ := b.Take(kind, "other"); third != nil {
			t.Errorf("partial job handed out again while %s runs it", second.Executor)
		}
		if taken, _ := b.GetTaken(); len(taken) != 1 || taken[0].Executor != "live" {
			t.Errorf("want the live take to stay, got %v", taken)
		}
	})
}

// The Runner hands every cancellation it hears to OnCancel, which the FFI
// wires to the host's CancelListener.
func TestRunner_DeliversCancellationsToOnCancel(t *testing.T) {
	forBothBackends(t, func(t *testing.T, b Backend) {
		probe := uniqueType("probe")
		ran := make(chan struct{})
		cancelled := make(chan string, 1)
		r := fastRunner(b)
		r.OnCancel = func(jobID string) { cancelled <- jobID }
		r.Register(&JobProcessor{Kind: probe, Priority: 1, Process: func(*model.PartialJob, *model.Job, Backend) model.JobResult {
			close(ran)
			return model.Success()
		}})

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = r.Run(ctx); close(done) }()
		defer func() { cancel(); <-done }()

		// Run subscribes before it dispatches, so a processed probe proves
		// the subscription is in place.
		if err := b.PushPartialJob(NewPartialJob(uuid.NewString(), probe, 1, ""), false); err != nil {
			t.Fatalf("PushPartialJob: %v", err)
		}
		select {
		case <-ran:
		case <-time.After(2 * time.Second):
			t.Fatal("runner never dispatched")
		}

		worker := uniqueType("worker")
		job := NewJobFromConfiguration(model.JobConfiguration{Kind: uniqueType("job"), Priority: 1, Partials: []string{worker}})
		if err := b.PushJob(&job); err != nil {
			t.Fatalf("PushJob: %v", err)
		}
		if ok, err := b.Cancel(job.Id); !ok || err != nil {
			t.Fatalf("Cancel = %v, %v", ok, err)
		}

		select {
		case got := <-cancelled:
			if got != job.Id {
				t.Errorf("OnCancel got %s, want %s", got, job.Id)
			}
		case <-time.After(2 * time.Second):
			t.Error("OnCancel was never called")
		}
	})
}

// A PartialJob can be taken after its Job is gone, e.g. a PartialJob of a
// sequence that never became ready and whose failed Job expired. It fails
// for good instead of reaching a processor that expects its Job.
func TestRunner_FailsAPartialJobWhoseJobIsGone(t *testing.T) {
	forBothBackends(t, func(t *testing.T, b Backend) {
		kind, goneJobID := uniqueType("orphan"), uuid.NewString()
		partialJob := NewPartialJob(uuid.NewString(), kind, 1, goneJobID)
		if redis, ok := b.(*RedisBackend); ok {
			cleanupPartialJob(t, redis, partialJob.Id)
		}
		if err := b.PushPartialJob(partialJob, false); err != nil {
			t.Fatalf("PushPartialJob: %v", err)
		}

		var processed atomic.Int32
		r := fastRunner(b)
		r.Register(&JobProcessor{Kind: kind, Priority: 1, Process: func(*model.PartialJob, *model.Job, Backend) model.JobResult {
			processed.Add(1)
			return model.Success()
		}})

		var failed *model.PartialJob
		runUntil(t, r, 3*time.Second, "the partial job to fail", func() bool {
			failed = failedPartialJob(b, partialJob.Id)
			return failed != nil
		})

		if processed.Load() != 0 {
			t.Errorf("processor ran %d times for a partial job without its job", processed.Load())
		}
		want := "partial job " + partialJob.Id + " has no job (partOf=\"" + goneJobID + "\")"
		if len(failed.Errors) != 1 || failed.Errors[0] != want {
			t.Errorf("errors = %q, want [%q]", failed.Errors, want)
		}
	})
}

func failedPartialJob(b Backend, id string) *model.PartialJob {
	failed, _ := b.GetFailed()
	for _, partialJob := range failed {
		if partialJob.Id == id {
			return partialJob
		}
	}
	return nil
}
