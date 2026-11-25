package jobs

import (
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/ldproxy/xtralink/model"
)

// A setup that fails permanently means none of the Job's work ran, so
// there is nothing for its follow-ups to build on, with or without a
// cleanup in between.
func TestFailedSetup_DeclinesFollowUps(t *testing.T) {
	forBothBackends(t, func(t *testing.T, b Backend) {
		for _, withCleanup := range []bool{false, true} {
			worker, followUpWorker, kind := uniqueType("worker"), uniqueType("follow-up"), uniqueType("job")
			job := NewJobFromConfiguration(model.JobConfiguration{
				Kind: kind, Priority: 1, Partials: []string{worker}, Setup: true, Cleanup: withCleanup,
				FollowUps: []model.JobConfiguration{{Kind: kind + "-next", Priority: 1, Partials: []string{followUpWorker}}},
			})
			if rb, ok := b.(*RedisBackend); ok {
				cleanupJob(t, rb, job.Id)
			}
			if err := b.PushJob(&job); err != nil {
				t.Fatalf("PushJob: %v", err)
			}

			setup, _ := b.Take(SetupKind(kind), "a")
			_ = b.Error(setup.Id, "setup broke", false)
			if withCleanup {
				cleanup, _ := b.Take(CleanupKind(kind), "a")
				_ = b.Done(cleanup.Id)
			}

			if queued, _ := b.GetOpen(followUpWorker); len(queued) > 0 {
				t.Errorf("follow-up queued after a failed setup (cleanup=%v)", withCleanup)
			}
		}
	})
}

// A finished Job whose cleanup is still pending is not settled yet:
// removing it then would lose the cleanup's outcome and its follow-ups.
func TestIsSettled_WaitsForTheCleanup(t *testing.T) {
	forBothBackends(t, func(t *testing.T, b Backend) {
		worker, kind := uniqueType("worker"), uniqueType("job")
		job := NewJobFromConfiguration(model.JobConfiguration{Kind: kind, Priority: 1, Partials: []string{worker}, Cleanup: true})
		if rb, ok := b.(*RedisBackend); ok {
			cleanupJob(t, rb, job.Id)
		}
		if err := b.PushJob(&job); err != nil {
			t.Fatalf("PushJob: %v", err)
		}
		part, _ := b.Take(worker, "a")
		_ = b.StartJob(job.Id)
		_ = b.UpdatePartialJob(part.Id, 1)
		_ = b.Done(part.Id)

		finished, _ := b.GetJob(job.Id)
		if finished.FinishedAt <= 0 || IsSettled(finished) {
			t.Fatalf("want finished but not settled before the cleanup ran, got finishedAt=%d settled=%v", finished.FinishedAt, IsSettled(finished))
		}

		cleanup, _ := b.Take(CleanupKind(kind), "a")
		_ = b.Done(cleanup.Id)
		if settled, _ := b.GetJob(job.Id); !IsSettled(settled) {
			t.Error("want settled once the cleanup finished")
		}
	})
}

// StartJob derives the status it writes from the Job it read, so a Cancel
// in between must make it retry rather than be overwritten with RUNNING.
func TestRedisBackend_StartJobKeepsAConcurrentDismissal(t *testing.T) {
	b := requireRedis(t)
	for range 30 {
		worker := uniqueType("worker")
		job := NewJobFromConfiguration(model.JobConfiguration{Kind: uniqueType("job"), Priority: 1, Partials: []string{worker, worker}})
		cleanupJob(t, b, job.Id)
		if err := b.PushJob(&job); err != nil {
			t.Fatalf("PushJob: %v", err)
		}
		part, _ := b.Take(worker, "a")
		t.Cleanup(func() { _ = b.Done(part.Id) })

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _ = b.StartJob(job.Id) }()
		go func() { defer wg.Done(); _, _ = b.Cancel(job.Id) }()
		wg.Wait()

		if got, _ := b.GetJob(job.Id); got.Status != model.StatusDISMISSED {
			t.Fatalf("status = %s after a concurrent StartJob and Cancel, want DISMISSED", got.Status)
		}
	}
}

func TestRedisBackend_CloseReleasesTheClient(t *testing.T) {
	b := requireRedis(t)
	job := NewJob(uuid.NewString(), uniqueType("job"), 1, "", nil)
	cleanupJob(t, requireRedis(t), job.Id)
	if err := b.PushJobListen(job, &recordingJobListener{}); err != nil {
		t.Fatalf("PushJobListen: %v", err)
	}

	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(b.watchedJobs()) != 0 {
		t.Error("listeners survived Close")
	}
	if _, err := b.GetJob(job.Id); err == nil {
		t.Error("expected the closed client to refuse commands")
	}
}
