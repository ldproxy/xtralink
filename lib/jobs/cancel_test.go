package jobs

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ldproxy/xtralink/model"
)

// forEachBackend runs one cancellation case against both implementations,
// since Cancel's queue scanning is the part most likely to diverge between
// them.
func forEachBackend(t *testing.T, run func(t *testing.T, b Backend)) {
	t.Helper()

	t.Run("memory", func(t *testing.T) { run(t, NewMemoryBackend()) })
	t.Run("redis", func(t *testing.T) { run(t, requireIsolatedRedis(t)) })
}

// requireIsolatedRedis returns a RedisBackend whose key namespace is private
// to this test and removes every key it created afterwards. Cancellation
// scans across every queue in the namespace, so sharing one with the other
// tests would make these both slow and hard to reason about.
func requireIsolatedRedis(t *testing.T) *RedisBackend {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	cluster := "cancel-" + uuid.NewString()
	b := NewRedisBackend([]string{addr}, cluster)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := b.client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not reachable at %s, skipping integration test: %v", addr, err)
	}

	t.Cleanup(func() {
		keys, err := b.client.Keys(context.Background(), "xtrasync:jobs:"+cluster+":*").Result()
		if err == nil && len(keys) > 0 {
			b.client.Del(context.Background(), keys...)
		}
	})

	return b
}

// cancelJob is the fixture the cases below cancel: one Job, some worker
// PartialJobs, optionally a Cleanup step.
type cancelJob struct {
	id          string
	workerKind  string
	cleanupKind string
}

// pushCancelJob pushes a Job scoped to total units of work with workers
// open worker PartialJobs.
func pushCancelJob(t *testing.T, b Backend, total, workers int, withCleanup bool) cancelJob {
	t.Helper()

	kind := uniqueType("cancel")
	job := NewJob(uuid.NewString(), kind, 1000, "", nil)
	fixture := cancelJob{id: job.Id, workerKind: kind + ":worker", cleanupKind: kind + ":cleanup"}

	if withCleanup {
		job.Cleanup = NewPartialJob(uuid.NewString(), fixture.cleanupKind, 1000, job.Id)
	}
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	if err := b.InitJob(job.Id, total, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	for i := 0; i < workers; i++ {
		if err := b.PushPartialJob(NewPartialJob(uuid.NewString(), fixture.workerKind, 1000, job.Id), false); err != nil {
			t.Fatalf("PushPartialJob: %v", err)
		}
	}

	return fixture
}

func requireJob(t *testing.T, b Backend, jobID string) *model.Job {
	t.Helper()

	job, err := b.GetJob(jobID)
	if err != nil || job == nil {
		t.Fatalf("GetJob: %v, %+v", err, job)
	}
	return job
}

func openCount(t *testing.T, b Backend, partialJobKind string) int {
	t.Helper()

	open, err := b.GetOpen(partialJobKind)
	if err != nil {
		t.Fatalf("GetOpen(%s): %v", partialJobKind, err)
	}
	return len(open)
}

func mustCancel(t *testing.T, b Backend, jobID string) {
	t.Helper()

	cancelled, err := b.Cancel(jobID)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !cancelled {
		t.Fatal("expected Cancel to report the Job as cancelled")
	}
}

func mustTake(t *testing.T, b Backend, partialJobKind string) *model.PartialJob {
	t.Helper()

	partialJob, err := b.Take(partialJobKind, "test")
	if err != nil || partialJob == nil {
		t.Fatalf("Take(%s): %v, %+v", partialJobKind, err, partialJob)
	}
	return partialJob
}

func TestCancel_DropsQueuedAndLeavesTakenRunning(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b Backend) {
		fixture := pushCancelJob(t, b, 2, 2, false)
		taken := mustTake(t, b, fixture.workerKind)

		mustCancel(t, b, fixture.id)

		if got := openCount(t, b, fixture.workerKind); got != 0 {
			t.Errorf("expected the queued partial job to be dropped, %d still open", got)
		}
		stillTaken, err := b.GetTaken()
		if err != nil {
			t.Fatalf("GetTaken: %v", err)
		}
		if len(stillTaken) != 1 || stillTaken[0].Id != taken.Id {
			t.Errorf("expected the taken partial job to be left running, got %+v", stillTaken)
		}

		job := requireJob(t, b, fixture.id)
		if job.Status != model.StatusDISMISSED {
			t.Errorf("Status = %q, want DISMISSED", job.Status)
		}
		if job.FinishedAt > 0 {
			t.Error("expected the Job to stay unfinished while a partial job is still running")
		}
	})
}

func TestCancel_FinishesAtOnceWhenNothingIsRunning(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b Backend) {
		fixture := pushCancelJob(t, b, 2, 2, false)

		mustCancel(t, b, fixture.id)

		job := requireJob(t, b, fixture.id)
		if job.FinishedAt <= 0 {
			t.Error("expected the Job to wind down immediately with nothing left to run")
		}
		if job.Status != model.StatusDISMISSED {
			t.Errorf("Status = %q, want DISMISSED", job.Status)
		}
	})
}

func TestCancel_WindsDownWhenTheLastTakenPartialCompletes(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b Backend) {
		fixture := pushCancelJob(t, b, 2, 2, false)
		taken := mustTake(t, b, fixture.workerKind)

		mustCancel(t, b, fixture.id)
		if job := requireJob(t, b, fixture.id); job.FinishedAt > 0 {
			t.Fatal("expected the Job to stay unfinished until its running partial job reports")
		}

		if err := b.Done(taken.Id); err != nil {
			t.Fatalf("Done: %v", err)
		}

		job := requireJob(t, b, fixture.id)
		if job.FinishedAt <= 0 {
			t.Error("expected the Job to wind down once its last partial job finished")
		}
		if job.Status != model.StatusDISMISSED {
			t.Errorf("Status = %q, want DISMISSED", job.Status)
		}
	})
}

func TestCancel_CleanupRunsOnlyAfterEveryOtherPartial(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b Backend) {
		fixture := pushCancelJob(t, b, 2, 2, true)
		taken := mustTake(t, b, fixture.workerKind)

		mustCancel(t, b, fixture.id)
		if got := openCount(t, b, fixture.cleanupKind); got != 0 {
			t.Fatalf("expected cleanup to wait for the running partial job, %d already queued", got)
		}

		if err := b.Done(taken.Id); err != nil {
			t.Fatalf("Done(worker): %v", err)
		}
		if got := openCount(t, b, fixture.cleanupKind); got != 1 {
			t.Fatalf("expected cleanup to be pushed once nothing else was left, %d queued", got)
		}

		cleanup := mustTake(t, b, fixture.cleanupKind)
		if err := b.Done(cleanup.Id); err != nil {
			t.Fatalf("Done(cleanup): %v", err)
		}

		job := requireJob(t, b, fixture.id)
		if job.Status != model.StatusDISMISSED {
			t.Errorf("Status = %q, want DISMISSED", job.Status)
		}
		if job.FinishedAt <= 0 {
			t.Error("expected the Job to be finished after its cleanup ran")
		}
	})
}

func TestCancel_KeepsTheProgressItCaught(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b Backend) {
		fixture := pushCancelJob(t, b, 10, 2, false)
		taken := mustTake(t, b, fixture.workerKind)
		if err := b.UpdatePartialJob(taken.Id, 3); err != nil {
			t.Fatalf("UpdatePartialJob: %v", err)
		}

		mustCancel(t, b, fixture.id)
		if err := b.Done(taken.Id); err != nil {
			t.Fatalf("Done: %v", err)
		}

		job := requireJob(t, b, fixture.id)
		if job.Progress.Current != 3 || job.Progress.Total != 10 {
			t.Errorf("progress = %d/%d, want 3/10", job.Progress.Current, job.Progress.Total)
		}
		if job.Progress.Percent != 30 {
			t.Errorf("percent = %d, want 30 - a dismissed Job must not claim a completion it never reached", job.Progress.Percent)
		}
	})
}

func TestCancel_DoesNotPushFollowUps(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b Backend) {
		fixture := pushCancelJob(t, b, 1, 1, true)

		job := requireJob(t, b, fixture.id)
		job.FollowUps = []model.Job{*NewJob(uuid.NewString(), "follow-up", 1000, "", nil)}
		if err := b.PushJob(job); err != nil {
			t.Fatalf("PushJob: %v", err)
		}

		mustCancel(t, b, fixture.id)

		cleanup := mustTake(t, b, fixture.cleanupKind)
		if err := b.Done(cleanup.Id); err != nil {
			t.Fatalf("Done(cleanup): %v", err)
		}

		jobs, err := b.GetJobs()
		if err != nil {
			t.Fatalf("GetJobs: %v", err)
		}
		if len(jobs) != 1 {
			t.Errorf("expected the dismissed Job to spawn no follow-ups, got %d jobs", len(jobs))
		}
	})
}

func TestCancel_StatusSurvivesAPermanentlyFailingPartial(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b Backend) {
		fixture := pushCancelJob(t, b, 2, 1, false)
		taken := mustTake(t, b, fixture.workerKind)

		mustCancel(t, b, fixture.id)
		if err := b.Error(taken.Id, "boom", false); err != nil {
			t.Fatalf("Error: %v", err)
		}

		job := requireJob(t, b, fixture.id)
		if job.Status != model.StatusDISMISSED {
			t.Errorf("Status = %q, want DISMISSED - an explicit dismissal outranks a partial job's failure", job.Status)
		}
		if !strings.Contains(strings.Join(job.Errors, " "), "boom") {
			t.Errorf("expected the failure to still be recorded on the Job, got %v", job.Errors)
		}
		if job.FinishedAt <= 0 {
			t.Error("expected the Job to wind down after its last partial job failed")
		}
	})
}

func TestCancel_RefusesUnknownFinishedAndRepeated(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b Backend) {
		if cancelled, err := b.Cancel(uuid.NewString()); err != nil || cancelled {
			t.Errorf("Cancel(unknown) = %t, %v, want false, nil", cancelled, err)
		}

		fixture := pushCancelJob(t, b, 1, 1, false)
		mustCancel(t, b, fixture.id)

		if cancelled, err := b.Cancel(fixture.id); err != nil || cancelled {
			t.Errorf("Cancel(already dismissed) = %t, %v, want false, nil", cancelled, err)
		}

		done := pushCancelJob(t, b, 1, 1, false)
		taken := mustTake(t, b, done.workerKind)
		if err := b.StartJob(done.id); err != nil {
			t.Fatalf("StartJob: %v", err)
		}
		if err := b.UpdatePartialJob(taken.Id, 1); err != nil {
			t.Fatalf("UpdatePartialJob: %v", err)
		}
		if err := b.Done(taken.Id); err != nil {
			t.Fatalf("Done: %v", err)
		}
		if cancelled, err := b.Cancel(done.id); err != nil || cancelled {
			t.Errorf("Cancel(finished) = %t, %v, want false, nil", cancelled, err)
		}
	})
}

func TestCancel_SequencedJobWindsDownWithoutStalling(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b Backend) {
		kind := uniqueType("cancel-seq")
		job := NewJob(uuid.NewString(), kind, 1000, "", nil)
		job.Sequence = &model.JobSequence{Current: 0, Remaining: 0}
		if err := b.PushJob(job); err != nil {
			t.Fatalf("PushJob: %v", err)
		}
		if err := b.InitJob(job.Id, 3, nil); err != nil {
			t.Fatalf("InitJob: %v", err)
		}

		stepKinds := []string{kind + ":step0", kind + ":step1", kind + ":step2"}
		for _, stepKind := range stepKinds {
			if err := b.PushPartialJob(NewPartialJob(uuid.NewString(), stepKind, 1000, job.Id), false); err != nil {
				t.Fatalf("PushPartialJob(%s): %v", stepKind, err)
			}
		}
		step0 := mustTake(t, b, stepKinds[0])

		mustCancel(t, b, job.Id)
		for _, stepKind := range stepKinds[1:] {
			if got := openCount(t, b, stepKind); got != 0 {
				t.Errorf("expected %s to be dropped, %d still open", stepKind, got)
			}
		}

		if err := b.Done(step0.Id); err != nil {
			t.Fatalf("Done(step0): %v", err)
		}
		if got := requireJob(t, b, job.Id); got.FinishedAt <= 0 {
			t.Error("expected a sequenced Job to wind down rather than stall on dropped steps")
		}
	})
}
