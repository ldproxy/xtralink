package jobs

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

// requireRedis skips the test if no Redis instance is reachable, so `go
// test ./...` degrades gracefully (skip, not fail) without a local Redis -
// e.g. `docker compose up -d redis` not running.
func requireRedis(t *testing.T) *RedisBackend {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	b := NewRedisBackend([]string{addr}, "")
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := b.client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not reachable at %s, skipping integration test: %v", addr, err)
	}
	return b
}

// uniqueType returns a partial job type unique to this test/call, so
// concurrent tests never share a priority/queue key - both are keyed by
// partial job type in Redis, so two tests using e.g. "worker" would steal
// each other's partial jobs.
func uniqueType(base string) string {
	return base + "-" + uuid.NewString()
}

// cleanupJob removes a Job document (and its finalize-lock key) once the
// test using it is done.
func cleanupJob(t *testing.T, b *RedisBackend, id string) {
	t.Helper()
	t.Cleanup(func() {
		b.client.Del(context.Background(), b.keyJob+id, b.keyFinalized+id)
	})
}

// assertStoredPercent drives job (already pushed to b, with a single
// PartialJob of the given type also already pushed) through
// init/start/progress/finish, checking the stored progress.percent of both
// the Job and its PartialJob at every step. percent is a stored field, not
// derived on read, so each backend has to rewrite it after every change to
// current, total or startedAt - this is what makes sure none of those is
// forgotten. Shared by the MemoryBackend and RedisBackend variants of the
// test so both are held to exactly the same expectations.
func assertStoredPercent(t *testing.T, b Backend, jobID, partialJobType string) {
	t.Helper()

	check := func(step string, wantJob, wantPartialJob int, partialJobID string) {
		t.Helper()

		job, err := b.GetJob(jobID)
		if err != nil || job == nil {
			t.Fatalf("%s: GetJob: %v, %+v", step, err, job)
		}
		if job.Progress.Percent != wantJob {
			t.Errorf("%s: Job percent = %d, want %d (current/total = %d/%d)",
				step, job.Progress.Percent, wantJob, job.Progress.Current, job.Progress.Total)
		}

		if partialJobID == "" {
			return
		}
		partialJob, err := b.GetPartialJob(partialJobID)
		if err != nil || partialJob == nil {
			t.Fatalf("%s: GetPartialJob: %v, %+v", step, err, partialJob)
		}
		if partialJob.Progress.Percent != wantPartialJob {
			t.Errorf("%s: PartialJob percent = %d, want %d (current/total = %d/%d)",
				step, partialJob.Progress.Percent, wantPartialJob,
				partialJob.Progress.Current, partialJob.Progress.Total)
		}
	}

	check("pushed", 0, 0, "")

	if err := b.InitJob(jobID, 4, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	check("init", 0, 0, "")

	taken, err := b.Take(partialJobType, "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}
	if err := b.StartJob(jobID); err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	check("taken", 0, 0, taken.Id)

	if err := b.UpdatePartialJob(taken.Id, 1); err != nil {
		t.Fatalf("UpdatePartialJob: %v", err)
	}
	check("1 of 4", 25, 25, taken.Id)

	if err := b.UpdatePartialJob(taken.Id, 2); err != nil {
		t.Fatalf("UpdatePartialJob: %v", err)
	}
	check("3 of 4", 75, 75, taken.Id)

	if err := b.UpdatePartialJob(taken.Id, 1); err != nil {
		t.Fatalf("UpdatePartialJob: %v", err)
	}
	if err := b.Done(taken.Id); err != nil {
		t.Fatalf("Done: %v", err)
	}
	// The PartialJob is gone once it is done, so only the Job is left to
	// check - it must have been finalized at 100.
	check("done", 100, 0, "")
}

// assertStoredPercentWithoutPartialJobs covers the other shape: a Job with
// no PartialJobs of its own, reporting through InitJob/UpdateJob directly.
// It also pins down that percent depends on startedAt, not just on
// current/total.
func assertStoredPercentWithoutPartialJobs(t *testing.T, b Backend, jobID string) {
	t.Helper()

	check := func(step string, want int) {
		t.Helper()

		job, err := b.GetJob(jobID)
		if err != nil || job == nil {
			t.Fatalf("%s: GetJob: %v, %+v", step, err, job)
		}
		if job.Progress.Percent != want {
			t.Errorf("%s: Job percent = %d, want %d (current/total = %d/%d)",
				step, job.Progress.Percent, want, job.Progress.Current, job.Progress.Total)
		}
	}

	// A Job with no scope of its own counts as complete from the moment it
	// starts - so starting it changes the percentage without any progress
	// being reported at all.
	if err := b.StartJob(jobID); err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	check("started, no scope yet", 100)

	if err := b.InitJob(jobID, 2, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	check("scope grown to 2", 0)

	if err := b.UpdateJob(jobID, 1, nil); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	check("1 of 2", 50)

	if err := b.UpdateJob(jobID, 1, nil); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	check("2 of 2", 100)
}

// cleanupPartialJob removes a standalone PartialJob document and any trace
// of it in the taken list once the test using it is done (Done()/Error()
// normally do this as part of the real lifecycle; tests that call Take()
// without following through need it done explicitly).
func cleanupPartialJob(t *testing.T, b *RedisBackend, id string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		b.client.LRem(ctx, b.keyTaken, 0, id)
		b.client.Del(ctx, b.keyPartial+id)
	})
}
