package jobs

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/ldproxy/xtralink/model"
)

func TestRedisBackend_PushJobAndGetJob(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("push-job")

	job := NewJob(uuid.NewString(), jobType, 1000, "Label", map[string]any{"a": 1})
	cleanupJob(t, b, job.Id)

	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	got, err := b.GetJob(job.Id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got == nil {
		t.Fatal("expected job to exist")
	}
	if got.Label != "Label" || got.Kind != jobType {
		t.Errorf("unexpected job: %+v", got)
	}
	// Inputs is an opaque map round-tripped through JSON, so numbers come
	// back as float64 - the same shape a JobProcessor sees.
	if got.Inputs == nil || got.Inputs["a"] != float64(1) {
		t.Errorf("Inputs = %v, want {\"a\":1}", got.Inputs)
	}
}

func TestRedisBackend_GetJobReturnsNilForUnknownID(t *testing.T) {
	b := requireRedis(t)

	got, err := b.GetJob(uuid.NewString())
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for unknown id, got %+v", got)
	}
}

func TestRedisBackend_PushJobAutoPushesSetup(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("auto-setup")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.Setup = NewPartialJob(uuid.NewString(), jobType+":setup", 1000, job.Id)
	cleanupJob(t, b, job.Id)
	cleanupPartialJob(t, b, job.Setup.Id)

	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	taken, err := b.Take(jobType+":setup", "test-executor")
	if err != nil {
		t.Fatalf("Take: %v", err)
	}
	if taken == nil || taken.Id != job.Setup.Id {
		t.Fatalf("expected to take the auto-pushed setup partial job, got %+v", taken)
	}
	if taken.Executor != "test-executor" {
		t.Error("expected Executor to be set on take")
	}
	if taken.StartedAt <= 0 {
		t.Error("expected StartedAt to be set on take")
	}
}

func TestRedisBackend_PushJobWithoutSetupEnqueuesNothing(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("no-setup")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	cleanupJob(t, b, job.Id)

	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	taken, err := b.Take(jobType, "test")
	if err != nil {
		t.Fatalf("Take: %v", err)
	}
	if taken != nil {
		t.Errorf("expected nothing to be queued for a Job without Setup, got %+v", taken)
	}
}

func TestRedisBackend_TakeReturnsHighestPriorityFirst(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("priority")

	low := NewPartialJob(uuid.NewString(), jobType, 100, "")
	high := NewPartialJob(uuid.NewString(), jobType, 900, "")
	cleanupPartialJob(t, b, low.Id)
	cleanupPartialJob(t, b, high.Id)

	if err := b.PushPartialJob(low, false); err != nil {
		t.Fatalf("PushPartialJob(low): %v", err)
	}
	if err := b.PushPartialJob(high, false); err != nil {
		t.Fatalf("PushPartialJob(high): %v", err)
	}

	taken, err := b.Take(jobType, "test")
	if err != nil {
		t.Fatalf("Take: %v", err)
	}
	if taken == nil || taken.Id != high.Id {
		t.Fatalf("expected higher-priority partial job first, got %+v", taken)
	}

	taken2, err := b.Take(jobType, "test")
	if err != nil {
		t.Fatalf("Take (2): %v", err)
	}
	if taken2 == nil || taken2.Id != low.Id {
		t.Fatalf("expected lower-priority partial job second, got %+v", taken2)
	}
}

func TestRedisBackend_TakeReturnsNilWhenQueueEmpty(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("empty-queue")

	taken, err := b.Take(jobType, "test")
	if err != nil {
		t.Fatalf("Take: %v", err)
	}
	if taken != nil {
		t.Errorf("expected nil for an empty queue, got %+v", taken)
	}
}

func TestRedisBackend_DoneRemovesFromTakenAndDeletesPartialJob(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("done")

	partialJob := NewPartialJob(uuid.NewString(), jobType, 1000, "")
	cleanupPartialJob(t, b, partialJob.Id)
	if err := b.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}
	taken, err := b.Take(jobType, "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}

	if err := b.Done(taken.Id); err != nil {
		t.Fatalf("Done: %v", err)
	}

	ctx := context.Background()
	takenIDs, err := b.client.LRange(ctx, b.keyTaken, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange: %v", err)
	}
	for _, id := range takenIDs {
		if id == taken.Id {
			t.Error("expected partial job to be removed from the taken list")
		}
	}
	if got, err := b.getPartialJob(ctx, taken.Id); err != nil {
		t.Fatalf("getPartialJob: %v", err)
	} else if got != nil {
		t.Errorf("expected partial job document to be deleted after Done(), got %+v", got)
	}
}

func TestRedisBackend_DoneOnUnknownIsNoop(t *testing.T) {
	b := requireRedis(t)
	if err := b.Done(uuid.NewString()); err != nil {
		t.Errorf("expected no error for an unknown partial job id, got %v", err)
	}
}

func TestRedisBackend_ErrorRetriesThenPermanentlyFails(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("error-exhaust")

	partialJob := NewPartialJob(uuid.NewString(), jobType, 1000, "")
	cleanupPartialJob(t, b, partialJob.Id)
	if err := b.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	id := partialJob.Id
	for i := 0; i < maxRetries; i++ {
		taken, err := b.Take(jobType, "test")
		if err != nil || taken == nil {
			t.Fatalf("Take (attempt %d): %v, %+v", i+1, err, taken)
		}
		if err := b.Error(taken.Id, "transient", true); err != nil {
			t.Fatalf("Error (attempt %d): %v", i+1, err)
		}
	}

	// One more failure exceeds maxRetries and should be permanent.
	taken, err := b.Take(jobType, "test")
	if err != nil || taken == nil {
		t.Fatalf("Take (final): %v, %+v", err, taken)
	}
	if err := b.Error(taken.Id, "final failure", true); err != nil {
		t.Fatalf("Error (final): %v", err)
	}

	failed, err := b.GetFailed()
	if err != nil {
		t.Fatalf("GetFailed: %v", err)
	}
	found := false
	for _, pj := range failed {
		if pj.Id == id {
			found = true
			if len(pj.Errors) != maxRetries+1 {
				t.Errorf("expected %d accumulated error messages, got %d: %v", maxRetries+1, len(pj.Errors), pj.Errors)
			}
		}
	}
	if !found {
		t.Errorf("expected partial job %s in failed list", id)
	}
	t.Cleanup(func() { b.client.LRem(context.Background(), b.keyFailed, 0, id) })
}

func TestRedisBackend_InitJobGrowsTotal(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("init-total")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	cleanupJob(t, b, job.Id)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	if err := b.InitJob(job.Id, 5, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	if err := b.InitJob(job.Id, 3, nil); err != nil {
		t.Fatalf("InitJob (2): %v", err)
	}

	got, err := b.GetJob(job.Id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Progress.Total != 8 {
		t.Errorf("Total = %d, want 8", got.Progress.Total)
	}
}

func TestRedisBackend_UpdateJobAppliesProgressUpdatesToProgressDetails(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("update-progress")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.Progress.Details = map[string]any{"nested": map[string]any{"count": 0}}
	cleanupJob(t, b, job.Id)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	updates := []model.ProgressUpdate{{Path: "nested.count", Operation: model.ProgressOperationADD}}
	if err := b.UpdateJob(job.Id, 4, updates); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	got, err := b.GetJob(job.Id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Progress.Current != 4 {
		t.Errorf("Current = %d, want 4", got.Progress.Current)
	}

	nested, ok := got.Progress.Details["nested"].(map[string]any)
	if !ok {
		t.Fatalf("progressDetails.nested is not a map[string]any: %+v", got.Progress.Details)
	}
	count, ok := nested["count"].(float64)
	if !ok {
		t.Fatalf("progressDetails.nested.count is not a number: %+v", nested)
	}
	if int(count) != 4 {
		t.Errorf("progressDetails.nested.count = %d, want 4", int(count))
	}
}

// TestRedisBackend_UpdatePartialJobFansOutViaProgressUpdates is the Redis
// counterpart to the MemoryBackend test of the same shape, including the
// array-indexed path form (levels.<tms>[<level>]) tileseeding uses - not
// just plain dotted paths.
func TestRedisBackend_UpdatePartialJobFansOutViaProgressUpdates(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("fanout")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.Progress.Details = map[string]any{"remaining": 10, "levels": map[string]any{"demo": []int{-1, 8, -1}}}
	cleanupJob(t, b, job.Id)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	partialJob := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, job.Id)
	partialJob.Progress.Total = 5
	partialJob.ProgressUpdates = []model.ProgressUpdate{
		{Path: "remaining", Operation: model.ProgressOperationSUBTRACT},
		{Path: "levels.demo[1]", Operation: model.ProgressOperationSUBTRACT},
	}
	cleanupPartialJob(t, b, partialJob.Id)
	if err := b.InitJob(job.Id, 5, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	if err := b.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	if err := b.UpdatePartialJob(partialJob.Id, 3); err != nil {
		t.Fatalf("UpdatePartialJob: %v", err)
	}

	gotPartial, err := b.getPartialJob(context.Background(), partialJob.Id)
	if err != nil {
		t.Fatalf("getPartialJob: %v", err)
	}
	if gotPartial.Progress.Current != 3 {
		t.Errorf("partialJob.Current = %d, want 3", gotPartial.Progress.Current)
	}

	gotJob, err := b.GetJob(job.Id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if gotJob.Progress.Current != 3 {
		t.Errorf("job.Current = %d, want 3 (fanned out from partial job delta)", gotJob.Progress.Current)
	}

	remaining, ok := gotJob.Progress.Details["remaining"].(float64)
	if !ok {
		t.Fatalf("progressDetails.remaining is not a number: %+v", gotJob.Progress.Details)
	}
	if int(remaining) != 7 {
		t.Errorf("progressDetails.remaining = %d, want 7 (10-3)", int(remaining))
	}

	levels, ok := gotJob.Progress.Details["levels"].(map[string]any)
	if !ok {
		t.Fatalf("progressDetails.levels is not a map[string]any: %+v", gotJob.Progress.Details)
	}
	demo, ok := levels["demo"].([]any)
	if !ok {
		t.Fatalf("progressDetails.levels.demo is not a []any: %+v", levels)
	}
	level1, ok := demo[1].(float64)
	if !ok {
		t.Fatalf("progressDetails.levels.demo[1] is not a number: %+v", demo)
	}
	if int(level1) != 5 {
		t.Errorf("progressDetails.levels.demo[1] = %d, want 5 (8-3)", int(level1))
	}
}

func TestRedisBackend_UpdatePartialJob_UnknownIDIsError(t *testing.T) {
	b := requireRedis(t)
	if err := b.UpdatePartialJob(uuid.NewString(), 1); err == nil {
		t.Fatal("expected an error for an unknown partial job id")
	}
}

func TestRedisBackend_OnPartialJobDone_SetupFinishing_SyncsEmbeddedSnapshotOnly(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("setup-done")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.Setup = NewPartialJob(uuid.NewString(), jobType+":setup", 1000, job.Id)
	cleanupJob(t, b, job.Id)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	taken, err := b.Take(jobType+":setup", "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}
	if err := b.Done(taken.Id); err != nil {
		t.Fatalf("Done: %v", err)
	}

	got, err := b.GetJob(job.Id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Setup == nil || got.Setup.FinishedAt <= 0 {
		t.Errorf("expected embedded setup snapshot to show finishedAt set, got %+v", got.Setup)
	}
	// Setup finishing must not mark the Job itself as finished - only the
	// setup processor (elsewhere) decides what happens next.
	if got.FinishedAt > 0 {
		t.Errorf("expected Job.FinishedAt to remain unset after setup alone finishes, got %d", got.FinishedAt)
	}
}

func TestRedisBackend_OnPartialJobDone_LastPartialJobFinalizesAndPushesCleanup(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("finalize-cleanup")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.Cleanup = NewPartialJob(uuid.NewString(), jobType+":cleanup", 1000, job.Id)
	cleanupJob(t, b, job.Id)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	worker := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, job.Id)
	worker.Progress.Total = 1
	cleanupPartialJob(t, b, worker.Id)
	if err := b.InitJob(job.Id, 1, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	if err := b.PushPartialJob(worker, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	taken, err := b.Take(jobType+":worker", "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}
	// Calling the backend directly here, bypassing the Runner - which is
	// normally what calls StartJob for the first non-setup PartialJob taken.
	// Without it, IsStarted() stays false and IsDone() can never be true
	// even once current==total.
	if err := b.StartJob(job.Id); err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	if err := b.UpdatePartialJob(taken.Id, 1); err != nil {
		t.Fatalf("UpdatePartialJob: %v", err)
	}
	if err := b.Done(taken.Id); err != nil {
		t.Fatalf("Done: %v", err)
	}

	got, err := b.GetJob(job.Id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.FinishedAt <= 0 {
		t.Fatal("expected Job.FinishedAt to be set once the last partial job finishes")
	}

	cleanupTaken, err := b.Take(jobType+":cleanup", "test")
	if err != nil {
		t.Fatalf("Take (cleanup): %v", err)
	}
	if cleanupTaken == nil || cleanupTaken.Id != job.Cleanup.Id {
		t.Fatalf("expected cleanup partial job to have been pushed automatically, got %+v", cleanupTaken)
	}
	cleanupPartialJob(t, b, cleanupTaken.Id)
}

func TestRedisBackend_OnPartialJobDone_CleanupFinishing_KeepsProgressDetailsAndPushesFollowUps(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("cleanup-done")

	followUp := NewJob(uuid.NewString(), jobType+"-followup", 1000, "", nil)

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.Progress.Details = map[string]any{"some": "detail"}
	job.Cleanup = NewPartialJob(uuid.NewString(), jobType+":cleanup", 1000, job.Id)
	job.FollowUps = []model.Job{*followUp}
	cleanupJob(t, b, job.Id)
	cleanupJob(t, b, followUp.Id)

	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	if err := b.PushPartialJob(job.Cleanup, false); err != nil {
		t.Fatalf("PushPartialJob(cleanup): %v", err)
	}

	taken, err := b.Take(jobType+":cleanup", "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}
	if err := b.Done(taken.Id); err != nil {
		t.Fatalf("Done: %v", err)
	}

	got, err := b.GetJob(job.Id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Progress.Details == nil || got.Progress.Details["some"] != "detail" {
		t.Errorf("expected progressDetails to still be set after successful cleanup, got %+v", got.Progress.Details)
	}

	pushedFollowUp, err := b.GetJob(followUp.Id)
	if err != nil {
		t.Fatalf("GetJob(followUp): %v", err)
	}
	if pushedFollowUp == nil {
		t.Error("expected followUp job to have been pushed once cleanup finished")
	}
}

func TestRedisBackend_OnPartialJobPermanentlyFailed_SettlesRemainderAndFinalizes(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("permfail-total")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	cleanupJob(t, b, job.Id)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	okPartial := NewPartialJob(uuid.NewString(), jobType+":ok", 1000, job.Id)
	okPartial.Progress.Total = 3
	badPartial := NewPartialJob(uuid.NewString(), jobType+":bad", 1000, job.Id)
	badPartial.Progress.Total = 5
	cleanupPartialJob(t, b, okPartial.Id)
	cleanupPartialJob(t, b, badPartial.Id)

	for _, pj := range []*model.PartialJob{okPartial, badPartial} {
		if err := b.InitJob(job.Id, pj.Progress.Total, nil); err != nil {
			t.Fatalf("InitJob: %v", err)
		}
		if err := b.PushPartialJob(pj, false); err != nil {
			t.Fatalf("PushPartialJob: %v", err)
		}
	}

	okTaken, err := b.Take(jobType+":ok", "test")
	if err != nil || okTaken == nil {
		t.Fatalf("Take(ok): %v, %+v", err, okTaken)
	}
	// Bypassing the Runner here, which normally calls this automatically.
	if err := b.StartJob(job.Id); err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	if err := b.UpdatePartialJob(okTaken.Id, 3); err != nil {
		t.Fatalf("UpdatePartialJob(ok): %v", err)
	}
	if err := b.Done(okTaken.Id); err != nil {
		t.Fatalf("Done(ok): %v", err)
	}

	badTaken, err := b.Take(jobType+":bad", "test")
	if err != nil || badTaken == nil {
		t.Fatalf("Take(bad): %v, %+v", err, badTaken)
	}
	if err := b.UpdatePartialJob(badTaken.Id, 2); err != nil {
		t.Fatalf("UpdatePartialJob(bad): %v", err)
	}
	if err := b.Error(badTaken.Id, "boom", false); err != nil {
		t.Fatalf("Error(bad): %v", err)
	}
	t.Cleanup(func() { b.client.LRem(context.Background(), b.keyFailed, 0, badTaken.Id) })

	final, err := b.GetJob(job.Id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	// Total stays at 3+5=8; the bad partial job only reached 2 of its 5, so
	// its unfinished remainder (3) is added to current instead, letting
	// current reach total and the Job finalize.
	if final.Progress.Total != 8 {
		t.Errorf("Total = %d, want 8", final.Progress.Total)
	}
	if final.Progress.Current != 8 {
		t.Errorf("Current = %d, want 8", final.Progress.Current)
	}
	if final.Status != model.StatusFAILED {
		t.Errorf("Status = %s, want failed", final.Status)
	}
}

func TestRedisBackend_OnPartialJobPermanentlyFailed_SetupForcesJobFailed(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("permfail-setup")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.Setup = NewPartialJob(uuid.NewString(), jobType+":setup", 1000, job.Id)
	cleanupJob(t, b, job.Id)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	taken, err := b.Take(jobType+":setup", "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}
	if err := b.Error(taken.Id, "setup exploded", false); err != nil {
		t.Fatalf("Error: %v", err)
	}
	t.Cleanup(func() { b.client.LRem(context.Background(), b.keyFailed, 0, taken.Id) })

	final, err := b.GetJob(job.Id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if final.FinishedAt <= 0 {
		t.Fatal("expected Job to be forced to a finished state when setup fails permanently")
	}
	if final.Status != model.StatusFAILED {
		t.Errorf("Status = %s, want failed", final.Status)
	}
	found := false
	for _, e := range final.Errors {
		if e == "setup exploded" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected setup's error message in Job.Errors, got %v", final.Errors)
	}
}

func TestRedisBackend_OnPartialJobPermanentlyFailed_CleanupMergesErrorWithoutTouchingTotal(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("permfail-cleanup")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.Cleanup = NewPartialJob(uuid.NewString(), jobType+":cleanup", 1000, job.Id)
	cleanupJob(t, b, job.Id)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	// Simulate the job already being done before cleanup runs.
	if err := b.InitJob(job.Id, 2, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}

	if err := b.PushPartialJob(job.Cleanup, false); err != nil {
		t.Fatalf("PushPartialJob(cleanup): %v", err)
	}
	taken, err := b.Take(jobType+":cleanup", "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}
	if err := b.Error(taken.Id, "cleanup exploded", false); err != nil {
		t.Fatalf("Error: %v", err)
	}
	t.Cleanup(func() { b.client.LRem(context.Background(), b.keyFailed, 0, taken.Id) })

	final, err := b.GetJob(job.Id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if final.Progress.Total != 2 {
		t.Errorf("expected cleanup failure to leave Total untouched, got %d", final.Progress.Total)
	}
	found := false
	for _, e := range final.Errors {
		if e == "cleanup exploded" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected cleanup's error message in Job.Errors, got %v", final.Errors)
	}
}

// TestRedisBackend_ProgressDetailsPreservedOnSuccessAndFailure pins down that
// progressDetails outlives the Job either way - it is the finished job's
// per-type result detail (e.g. which tiles were seeded), not scratch state to
// be discarded once the last PartialJob is in.
func TestRedisBackend_ProgressDetailsPreservedOnSuccessAndFailure(t *testing.T) {
	b := requireRedis(t)

	run := func(t *testing.T, fail bool) *model.Job {
		jobType := uniqueType("pd-keep")
		job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
		job.Progress.Details = map[string]any{"some": "detail"}
		cleanupJob(t, b, job.Id)
		if err := b.PushJob(job); err != nil {
			t.Fatalf("PushJob: %v", err)
		}

		worker := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, job.Id)
		worker.Progress.Total = 1
		cleanupPartialJob(t, b, worker.Id)
		if err := b.InitJob(job.Id, worker.Progress.Total, nil); err != nil {
			t.Fatalf("InitJob: %v", err)
		}
		if err := b.PushPartialJob(worker, false); err != nil {
			t.Fatalf("PushPartialJob: %v", err)
		}

		taken, err := b.Take(worker.Kind, "test")
		if err != nil || taken == nil {
			t.Fatalf("Take: %v, %+v", err, taken)
		}
		// Bypassing the Runner here, which normally calls this automatically.
		if err := b.StartJob(job.Id); err != nil {
			t.Fatalf("StartJob: %v", err)
		}
		if fail {
			if err := b.Error(taken.Id, "boom", false); err != nil {
				t.Fatalf("Error: %v", err)
			}
			t.Cleanup(func() { b.client.LRem(context.Background(), b.keyFailed, 0, taken.Id) })
		} else {
			if err := b.UpdatePartialJob(taken.Id, 1); err != nil {
				t.Fatalf("UpdatePartialJob: %v", err)
			}
			if err := b.Done(taken.Id); err != nil {
				t.Fatalf("Done: %v", err)
			}
		}

		final, err := b.GetJob(job.Id)
		if err != nil {
			t.Fatalf("GetJob: %v", err)
		}
		return final
	}

	success := run(t, false)
	if success.Progress.Details == nil || success.Progress.Details["some"] != "detail" {
		t.Errorf("success: expected progressDetails preserved, got %+v", success.Progress.Details)
	}

	failure := run(t, true)
	if failure.Progress.Details == nil || failure.Progress.Details["some"] != "detail" {
		t.Errorf("failure: expected progressDetails preserved, got %+v", failure.Progress.Details)
	}
}

// TestRedisBackend_RetriedThenSucceededPartialJobDoesNotFailJob is a
// regression test: a PartialJob that fails a couple of times (retried) but
// eventually succeeds must not drag its transient retry-attempt messages
// into the Job's permanent error list - only Error()'s *permanent* failure
// path should ever do that.
func TestRedisBackend_RetriedThenSucceededPartialJobDoesNotFailJob(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("retry-then-succeed")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	cleanupJob(t, b, job.Id)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	worker := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, job.Id)
	worker.Progress.Total = 1
	cleanupPartialJob(t, b, worker.Id)
	if err := b.InitJob(job.Id, worker.Progress.Total, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	if err := b.PushPartialJob(worker, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	for i := 0; i < 2; i++ {
		taken, err := b.Take(worker.Kind, "test")
		if err != nil || taken == nil {
			t.Fatalf("Take (retry %d): %v, %+v", i+1, err, taken)
		}
		if err := b.Error(taken.Id, "transient", true); err != nil {
			t.Fatalf("Error (retry %d): %v", i+1, err)
		}
	}

	taken, err := b.Take(worker.Kind, "test")
	if err != nil || taken == nil {
		t.Fatalf("Take (final): %v, %+v", err, taken)
	}
	// Bypassing the Runner here, which normally calls this automatically.
	if err := b.StartJob(job.Id); err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	if err := b.UpdatePartialJob(taken.Id, 1); err != nil {
		t.Fatalf("UpdatePartialJob: %v", err)
	}
	if err := b.Done(taken.Id); err != nil {
		t.Fatalf("Done: %v", err)
	}

	final, err := b.GetJob(job.Id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if final.Status != model.StatusSUCCESSFUL {
		t.Errorf("Status = %s, want successful (errors=%v)", final.Status, final.Errors)
	}
	if len(final.Errors) != 0 {
		t.Errorf("expected no errors on the Job from a partial job that ultimately succeeded, got %v", final.Errors)
	}
}

// TestRedisBackend_SequentialPartialJobsGatedByCurrentSequence is the Redis
// counterpart to the MemoryBackend test of the same shape: one PartialJob
// per step, the parent Job opting into sequencing, so each step only becomes
// takeable once the previous one's Done() has advanced Job.Sequence.Current.
func TestRedisBackend_SequentialPartialJobsGatedByCurrentSequence(t *testing.T) {
	b := requireRedis(t)

	job := NewJob(uuid.NewString(), uniqueType("sequential"), 1000, "", nil)
	job.Sequence = &model.JobSequence{Current: 0, Remaining: 0}
	cleanupJob(t, b, job.Id)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	step0Type := job.Kind + ":step0"
	step1Type := job.Kind + ":step1"
	step2Type := job.Kind + ":step2"

	step0 := NewPartialJob(uuid.NewString(), step0Type, 1000, job.Id)
	step1 := NewPartialJob(uuid.NewString(), step1Type, 1000, job.Id)
	step2 := NewPartialJob(uuid.NewString(), step2Type, 1000, job.Id)
	for _, pj := range []*model.PartialJob{step0, step1, step2} {
		cleanupPartialJob(t, b, pj.Id)
		if err := b.PushPartialJob(pj, false); err != nil {
			t.Fatalf("PushPartialJob: %v", err)
		}
	}

	if taken, err := b.Take(step1Type, "test"); err != nil || taken != nil {
		t.Fatalf("Take(step1) before step0 is done = %+v, %v, want nil, nil", taken, err)
	}
	if taken, err := b.Take(step2Type, "test"); err != nil || taken != nil {
		t.Fatalf("Take(step2) before step0 is done = %+v, %v, want nil, nil", taken, err)
	}

	taken0, err := b.Take(step0Type, "test")
	if err != nil || taken0 == nil || taken0.Id != step0.Id {
		t.Fatalf("Take(step0): %v, %+v", err, taken0)
	}
	if err := b.Done(taken0.Id); err != nil {
		t.Fatalf("Done(step0): %v", err)
	}

	got, err := b.GetJob(job.Id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Sequence.Current != 1 {
		t.Errorf("Sequence.Current = %d, want 1 after step0 finished", got.Sequence.Current)
	}
	if taken, err := b.Take(step2Type, "test"); err != nil || taken != nil {
		t.Fatalf("Take(step2) before step1 is done = %+v, %v, want nil, nil", taken, err)
	}

	taken1, err := b.Take(step1Type, "test")
	if err != nil || taken1 == nil || taken1.Id != step1.Id {
		t.Fatalf("Take(step1): %v, %+v", err, taken1)
	}
	if err := b.Done(taken1.Id); err != nil {
		t.Fatalf("Done(step1): %v", err)
	}

	got, err = b.GetJob(job.Id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Sequence.Current != 2 {
		t.Errorf("Sequence.Current = %d, want 2 after step1 finished", got.Sequence.Current)
	}

	taken2, err := b.Take(step2Type, "test")
	if err != nil || taken2 == nil || taken2.Id != step2.Id {
		t.Fatalf("Take(step2): %v, %+v", err, taken2)
	}
}

// TestRedisBackend_SequenceAdvancesOnlyWhenAllItsPartialJobsAreDone covers
// fan-out within a single Sequence - the next Sequence must stay gated
// until every PartialJob of the current one has finished, not just one.
func TestRedisBackend_SequenceAdvancesOnlyWhenAllItsPartialJobsAreDone(t *testing.T) {
	b := requireRedis(t)

	job := NewJob(uuid.NewString(), uniqueType("fanout-sequence"), 1000, "", nil)
	job.Sequence = &model.JobSequence{Current: 0, Remaining: 0}
	cleanupJob(t, b, job.Id)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	step0Type := job.Kind + ":step0"
	step1Type := job.Kind + ":step1"

	step0a := NewPartialJob(uuid.NewString(), step0Type, 1000, job.Id)
	step0b := NewPartialJob(uuid.NewString(), step0Type, 1000, job.Id)
	step1 := NewPartialJob(uuid.NewString(), step1Type, 1000, job.Id)
	for _, pj := range []*model.PartialJob{step0a, step0b, step1} {
		cleanupPartialJob(t, b, pj.Id)
		if err := b.PushPartialJob(pj, false); err != nil {
			t.Fatalf("PushPartialJob: %v", err)
		}
	}

	takenA, err := b.Take(step0Type, "test")
	if err != nil || takenA == nil {
		t.Fatalf("Take(step0 #1): %v, %+v", err, takenA)
	}
	if err := b.Done(takenA.Id); err != nil {
		t.Fatalf("Done(step0 #1): %v", err)
	}
	if taken, err := b.Take(step1Type, "test"); err != nil || taken != nil {
		t.Fatalf("Take(step1) before both step0 partials are done = %+v, %v", taken, err)
	}

	takenB, err := b.Take(step0Type, "test")
	if err != nil || takenB == nil {
		t.Fatalf("Take(step0 #2): %v, %+v", err, takenB)
	}
	if err := b.Done(takenB.Id); err != nil {
		t.Fatalf("Done(step0 #2): %v", err)
	}

	taken1, err := b.Take(step1Type, "test")
	if err != nil || taken1 == nil || taken1.Id != step1.Id {
		t.Fatalf("Take(step1) after both step0 partials done: %v, %+v", err, taken1)
	}
}

// TestRedisBackend_JobWithoutSequenceIgnoresSequenceGating confirms Sequence
// gating is a no-op unless the parent Job explicitly opts into it.
func TestRedisBackend_JobWithoutSequenceIgnoresSequenceGating(t *testing.T) {
	b := requireRedis(t)

	job := NewJob(uuid.NewString(), uniqueType("parallel-with-sequence"), 1000, "", nil)
	cleanupJob(t, b, job.Id)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	late := NewPartialJob(uuid.NewString(), job.Kind+":late", 1000, job.Id)
	seq := 5 // would never be "its turn" under gating (Sequence.Current starts at 0)
	late.Sequence = &seq
	cleanupPartialJob(t, b, late.Id)
	if err := b.PushPartialJob(late, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	taken, err := b.Take(late.Kind, "test")
	if err != nil || taken == nil || taken.Id != late.Id {
		t.Fatalf("expected a Job without Sequence to ignore Sequence gating entirely, got %v, %+v", err, taken)
	}
}

// TestRedisBackend_PercentIsStoredAndKeptUpToDate covers the stored
// progress.percent, which Redis cannot derive on read: progress moves via
// atomic JSON.NUMINCRBY, so percent has to be recomputed and written back
// after each of those.
func TestRedisBackend_PercentIsStoredAndKeptUpToDate(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("percent")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	cleanupJob(t, b, job.Id)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	worker := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, job.Id)
	worker.Progress.Total = 4
	cleanupPartialJob(t, b, worker.Id)
	if err := b.PushPartialJob(worker, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	assertStoredPercent(t, b, job.Id, worker.Kind)

	standalone := NewJob(uuid.NewString(), jobType+":standalone", 1000, "", nil)
	cleanupJob(t, b, standalone.Id)
	if err := b.PushJob(standalone); err != nil {
		t.Fatalf("PushJob(standalone): %v", err)
	}
	assertStoredPercentWithoutPartialJobs(t, b, standalone.Id)
}
