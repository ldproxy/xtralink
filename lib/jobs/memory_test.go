package jobs

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ldproxy/xtralink/model"
)

func TestMemoryBackend_PushJobAndGetJob(t *testing.T) {
	b := NewMemoryBackend()

	job := NewJob(uuid.NewString(), "demo", 1000, "Label", map[string]any{"a": 1})
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	got, err := b.GetJob(job.Id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got == nil || got.Label != "Label" || got.Kind != "demo" {
		t.Errorf("unexpected job: %+v", got)
	}
	if got.Inputs == nil || got.Inputs["a"] != float64(1) {
		t.Errorf("Inputs = %v, want {\"a\":1}", got.Inputs)
	}
	if got == job {
		t.Error("expected GetJob to return a copy, not the same pointer that was pushed")
	}
}

func TestMemoryBackend_GetJobReturnsNilForUnknownID(t *testing.T) {
	b := NewMemoryBackend()

	got, err := b.GetJob(uuid.NewString())
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for unknown id, got %+v", got)
	}
}

func TestMemoryBackend_PushJobAutoPushesSetup(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("auto-setup")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.Setup = NewPartialJob(uuid.NewString(), jobType+":setup", 1000, job.Id)

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

func TestMemoryBackend_PushJobWithoutSetupEnqueuesNothing(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("no-setup")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
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

func TestMemoryBackend_TakeReturnsHighestPriorityFirst(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("priority")

	low := NewPartialJob(uuid.NewString(), jobType, 100, "")
	high := NewPartialJob(uuid.NewString(), jobType, 900, "")
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

func TestMemoryBackend_TakeReturnsNilWhenQueueEmpty(t *testing.T) {
	b := NewMemoryBackend()

	taken, err := b.Take(uniqueType("empty-queue"), "test")
	if err != nil {
		t.Fatalf("Take: %v", err)
	}
	if taken != nil {
		t.Errorf("expected nil for an empty queue, got %+v", taken)
	}
}

func TestMemoryBackend_DoneRemovesFromTakenAndDeletesPartialJob(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("done")

	partialJob := NewPartialJob(uuid.NewString(), jobType, 1000, "")
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

	stillTaken, err := b.GetTaken()
	if err != nil {
		t.Fatalf("GetTaken: %v", err)
	}
	for _, pj := range stillTaken {
		if pj.Id == taken.Id {
			t.Error("expected partial job to be removed from the taken list")
		}
	}
	if b.partial[taken.Id] != nil {
		t.Error("expected partial job document to be deleted after Done()")
	}
}

func TestMemoryBackend_DoneOnUnknownIsNoop(t *testing.T) {
	b := NewMemoryBackend()
	if err := b.Done(uuid.NewString()); err != nil {
		t.Errorf("expected no error for an unknown partial job id, got %v", err)
	}
}

func TestMemoryBackend_ErrorRetriesThenPermanentlyFails(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("error-exhaust")

	partialJob := NewPartialJob(uuid.NewString(), jobType, 1000, "")
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
}

func TestMemoryBackend_InitJobGrowsTotal(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("init-total")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
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

func TestMemoryBackend_UpdateJobAppliesProgressUpdatesToProgressDetails(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("update-progress")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.Progress.Details = map[string]any{"nested": map[string]any{"count": 0}}
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

	var details struct {
		Nested struct {
			Count int `json:"count"`
		} `json:"nested"`
	}
	if got.Progress.Details == nil {
		t.Fatalf("progressDetails is not a map[string]any")
	}
	nested, ok := got.Progress.Details["nested"].(map[string]any)
	if !ok {
		t.Fatalf("progressDetails.nested is not a map[string]any")
	}
	count, ok := nested["count"].(float64)
	if !ok {
		t.Fatalf("progressDetails.nested.count is not a number")
	}
	details.Nested.Count = int(count)
	if details.Nested.Count != 4 {
		t.Errorf("progressDetails.nested.count = %d, want 4", details.Nested.Count)
	}
}

// TestMemoryBackend_UpdatePartialJobFansOutViaUpdateTargets exercises the
// array-indexed path form (tileSets.<x>.progress.levels.<tms>[<level>]),
// not just plain dotted paths - the same shape tileseedingdemo uses.
func TestMemoryBackend_UpdatePartialJobFansOutViaUpdateTargets(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("fanout")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.Progress.Details = map[string]any{"remaining": 10, "levels": map[string]any{"demo": []int{-1, 8, -1}}}
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	partialJob := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, job.Id)
	partialJob.Progress.Total = 5
	partialJob.ProgressUpdates = []model.ProgressUpdate{
		{Path: "remaining", Operation: model.ProgressOperationSUBTRACT},
		{Path: "levels.demo[1]", Operation: model.ProgressOperationSUBTRACT},
	}
	if err := b.InitJob(job.Id, 5, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	if err := b.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	if err := b.UpdatePartialJob(partialJob.Id, 3); err != nil {
		t.Fatalf("UpdatePartialJob: %v", err)
	}

	gotPartial := b.partial[partialJob.Id]
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
	resultRemaining := 0
	resultLevelsDemo := make([]int, 3)
	if gotJob.Progress.Details == nil {
		t.Fatalf("progressDetails is not a map[string]any")
	}
	remaining, ok := gotJob.Progress.Details["remaining"].(float64)
	if !ok {
		t.Fatalf("progressDetails.remaining is not a number")
	}
	resultRemaining = int(remaining)
	levels, ok := gotJob.Progress.Details["levels"].(map[string]any)
	if !ok {
		t.Fatalf("progressDetails.levels is not a map[string]any")
	}
	demo, ok := levels["demo"].([]any)
	if !ok {
		t.Fatalf("progressDetails.levels.demo is not a []any")
	}
	for i, v := range demo {
		val, ok := v.(float64)
		if !ok {
			t.Fatalf("progressDetails.levels.demo[%d] is not a number", i)
		}
		resultLevelsDemo[i] = int(val)
	}
	if resultRemaining != 7 {
		t.Errorf("progressDetails.remaining = %d, want 7 (10-3)", resultRemaining)
	}
	if resultLevelsDemo[1] != 5 {
		t.Errorf("progressDetails.levels.demo[1] = %d, want 5 (8-3)", resultLevelsDemo[1])
	}
}

func TestMemoryBackend_UpdatePartialJob_UnknownIDIsError(t *testing.T) {
	b := NewMemoryBackend()
	if err := b.UpdatePartialJob(uuid.NewString(), 1); err == nil {
		t.Fatal("expected an error for an unknown partial job id")
	}
}

func TestMemoryBackend_OnPartialJobDone_SetupFinishing_SyncsEmbeddedSnapshotOnly(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("setup-done")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.Setup = NewPartialJob(uuid.NewString(), jobType+":setup", 1000, job.Id)
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
	if got.FinishedAt > 0 {
		t.Errorf("expected Job.FinishedAt to remain unset after setup alone finishes, got %d", got.FinishedAt)
	}
}

func TestMemoryBackend_OnPartialJobDone_LastPartialJobFinalizesAndPushesCleanup(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("finalize-cleanup")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.Cleanup = NewPartialJob(uuid.NewString(), jobType+":cleanup", 1000, job.Id)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	worker := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, job.Id)
	worker.Progress.Total = 1
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
}

func TestMemoryBackend_OnPartialJobDone_CleanupFinishing_KeepsProgressDetailsAndPushesFollowUps(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("cleanup-done")

	followUp := NewJob(uuid.NewString(), jobType+"-followup", 1000, "", nil)

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.Progress.Details = map[string]any{"some": "detail"}
	job.Cleanup = NewPartialJob(uuid.NewString(), jobType+":cleanup", 1000, job.Id)
	job.FollowUps = []model.Job{*followUp}

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

func TestMemoryBackend_OnPartialJobPermanentlyFailed_ReducesTotalAndFinalizes(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("permfail-total")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	okPartial := NewPartialJob(uuid.NewString(), jobType+":ok", 1000, job.Id)
	okPartial.Progress.Total = 3
	badPartial := NewPartialJob(uuid.NewString(), jobType+":bad", 1000, job.Id)
	badPartial.Progress.Total = 5

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
	if err := b.Error(badTaken.Id, "boom", false); err != nil {
		t.Fatalf("Error(bad): %v", err)
	}

	final, err := b.GetJob(job.Id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
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

func TestMemoryBackend_OnPartialJobPermanentlyFailed_SetupForcesJobFailed(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("permfail-setup")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.Setup = NewPartialJob(uuid.NewString(), jobType+":setup", 1000, job.Id)
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

func TestMemoryBackend_ClearProgressDetailsOnSuccessKeptOnFailure(t *testing.T) {
	b := NewMemoryBackend()

	run := func(t *testing.T, fail bool) *model.Job {
		jobType := uniqueType("pd-clear")
		job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
		job.Progress.Details = map[string]any{"some": "detail"}
		if err := b.PushJob(job); err != nil {
			t.Fatalf("PushJob: %v", err)
		}

		worker := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, job.Id)
		worker.Progress.Total = 1
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
		if err := b.StartJob(job.Id); err != nil {
			t.Fatalf("StartJob: %v", err)
		}
		if fail {
			if err := b.Error(taken.Id, "boom", false); err != nil {
				t.Fatalf("Error: %v", err)
			}
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
		t.Errorf("success: expected progressDetails preserved, got %s", success.Progress.Details)
	}

	failure := run(t, true)
	if failure.Progress.Details == nil || failure.Progress.Details["some"] != "detail" {
		t.Errorf("failure: expected progressDetails preserved, got %s", failure.Progress.Details)
	}
}

// TestMemoryBackend_RetriedThenSucceededPartialJobDoesNotFailJob is a
// regression test mirroring the Redis one: a PartialJob that fails a couple
// of times (retried) but eventually succeeds must not drag its transient
// retry-attempt messages into the Job's permanent error list.
func TestMemoryBackend_RetriedThenSucceededPartialJobDoesNotFailJob(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("retry-then-succeed")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	worker := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, job.Id)
	worker.Progress.Total = 1
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

// TestMemoryBackend_ConcurrentUpdateDoesNotLoseProgress is the in-memory
// counterpart to TestConcurrentUpdateJobDoesNotLoseProgress in
// concurrency_test.go - many goroutines calling UpdatePartialJob/UpdateJob
// for different PartialJobs of the same Job truly concurrently. Run with
// -race to also check for data races, not just lost updates.
func TestMemoryBackend_ConcurrentUpdateDoesNotLoseProgress(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("concurrent-update")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	const numPartials = 40
	ids := make([]string, numPartials)
	for i := 0; i < numPartials; i++ {
		pj := NewPartialJob(uuid.NewString(), jobType, 1000, job.Id)
		pj.Progress.Total = 1
		ids[i] = pj.Id
		if err := b.InitJob(job.Id, pj.Progress.Total, nil); err != nil {
			t.Fatalf("InitJob: %v", err)
		}
		if err := b.PushPartialJob(pj, false); err != nil {
			t.Fatalf("PushPartialJob: %v", err)
		}
	}

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if err := b.UpdatePartialJob(id, 1); err != nil {
				t.Errorf("UpdatePartialJob(%s): %v", id, err)
			}
		}(id)
	}
	wg.Wait()

	got, err := b.GetJob(job.Id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Progress.Current != numPartials {
		t.Errorf("Current = %d, want %d - an update was lost under concurrency", got.Progress.Current, numPartials)
	}
}

// TestMemoryBackend_ConcurrentDoneOnlyPushesCleanupOnce mirrors
// TestConcurrentDoneOnlyFinalizesOnce: many PartialJobs of the same Job
// finish concurrently, and the cleanup PartialJob must be pushed exactly
// once despite every goroutine racing to be the one that observes
// current==total.
func TestMemoryBackend_ConcurrentDoneOnlyPushesCleanupOnce(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("concurrent-finalize")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.Cleanup = NewPartialJob(uuid.NewString(), jobType+":cleanup", 1000, job.Id)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	if err := b.StartJob(job.Id); err != nil {
		t.Fatalf("StartJob: %v", err)
	}

	// Each worker gets its own type so each goroutine's Take() deterministically
	// returns its own partial job (matching TestConcurrentDoneOnlyFinalizesOnce
	// in concurrency_test.go) - Done() is a no-op unless the item was actually
	// Take()n first (s. TestMemoryBackend_DoneOnUnknownIsNoop), so a real
	// processor always goes through Take().
	const numPartials = 25
	types := make([]string, numPartials)
	for i := 0; i < numPartials; i++ {
		types[i] = jobType + ":worker-" + strconv.Itoa(i)
		pj := NewPartialJob(uuid.NewString(), types[i], 1000, job.Id)
		pj.Progress.Total = 1
		if err := b.InitJob(job.Id, pj.Progress.Total, nil); err != nil {
			t.Fatalf("InitJob: %v", err)
		}
		if err := b.PushPartialJob(pj, false); err != nil {
			t.Fatalf("PushPartialJob: %v", err)
		}
	}

	var wg sync.WaitGroup
	for _, pjType := range types {
		wg.Add(1)
		go func(pjType string) {
			defer wg.Done()
			taken, err := b.Take(pjType, "test")
			if err != nil || taken == nil {
				t.Errorf("Take: %v, %+v", err, taken)
				return
			}
			if err := b.UpdatePartialJob(taken.Id, 1); err != nil {
				t.Errorf("UpdatePartialJob: %v", err)
				return
			}
			if err := b.Done(taken.Id); err != nil {
				t.Errorf("Done: %v", err)
			}
		}(pjType)
	}
	wg.Wait()

	if len(b.queues[jobType+":cleanup"][job.Cleanup.Priority]) != 1 {
		t.Fatalf("expected exactly 1 queued cleanup partial job, got %d",
			len(b.queues[jobType+":cleanup"][job.Cleanup.Priority]))
	}
}

// TestMemoryBackend_WithRunner proves the Runner works transparently
// against MemoryBackend, not just RedisBackend - same scenario as
// TestRunner_DispatchesToRegisteredProcessor in runner_test.go.
func TestMemoryBackend_WithRunner(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("dispatch")

	partialJob := NewPartialJob(uuid.NewString(), jobType, 1000, "")
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
	if b.partial[partialJob.Id] != nil {
		t.Error("expected partial job to be deleted (Done()) after successful processing")
	}
}

// TestMemoryBackend_SequentialPartialJobsGatedByCurrentSequence exercises
// the shape a workflow-wrapping pipeline uses: one PartialJob per step,
// Parallel=false, so each step only becomes takeable once the previous
// one's Done() has advanced Job.CurrentSequence.
func TestMemoryBackend_SequentialPartialJobsGatedByCurrentSequence(t *testing.T) {
	b := NewMemoryBackend()

	job := NewJob(uuid.NewString(), uniqueType("sequential"), 1000, "", nil)
	job.Sequence = &model.JobSequence{
		Current:   0,
		Remaining: 0,
	}
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
		t.Errorf("CurrentSequence = %d, want 1 after step0 finished", got.Sequence.Current)
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
		t.Errorf("CurrentSequence = %d, want 2 after step1 finished", got.Sequence.Current)
	}

	taken2, err := b.Take(step2Type, "test")
	if err != nil || taken2 == nil || taken2.Id != step2.Id {
		t.Fatalf("Take(step2): %v, %+v", err, taken2)
	}
}

// TestMemoryBackend_SequenceAdvancesOnlyWhenAllItsPartialJobsAreDone covers
// fan-out within a single Sequence (more than one PartialJob sharing the
// same Sequence, e.g. "process these files in parallel, but only after
// step 1 completes") - the next Sequence must stay gated until every
// PartialJob of the current one has finished, not just one of them.
func TestMemoryBackend_SequenceAdvancesOnlyWhenAllItsPartialJobsAreDone(t *testing.T) {
	b := NewMemoryBackend()

	job := NewJob(uuid.NewString(), uniqueType("fanout-sequence"), 1000, "", nil)
	job.Sequence = &model.JobSequence{
		Current:   0,
		Remaining: 0,
	}
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	step0Type := job.Kind + ":step0"
	step1Type := job.Kind + ":step1"

	step0a := NewPartialJob(uuid.NewString(), step0Type, 1000, job.Id)
	step0b := NewPartialJob(uuid.NewString(), step0Type, 1000, job.Id)
	step1 := NewPartialJob(uuid.NewString(), step1Type, 1000, job.Id)

	for _, pj := range []*model.PartialJob{step0a, step0b, step1} {
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

// TestMemoryBackend_ParallelJobIgnoresSequenceGating confirms Sequence is a
// no-op unless the parent Job explicitly opts into Parallel=false.
func TestMemoryBackend_ParallelJobIgnoresSequenceGating(t *testing.T) {
	b := NewMemoryBackend()

	job := NewJob(uuid.NewString(), uniqueType("parallel-with-sequence"), 1000, "", nil)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	late := NewPartialJob(uuid.NewString(), job.Kind+":late", 1000, job.Id)
	seq := 5
	late.Sequence = &seq // would never be "its turn" under gating (CurrentSequence starts at 0)
	if err := b.PushPartialJob(late, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	taken, err := b.Take(late.Kind, "test")
	if err != nil || taken == nil || taken.Id != late.Id {
		t.Fatalf("expected Parallel=true to ignore Sequence gating entirely, got %v, %+v", err, taken)
	}
}

// TestMemoryBackend_PercentIsStoredAndKeptUpToDate is the in-memory
// counterpart to TestRedisBackend_PercentIsStoredAndKeptUpToDate.
func TestMemoryBackend_PercentIsStoredAndKeptUpToDate(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("percent")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	worker := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, job.Id)
	worker.Progress.Total = 4
	if err := b.PushPartialJob(worker, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	assertStoredPercent(t, b, job.Id, worker.Kind)

	standalone := NewJob(uuid.NewString(), jobType+":standalone", 1000, "", nil)
	if err := b.PushJob(standalone); err != nil {
		t.Fatalf("PushJob(standalone): %v", err)
	}
	assertStoredPercentWithoutPartialJobs(t, b, standalone.Id)
}
