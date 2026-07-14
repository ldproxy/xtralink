package jobs

import (
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMemoryBackend_PushJobAndGetJob(t *testing.T) {
	b := NewMemoryBackend()

	job := NewJob(uuid.NewString(), "demo", 1000, "Label", json.RawMessage(`{"a":1}`))
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	got, err := b.GetJob(job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got == nil || got.Label != "Label" || got.Type != "demo" {
		t.Errorf("unexpected job: %+v", got)
	}
	if string(got.Inputs) != `{"a":1}` {
		t.Errorf("Inputs = %s, want {\"a\":1}", got.Inputs)
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
	job.Setup = NewPartialJob(uuid.NewString(), jobType+":setup", 1000, job.ID)

	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	taken, err := b.Take(jobType+":setup", "test-executor")
	if err != nil {
		t.Fatalf("Take: %v", err)
	}
	if taken == nil || taken.ID != job.Setup.ID {
		t.Fatalf("expected to take the auto-pushed setup partial job, got %+v", taken)
	}
	if taken.Executor == nil || *taken.Executor != "test-executor" {
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
	if taken == nil || taken.ID != high.ID {
		t.Fatalf("expected higher-priority partial job first, got %+v", taken)
	}

	taken2, err := b.Take(jobType, "test")
	if err != nil {
		t.Fatalf("Take (2): %v", err)
	}
	if taken2 == nil || taken2.ID != low.ID {
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

	if err := b.Done(taken.ID); err != nil {
		t.Fatalf("Done: %v", err)
	}

	stillTaken, err := b.GetTaken()
	if err != nil {
		t.Fatalf("GetTaken: %v", err)
	}
	for _, pj := range stillTaken {
		if pj.ID == taken.ID {
			t.Error("expected partial job to be removed from the taken list")
		}
	}
	if b.partial[taken.ID] != nil {
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

	id := partialJob.ID
	for i := 0; i < maxRetries; i++ {
		taken, err := b.Take(jobType, "test")
		if err != nil || taken == nil {
			t.Fatalf("Take (attempt %d): %v, %+v", i+1, err, taken)
		}
		if err := b.Error(taken.ID, "transient", true); err != nil {
			t.Fatalf("Error (attempt %d): %v", i+1, err)
		}
	}

	taken, err := b.Take(jobType, "test")
	if err != nil || taken == nil {
		t.Fatalf("Take (final): %v, %+v", err, taken)
	}
	if err := b.Error(taken.ID, "final failure", true); err != nil {
		t.Fatalf("Error (final): %v", err)
	}

	failed, err := b.GetFailed()
	if err != nil {
		t.Fatalf("GetFailed: %v", err)
	}
	found := false
	for _, pj := range failed {
		if pj.ID == id {
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

	if err := b.InitJob(job.ID, 5, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	if err := b.InitJob(job.ID, 3, nil); err != nil {
		t.Fatalf("InitJob (2): %v", err)
	}

	got, err := b.GetJob(job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Total != 8 {
		t.Errorf("Total = %d, want 8", got.Total)
	}
}

func TestMemoryBackend_UpdateJobAppliesProgressUpdatesToProgressDetails(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("update-progress")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.ProgressDetails = json.RawMessage(`{"nested":{"count":0}}`)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	updates := []ProgressUpdate{{Path: "nested.count", Op: ProgressOpAdd}}
	if err := b.UpdateJob(job.ID, 4, updates); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	got, err := b.GetJob(job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Current != 4 {
		t.Errorf("Current = %d, want 4", got.Current)
	}

	var details struct {
		Nested struct {
			Count int `json:"count"`
		} `json:"nested"`
	}
	if err := json.Unmarshal(got.ProgressDetails, &details); err != nil {
		t.Fatalf("unmarshal progressDetails: %v", err)
	}
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
	job.ProgressDetails = json.RawMessage(`{"remaining":10,"levels":{"demo":[-1,8,-1]}}`)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	partialJob := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, job.ID)
	partialJob.Total = 5
	partialJob.UpdateTargets = []ProgressUpdate{
		{Path: "remaining", Op: ProgressOpSubtract},
		{Path: "levels.demo[1]", Op: ProgressOpSubtract},
	}
	if err := b.InitJob(job.ID, 5, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	if err := b.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	if err := b.UpdatePartialJob(partialJob.ID, 3); err != nil {
		t.Fatalf("UpdatePartialJob: %v", err)
	}

	gotPartial := b.partial[partialJob.ID]
	if gotPartial.Current != 3 {
		t.Errorf("partialJob.Current = %d, want 3", gotPartial.Current)
	}

	gotJob, err := b.GetJob(job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if gotJob.Current != 3 {
		t.Errorf("job.Current = %d, want 3 (fanned out from partial job delta)", gotJob.Current)
	}
	var details struct {
		Remaining int `json:"remaining"`
		Levels    struct {
			Demo []int `json:"demo"`
		} `json:"levels"`
	}
	if err := json.Unmarshal(gotJob.ProgressDetails, &details); err != nil {
		t.Fatalf("unmarshal progressDetails: %v", err)
	}
	if details.Remaining != 7 {
		t.Errorf("progressDetails.remaining = %d, want 7 (10-3)", details.Remaining)
	}
	if details.Levels.Demo[1] != 5 {
		t.Errorf("progressDetails.levels.demo[1] = %d, want 5 (8-3)", details.Levels.Demo[1])
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
	job.Setup = NewPartialJob(uuid.NewString(), jobType+":setup", 1000, job.ID)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	taken, err := b.Take(jobType+":setup", "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}
	if err := b.Done(taken.ID); err != nil {
		t.Fatalf("Done: %v", err)
	}

	got, err := b.GetJob(job.ID)
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
	job.Cleanup = NewPartialJob(uuid.NewString(), jobType+":cleanup", 1000, job.ID)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	worker := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, job.ID)
	worker.Total = 1
	if err := b.InitJob(job.ID, 1, nil); err != nil {
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
	if err := b.StartJob(job.ID); err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	if err := b.UpdatePartialJob(taken.ID, 1); err != nil {
		t.Fatalf("UpdatePartialJob: %v", err)
	}
	if err := b.UpdateJob(job.ID, 1, nil); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if err := b.Done(taken.ID); err != nil {
		t.Fatalf("Done: %v", err)
	}

	got, err := b.GetJob(job.ID)
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
	if cleanupTaken == nil || cleanupTaken.ID != job.Cleanup.ID {
		t.Fatalf("expected cleanup partial job to have been pushed automatically, got %+v", cleanupTaken)
	}
}

func TestMemoryBackend_OnPartialJobDone_CleanupFinishing_ClearsProgressDetailsAndPushesFollowUps(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("cleanup-done")

	followUp := NewJob(uuid.NewString(), jobType+"-followup", 1000, "", nil)

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.ProgressDetails = json.RawMessage(`{"some":"detail"}`)
	job.Cleanup = NewPartialJob(uuid.NewString(), jobType+":cleanup", 1000, job.ID)
	job.FollowUps = []*Job{followUp}

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
	if err := b.Done(taken.ID); err != nil {
		t.Fatalf("Done: %v", err)
	}

	got, err := b.GetJob(job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if string(got.ProgressDetails) != "null" {
		t.Errorf("expected progressDetails cleared to null after successful cleanup, got %s", got.ProgressDetails)
	}

	pushedFollowUp, err := b.GetJob(followUp.ID)
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

	okPartial := NewPartialJob(uuid.NewString(), jobType+":ok", 1000, job.ID)
	okPartial.Total = 3
	badPartial := NewPartialJob(uuid.NewString(), jobType+":bad", 1000, job.ID)
	badPartial.Total = 5

	for _, pj := range []*PartialJob{okPartial, badPartial} {
		if err := b.InitJob(job.ID, pj.Total, nil); err != nil {
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
	if err := b.StartJob(job.ID); err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	if err := b.UpdatePartialJob(okTaken.ID, 3); err != nil {
		t.Fatalf("UpdatePartialJob(ok): %v", err)
	}
	if err := b.UpdateJob(job.ID, 3, nil); err != nil {
		t.Fatalf("UpdateJob(ok): %v", err)
	}
	if err := b.Done(okTaken.ID); err != nil {
		t.Fatalf("Done(ok): %v", err)
	}

	badTaken, err := b.Take(jobType+":bad", "test")
	if err != nil || badTaken == nil {
		t.Fatalf("Take(bad): %v, %+v", err, badTaken)
	}
	if err := b.UpdatePartialJob(badTaken.ID, 2); err != nil {
		t.Fatalf("UpdatePartialJob(bad): %v", err)
	}
	if err := b.UpdateJob(job.ID, 2, nil); err != nil {
		t.Fatalf("UpdateJob(bad): %v", err)
	}
	if err := b.Error(badTaken.ID, "boom", false); err != nil {
		t.Fatalf("Error(bad): %v", err)
	}

	final, err := b.GetJob(job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if final.Total != 5 {
		t.Errorf("Total = %d, want 5", final.Total)
	}
	if final.Current != 5 {
		t.Errorf("Current = %d, want 5", final.Current)
	}
	if final.Status() != StatusFailed {
		t.Errorf("Status() = %s, want failed", final.Status())
	}
}

func TestMemoryBackend_OnPartialJobPermanentlyFailed_SetupForcesJobFailed(t *testing.T) {
	b := NewMemoryBackend()
	jobType := uniqueType("permfail-setup")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	job.Setup = NewPartialJob(uuid.NewString(), jobType+":setup", 1000, job.ID)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	taken, err := b.Take(jobType+":setup", "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}
	if err := b.Error(taken.ID, "setup exploded", false); err != nil {
		t.Fatalf("Error: %v", err)
	}

	final, err := b.GetJob(job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if final.FinishedAt <= 0 {
		t.Fatal("expected Job to be forced to a finished state when setup fails permanently")
	}
	if final.Status() != StatusFailed {
		t.Errorf("Status() = %s, want failed", final.Status())
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

	run := func(t *testing.T, fail bool) *Job {
		jobType := uniqueType("pd-clear")
		job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
		job.ProgressDetails = json.RawMessage(`{"some":"detail"}`)
		if err := b.PushJob(job); err != nil {
			t.Fatalf("PushJob: %v", err)
		}

		worker := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, job.ID)
		worker.Total = 1
		if err := b.InitJob(job.ID, 1, nil); err != nil {
			t.Fatalf("InitJob: %v", err)
		}
		if err := b.PushPartialJob(worker, false); err != nil {
			t.Fatalf("PushPartialJob: %v", err)
		}

		taken, err := b.Take(worker.Type, "test")
		if err != nil || taken == nil {
			t.Fatalf("Take: %v, %+v", err, taken)
		}
		if err := b.StartJob(job.ID); err != nil {
			t.Fatalf("StartJob: %v", err)
		}
		if fail {
			if err := b.Error(taken.ID, "boom", false); err != nil {
				t.Fatalf("Error: %v", err)
			}
		} else {
			if err := b.UpdatePartialJob(taken.ID, 1); err != nil {
				t.Fatalf("UpdatePartialJob: %v", err)
			}
			if err := b.UpdateJob(job.ID, 1, nil); err != nil {
				t.Fatalf("UpdateJob: %v", err)
			}
			if err := b.Done(taken.ID); err != nil {
				t.Fatalf("Done: %v", err)
			}
		}

		final, err := b.GetJob(job.ID)
		if err != nil {
			t.Fatalf("GetJob: %v", err)
		}
		return final
	}

	success := run(t, false)
	if string(success.ProgressDetails) != "null" {
		t.Errorf("success: expected progressDetails=null, got %s", success.ProgressDetails)
	}

	failure := run(t, true)
	if string(failure.ProgressDetails) != `{"some":"detail"}` {
		t.Errorf("failure: expected progressDetails preserved, got %s", failure.ProgressDetails)
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

	worker := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, job.ID)
	worker.Total = 1
	if err := b.InitJob(job.ID, 1, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	if err := b.PushPartialJob(worker, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	for i := 0; i < 2; i++ {
		taken, err := b.Take(worker.Type, "test")
		if err != nil || taken == nil {
			t.Fatalf("Take (retry %d): %v, %+v", i+1, err, taken)
		}
		if err := b.Error(taken.ID, "transient", true); err != nil {
			t.Fatalf("Error (retry %d): %v", i+1, err)
		}
	}

	taken, err := b.Take(worker.Type, "test")
	if err != nil || taken == nil {
		t.Fatalf("Take (final): %v, %+v", err, taken)
	}
	if err := b.StartJob(job.ID); err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	if err := b.UpdatePartialJob(taken.ID, 1); err != nil {
		t.Fatalf("UpdatePartialJob: %v", err)
	}
	if err := b.UpdateJob(job.ID, 1, nil); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if err := b.Done(taken.ID); err != nil {
		t.Fatalf("Done: %v", err)
	}

	final, err := b.GetJob(job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if final.Status() != StatusSuccessful {
		t.Errorf("Status() = %s, want successful (errors=%v)", final.Status(), final.Errors)
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
		pj := NewPartialJob(uuid.NewString(), jobType, 1000, job.ID)
		pj.Total = 1
		ids[i] = pj.ID
		if err := b.InitJob(job.ID, 1, nil); err != nil {
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
			if err := b.UpdateJob(job.ID, 1, nil); err != nil {
				t.Errorf("UpdateJob: %v", err)
			}
		}(id)
	}
	wg.Wait()

	got, err := b.GetJob(job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Current != numPartials {
		t.Errorf("Current = %d, want %d - an update was lost under concurrency", got.Current, numPartials)
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
	job.Cleanup = NewPartialJob(uuid.NewString(), jobType+":cleanup", 1000, job.ID)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	if err := b.StartJob(job.ID); err != nil {
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
		pj := NewPartialJob(uuid.NewString(), types[i], 1000, job.ID)
		pj.Total = 1
		if err := b.InitJob(job.ID, 1, nil); err != nil {
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
			if err := b.UpdatePartialJob(taken.ID, 1); err != nil {
				t.Errorf("UpdatePartialJob: %v", err)
				return
			}
			if err := b.UpdateJob(job.ID, 1, nil); err != nil {
				t.Errorf("UpdateJob: %v", err)
				return
			}
			if err := b.Done(taken.ID); err != nil {
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
	r.Register(&funcProcessor{jobType: jobType, priority: 1000, process: func(*PartialJob, *Job, Backend) JobResult {
		atomic.AddInt32(&processed, 1)
		return Success()
	}})

	runRunnerUntil(t, r, 2*time.Second, func() bool { return atomic.LoadInt32(&processed) > 0 })

	if atomic.LoadInt32(&processed) != 1 {
		t.Errorf("expected the partial job to be processed exactly once, got %d", processed)
	}
	if b.partial[partialJob.ID] != nil {
		t.Error("expected partial job to be deleted (Done()) after successful processing")
	}
}
