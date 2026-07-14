package jobs

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestRedisBackend_PushJobSetAndGetSet(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("push-set")

	js := NewJob(uuid.NewString(), jobType, 1000, "Label", json.RawMessage(`{"a":1}`))
	cleanupJob(t, b, js.ID)

	if err := b.PushJob(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}

	got, err := b.GetJob(js.ID)
	if err != nil {
		t.Fatalf("GetSet: %v", err)
	}
	if got == nil {
		t.Fatal("expected job set to exist")
	}
	if got.Label != "Label" || got.Type != jobType {
		t.Errorf("unexpected job set: %+v", got)
	}
	if string(got.Inputs) != `{"a":1}` {
		t.Errorf("Inputs = %s, want {\"a\":1}", got.Inputs)
	}
}

func TestRedisBackend_GetSetReturnsNilForUnknownID(t *testing.T) {
	b := requireRedis(t)

	got, err := b.GetJob(uuid.NewString())
	if err != nil {
		t.Fatalf("GetSet: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for unknown id, got %+v", got)
	}
}

func TestRedisBackend_PushJobSetAutoPushesSetup(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("auto-setup")

	js := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	js.Setup = NewPartialJob(uuid.NewString(), jobType+":setup", 1000, js.ID)
	cleanupJob(t, b, js.ID)
	cleanupPartialJob(t, b, js.Setup.ID)

	if err := b.PushJob(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}

	taken, err := b.Take(jobType+":setup", "test-executor")
	if err != nil {
		t.Fatalf("Take: %v", err)
	}
	if taken == nil || taken.ID != js.Setup.ID {
		t.Fatalf("expected to take the auto-pushed setup job, got %+v", taken)
	}
	if taken.Executor == nil || *taken.Executor != "test-executor" {
		t.Error("expected Executor to be set on take")
	}
	if taken.StartedAt <= 0 {
		t.Error("expected StartedAt to be set on take")
	}
}

func TestRedisBackend_PushJobSetWithoutSetupEnqueuesNothing(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("no-setup")

	js := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	cleanupJob(t, b, js.ID)

	if err := b.PushJob(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}

	taken, err := b.Take(jobType, "test")
	if err != nil {
		t.Fatalf("Take: %v", err)
	}
	if taken != nil {
		t.Errorf("expected nothing to be queued for a JobSet without Setup, got %+v", taken)
	}
}

func TestRedisBackend_TakeReturnsHighestPriorityFirst(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("priority")

	low := NewPartialJob(uuid.NewString(), jobType, 100, "")
	high := NewPartialJob(uuid.NewString(), jobType, 900, "")
	cleanupPartialJob(t, b, low.ID)
	cleanupPartialJob(t, b, high.ID)

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
		t.Fatalf("expected higher-priority job first, got %+v", taken)
	}

	taken2, err := b.Take(jobType, "test")
	if err != nil {
		t.Fatalf("Take (2): %v", err)
	}
	if taken2 == nil || taken2.ID != low.ID {
		t.Fatalf("expected lower-priority job second, got %+v", taken2)
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

func TestRedisBackend_DoneRemovesFromTakenAndDeletesJob(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("done")

	job := NewPartialJob(uuid.NewString(), jobType, 1000, "")
	cleanupPartialJob(t, b, job.ID)
	if err := b.PushPartialJob(job, false); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	taken, err := b.Take(jobType, "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}

	if err := b.Done(taken.ID); err != nil {
		t.Fatalf("Done: %v", err)
	}

	ctx := context.Background()
	takenIDs, err := b.client.LRange(ctx, keyTaken, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange: %v", err)
	}
	for _, id := range takenIDs {
		if id == taken.ID {
			t.Error("expected job to be removed from taken list")
		}
	}
	if got, err := b.getPartialJob(ctx, taken.ID); err != nil {
		t.Fatalf("getPartialJob: %v", err)
	} else if got != nil {
		t.Errorf("expected partial job document to be deleted after Done(), got %+v", got)
	}
}

func TestRedisBackend_DoneOnUnknownJobIsNoop(t *testing.T) {
	b := requireRedis(t)
	if err := b.Done(uuid.NewString()); err != nil {
		t.Errorf("expected no error for an unknown job id, got %v", err)
	}
}

func TestRedisBackend_ErrorRetriesThenPermanentlyFails(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("error-exhaust")

	job := NewPartialJob(uuid.NewString(), jobType, 1000, "")
	cleanupPartialJob(t, b, job.ID)
	if err := b.PushPartialJob(job, false); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	id := job.ID
	for i := 0; i < maxRetries; i++ {
		taken, err := b.Take(jobType, "test")
		if err != nil || taken == nil {
			t.Fatalf("Take (attempt %d): %v, %+v", i+1, err, taken)
		}
		if err := b.Error(taken.ID, "transient", true); err != nil {
			t.Fatalf("Error (attempt %d): %v", i+1, err)
		}
	}

	// One more failure exceeds maxRetries and should be permanent.
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
	for _, j := range failed {
		if j.ID == id {
			found = true
			if len(j.Errors) != maxRetries+1 {
				t.Errorf("expected %d accumulated error messages, got %d: %v", maxRetries+1, len(j.Errors), j.Errors)
			}
		}
	}
	if !found {
		t.Errorf("expected job %s in failed list", id)
	}
	t.Cleanup(func() { b.client.LRem(context.Background(), keyFailed, 0, id) })
}

func TestRedisBackend_InitJobSetGrowsTotal(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("init-total")

	js := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	cleanupJob(t, b, js.ID)
	if err := b.PushJob(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}

	if err := b.InitJob(js.ID, 5, nil); err != nil {
		t.Fatalf("InitJobSet: %v", err)
	}
	if err := b.InitJob(js.ID, 3, nil); err != nil {
		t.Fatalf("InitJobSet (2): %v", err)
	}

	got, err := b.GetJob(js.ID)
	if err != nil {
		t.Fatalf("GetSet: %v", err)
	}
	if got.Total != 8 {
		t.Errorf("Total = %d, want 8", got.Total)
	}
}

func TestRedisBackend_UpdateJobSetAppliesProgressUpdatesToProgressDetails(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("update-set-progress")

	js := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	js.ProgressDetails = json.RawMessage(`{"nested":{"count":0}}`)
	cleanupJob(t, b, js.ID)
	if err := b.PushJob(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}

	updates := []ProgressUpdate{{Path: "nested.count", Op: ProgressOpAdd}}
	if err := b.UpdateJob(js.ID, 4, updates); err != nil {
		t.Fatalf("UpdateJobSet: %v", err)
	}

	got, err := b.GetJob(js.ID)
	if err != nil {
		t.Fatalf("GetSet: %v", err)
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

func TestRedisBackend_UpdateJobFansOutViaUpdateTargets(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("fanout")

	js := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	js.ProgressDetails = json.RawMessage(`{"remaining":10}`)
	cleanupJob(t, b, js.ID)
	if err := b.PushJob(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}

	job := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, js.ID)
	job.Total = 5
	job.UpdateTargets = []ProgressUpdate{{Path: "remaining", Op: ProgressOpSubtract}}
	cleanupPartialJob(t, b, job.ID)
	if err := b.InitJob(js.ID, 5, nil); err != nil {
		t.Fatalf("InitJobSet: %v", err)
	}
	if err := b.PushPartialJob(job, false); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	if err := b.UpdatePartialJob(job.ID, 3); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	gotJob, err := b.getPartialJob(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("getPartialJob: %v", err)
	}
	if gotJob.Current != 3 {
		t.Errorf("job.Current = %d, want 3", gotJob.Current)
	}

	gotSet, err := b.GetJob(js.ID)
	if err != nil {
		t.Fatalf("GetSet: %v", err)
	}
	if gotSet.Current != 3 {
		t.Errorf("jobSet.Current = %d, want 3 (fanned out from job delta)", gotSet.Current)
	}
	var details struct {
		Remaining int `json:"remaining"`
	}
	if err := json.Unmarshal(gotSet.ProgressDetails, &details); err != nil {
		t.Fatalf("unmarshal progressDetails: %v", err)
	}
	if details.Remaining != 7 {
		t.Errorf("progressDetails.remaining = %d, want 7 (10-3)", details.Remaining)
	}
}

func TestRedisBackend_OnJobDone_SetupFinishing_SyncsEmbeddedSnapshotOnly(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("setup-done")

	js := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	js.Setup = NewPartialJob(uuid.NewString(), jobType+":setup", 1000, js.ID)
	cleanupJob(t, b, js.ID)
	if err := b.PushJob(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}

	taken, err := b.Take(jobType+":setup", "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}
	if err := b.Done(taken.ID); err != nil {
		t.Fatalf("Done: %v", err)
	}

	got, err := b.GetJob(js.ID)
	if err != nil {
		t.Fatalf("GetSet: %v", err)
	}
	if got.Setup == nil || got.Setup.FinishedAt <= 0 {
		t.Errorf("expected embedded setup snapshot to show finishedAt set, got %+v", got.Setup)
	}
	// Setup finishing must not mark the JobSet itself as finished - only
	// the setup processor (elsewhere) decides what happens next.
	if got.FinishedAt > 0 {
		t.Errorf("expected JobSet.FinishedAt to remain unset after setup alone finishes, got %d", got.FinishedAt)
	}
}

func TestRedisBackend_OnJobDone_LastSubJobFinalizesAndPushesCleanup(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("finalize-cleanup")

	js := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	js.Cleanup = NewPartialJob(uuid.NewString(), jobType+":cleanup", 1000, js.ID)
	cleanupJob(t, b, js.ID)
	if err := b.PushJob(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}

	job := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, js.ID)
	job.Total = 1
	cleanupPartialJob(t, b, job.ID)
	if err := b.InitJob(js.ID, 1, nil); err != nil {
		t.Fatalf("InitJobSet: %v", err)
	}
	if err := b.PushPartialJob(job, false); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	taken, err := b.Take(jobType+":worker", "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}
	// Calling the backend directly here, bypassing the Runner - which is
	// normally what calls StartJobSet for the first non-setup Job taken.
	// Without it, IsStarted() stays false and IsDone() can never be true
	// even once current==total.
	if err := b.StartJob(js.ID); err != nil {
		t.Fatalf("StartJobSet: %v", err)
	}
	// No UpdateTargets on this job, so - like the demo processors that don't
	// need progressDetails - the JobSet's current is grown with a direct
	// UpdateJobSet call instead of relying on UpdateJob's fan-out.
	if err := b.UpdatePartialJob(taken.ID, 1); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if err := b.UpdateJob(js.ID, 1, nil); err != nil {
		t.Fatalf("UpdateJobSet: %v", err)
	}
	if err := b.Done(taken.ID); err != nil {
		t.Fatalf("Done: %v", err)
	}

	got, err := b.GetJob(js.ID)
	if err != nil {
		t.Fatalf("GetSet: %v", err)
	}
	if got.FinishedAt <= 0 {
		t.Fatal("expected JobSet.FinishedAt to be set once the last sub-Job finishes")
	}

	cleanupTaken, err := b.Take(jobType+":cleanup", "test")
	if err != nil {
		t.Fatalf("Take (cleanup): %v", err)
	}
	if cleanupTaken == nil || cleanupTaken.ID != js.Cleanup.ID {
		t.Fatalf("expected cleanup job to have been pushed automatically, got %+v", cleanupTaken)
	}
	cleanupPartialJob(t, b, cleanupTaken.ID)
}

func TestRedisBackend_OnJobDone_CleanupFinishing_ClearsProgressDetailsAndPushesFollowUps(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("cleanup-done")

	followUp := NewJob(uuid.NewString(), jobType+"-followup", 1000, "", nil)

	js := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	js.ProgressDetails = json.RawMessage(`{"some":"detail"}`)
	js.Cleanup = NewPartialJob(uuid.NewString(), jobType+":cleanup", 1000, js.ID)
	js.FollowUps = []*Job{followUp}
	cleanupJob(t, b, js.ID)
	cleanupJob(t, b, followUp.ID)

	if err := b.PushJob(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}
	if err := b.PushPartialJob(js.Cleanup, false); err != nil {
		t.Fatalf("PushPartialJob(cleanup): %v", err)
	}

	taken, err := b.Take(jobType+":cleanup", "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}
	if err := b.Done(taken.ID); err != nil {
		t.Fatalf("Done: %v", err)
	}

	got, err := b.GetJob(js.ID)
	if err != nil {
		t.Fatalf("GetSet: %v", err)
	}
	if string(got.ProgressDetails) != "null" {
		t.Errorf("expected progressDetails cleared to null after successful cleanup, got %s", got.ProgressDetails)
	}

	pushedFollowUp, err := b.GetJob(followUp.ID)
	if err != nil {
		t.Fatalf("GetSet(followUp): %v", err)
	}
	if pushedFollowUp == nil {
		t.Error("expected followUp job set to have been pushed once cleanup finished")
	}
}

func TestRedisBackend_OnJobPermanentlyFailed_ReducesTotalAndFinalizes(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("permfail-total")

	js := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	cleanupJob(t, b, js.ID)
	if err := b.PushJob(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}

	okJob := NewPartialJob(uuid.NewString(), jobType+":ok", 1000, js.ID)
	okJob.Total = 3
	badJob := NewPartialJob(uuid.NewString(), jobType+":bad", 1000, js.ID)
	badJob.Total = 5
	cleanupPartialJob(t, b, okJob.ID)
	cleanupPartialJob(t, b, badJob.ID)

	for _, j := range []*PartialJob{okJob, badJob} {
		if err := b.InitJob(js.ID, j.Total, nil); err != nil {
			t.Fatalf("InitJobSet: %v", err)
		}
		if err := b.PushPartialJob(j, false); err != nil {
			t.Fatalf("PushJob: %v", err)
		}
	}

	okTaken, err := b.Take(jobType+":ok", "test")
	if err != nil || okTaken == nil {
		t.Fatalf("Take(ok): %v, %+v", err, okTaken)
	}
	// Bypassing the Runner here, which normally calls this automatically.
	if err := b.StartJob(js.ID); err != nil {
		t.Fatalf("StartJobSet: %v", err)
	}
	if err := b.UpdatePartialJob(okTaken.ID, 3); err != nil {
		t.Fatalf("UpdatePartialJob(ok): %v", err)
	}
	if err := b.UpdateJob(js.ID, 3, nil); err != nil {
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
	if err := b.UpdateJob(js.ID, 2, nil); err != nil {
		t.Fatalf("UpdateJob(bad): %v", err)
	}
	if err := b.Error(badTaken.ID, "boom", false); err != nil {
		t.Fatalf("Error(bad): %v", err)
	}
	t.Cleanup(func() { b.client.LRem(context.Background(), keyFailed, 0, badTaken.ID) })

	final, err := b.GetJob(js.ID)
	if err != nil {
		t.Fatalf("GetSet: %v", err)
	}
	// total started at 3+5=8; bad job only reached 2 of 5, so its
	// remaining 3 must have been subtracted: total should end at 5,
	// matching current=3(ok)+2(bad)=5.
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

func TestRedisBackend_OnJobPermanentlyFailed_SetupForcesJobSetFailed(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("permfail-setup")

	js := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	js.Setup = NewPartialJob(uuid.NewString(), jobType+":setup", 1000, js.ID)
	cleanupJob(t, b, js.ID)
	if err := b.PushJob(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}

	taken, err := b.Take(jobType+":setup", "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}
	if err := b.Error(taken.ID, "setup exploded", false); err != nil {
		t.Fatalf("Error: %v", err)
	}
	t.Cleanup(func() { b.client.LRem(context.Background(), keyFailed, 0, taken.ID) })

	final, err := b.GetJob(js.ID)
	if err != nil {
		t.Fatalf("GetSet: %v", err)
	}
	if final.FinishedAt <= 0 {
		t.Fatal("expected JobSet to be forced to a finished state when setup fails permanently")
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
		t.Errorf("expected setup's error message in JobSet.Errors, got %v", final.Errors)
	}
}

func TestRedisBackend_OnJobPermanentlyFailed_CleanupMergesErrorWithoutTouchingTotal(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("permfail-cleanup")

	js := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	js.Cleanup = NewPartialJob(uuid.NewString(), jobType+":cleanup", 1000, js.ID)
	cleanupJob(t, b, js.ID)
	if err := b.PushJob(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}
	// Simulate the set already being done before cleanup runs.
	if err := b.InitJob(js.ID, 2, nil); err != nil {
		t.Fatalf("InitJobSet: %v", err)
	}

	if err := b.PushPartialJob(js.Cleanup, false); err != nil {
		t.Fatalf("PushPartialJob(cleanup): %v", err)
	}
	taken, err := b.Take(jobType+":cleanup", "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}
	if err := b.Error(taken.ID, "cleanup exploded", false); err != nil {
		t.Fatalf("Error: %v", err)
	}
	t.Cleanup(func() { b.client.LRem(context.Background(), keyFailed, 0, taken.ID) })

	final, err := b.GetJob(js.ID)
	if err != nil {
		t.Fatalf("GetSet: %v", err)
	}
	if final.Total != 2 {
		t.Errorf("expected cleanup failure to leave Total untouched, got %d", final.Total)
	}
	found := false
	for _, e := range final.Errors {
		if e == "cleanup exploded" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected cleanup's error message in JobSet.Errors, got %v", final.Errors)
	}
}

func TestRedisBackend_ClearProgressDetailsOnSuccessKeptOnFailure(t *testing.T) {
	b := requireRedis(t)

	run := func(t *testing.T, fail bool) *Job {
		jobType := uniqueType("pd-clear")
		js := NewJob(uuid.NewString(), jobType, 1000, "", nil)
		js.ProgressDetails = json.RawMessage(`{"some":"detail"}`)
		cleanupJob(t, b, js.ID)
		if err := b.PushJob(js); err != nil {
			t.Fatalf("PushJobSet: %v", err)
		}

		job := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, js.ID)
		job.Total = 1
		cleanupPartialJob(t, b, job.ID)
		if err := b.InitJob(js.ID, 1, nil); err != nil {
			t.Fatalf("InitJobSet: %v", err)
		}
		if err := b.PushPartialJob(job, false); err != nil {
			t.Fatalf("PushJob: %v", err)
		}

		taken, err := b.Take(job.Type, "test")
		if err != nil || taken == nil {
			t.Fatalf("Take: %v, %+v", err, taken)
		}
		// Bypassing the Runner here, which normally calls this automatically.
		if err := b.StartJob(js.ID); err != nil {
			t.Fatalf("StartJobSet: %v", err)
		}
		if fail {
			if err := b.Error(taken.ID, "boom", false); err != nil {
				t.Fatalf("Error: %v", err)
			}
			t.Cleanup(func() { b.client.LRem(context.Background(), keyFailed, 0, taken.ID) })
		} else {
			if err := b.UpdatePartialJob(taken.ID, 1); err != nil {
				t.Fatalf("UpdateJob: %v", err)
			}
			if err := b.UpdateJob(js.ID, 1, nil); err != nil {
				t.Fatalf("UpdateJobSet: %v", err)
			}
			if err := b.Done(taken.ID); err != nil {
				t.Fatalf("Done: %v", err)
			}
		}

		final, err := b.GetJob(js.ID)
		if err != nil {
			t.Fatalf("GetSet: %v", err)
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

// TestRedisBackend_RetriedThenSucceededSubJobDoesNotFailJobSet is a
// regression test: a sub-Job that fails a couple of times (retried) but
// eventually succeeds must not drag its transient retry-attempt messages
// into the JobSet's permanent error list - only Error()'s *permanent*
// failure path should ever do that.
func TestRedisBackend_RetriedThenSucceededSubJobDoesNotFailJobSet(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("retry-then-succeed")

	js := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	cleanupJob(t, b, js.ID)
	if err := b.PushJob(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}

	job := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, js.ID)
	job.Total = 1
	cleanupPartialJob(t, b, job.ID)
	if err := b.InitJob(js.ID, 1, nil); err != nil {
		t.Fatalf("InitJobSet: %v", err)
	}
	if err := b.PushPartialJob(job, false); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	for i := 0; i < 2; i++ {
		taken, err := b.Take(job.Type, "test")
		if err != nil || taken == nil {
			t.Fatalf("Take (retry %d): %v, %+v", i+1, err, taken)
		}
		if err := b.Error(taken.ID, "transient", true); err != nil {
			t.Fatalf("Error (retry %d): %v", i+1, err)
		}
	}

	taken, err := b.Take(job.Type, "test")
	if err != nil || taken == nil {
		t.Fatalf("Take (final): %v, %+v", err, taken)
	}
	// Bypassing the Runner here, which normally calls this automatically.
	if err := b.StartJob(js.ID); err != nil {
		t.Fatalf("StartJobSet: %v", err)
	}
	if err := b.UpdatePartialJob(taken.ID, 1); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if err := b.UpdateJob(js.ID, 1, nil); err != nil {
		t.Fatalf("UpdateJobSet: %v", err)
	}
	if err := b.Done(taken.ID); err != nil {
		t.Fatalf("Done: %v", err)
	}

	final, err := b.GetJob(js.ID)
	if err != nil {
		t.Fatalf("GetSet: %v", err)
	}
	if final.Status() != StatusSuccessful {
		t.Errorf("Status() = %s, want successful (errors=%v)", final.Status(), final.Errors)
	}
	if len(final.Errors) != 0 {
		t.Errorf("expected no errors on the JobSet from a job that ultimately succeeded, got %v", final.Errors)
	}
}
