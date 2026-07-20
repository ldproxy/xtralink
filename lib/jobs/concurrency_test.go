package jobs

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

// These tests specifically target the scenario the job queue is built
// around: a single Job is only ever handled by one processor, but many
// Jobs belonging to the same JobSet run concurrently (different
// processors/goroutines), all reporting progress into the same JobSet
// document at once. This is exactly the class of bug found and fixed
// earlier (a full-document overwrite in onJobDone clobbering a concurrent
// JSON.NUMINCRBY) - these tests guard against that regressing.

// TestConcurrentUpdateJobDoesNotLoseProgress fires UpdateJob/UpdateJobSet
// for many different sub-Jobs of the same JobSet truly concurrently and
// checks that JobSet.Current ends up as the exact sum of all deltas - no
// update silently lost to a race.
func TestConcurrentUpdateJobDoesNotLoseProgress(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("concurrent-update")

	js := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	cleanupJob(t, b, js.ID)
	if err := b.PushJob(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}

	const numJobs = 40
	jobIDs := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		j := NewPartialJob(uuid.NewString(), jobType+":worker-"+strconv.Itoa(i), 1000, js.ID)
		j.Total = 1
		jobIDs[i] = j.ID
		cleanupPartialJob(t, b, j.ID)
		if err := b.InitJob(js.ID, 1, nil); err != nil {
			t.Fatalf("InitJobSet: %v", err)
		}
		if err := b.PushPartialJob(j, false); err != nil {
			t.Fatalf("PushJob: %v", err)
		}
	}

	var wg sync.WaitGroup
	for _, id := range jobIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if err := b.UpdatePartialJob(id, 1); err != nil {
				t.Errorf("UpdatePartialJob(%s): %v", id, err)
			}
			if err := b.UpdateJob(js.ID, 1, nil); err != nil {
				t.Errorf("UpdateJobSet: %v", err)
			}
		}(id)
	}
	wg.Wait()

	got, err := b.GetJob(js.ID)
	if err != nil {
		t.Fatalf("GetSet: %v", err)
	}
	if got.Current != numJobs {
		t.Errorf("Current = %d, want %d - an update was lost under concurrency", got.Current, numJobs)
	}
}

// TestConcurrentDoneOnlyFinalizesOnce drives many sub-Jobs of the same
// JobSet to completion truly concurrently and checks that the cleanup Job
// was pushed exactly once - the SETNX guard in finalizeIfDone must let only
// one of the racing goroutines win, even though every one of them observes
// current==total right before finalizing.
func TestConcurrentDoneOnlyFinalizesOnce(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("concurrent-finalize")

	js := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	js.Cleanup = NewPartialJob(uuid.NewString(), jobType+":cleanup", 1000, js.ID)
	cleanupJob(t, b, js.ID)
	cleanupPartialJob(t, b, js.Cleanup.ID)
	if err := b.PushJob(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}
	if err := b.StartJob(js.ID); err != nil {
		t.Fatalf("StartJobSet: %v", err)
	}

	const numJobs = 25
	jobIDs := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		j := NewPartialJob(uuid.NewString(), jobType+":worker-"+strconv.Itoa(i), 1000, js.ID)
		j.Total = 1
		jobIDs[i] = j.ID
		cleanupPartialJob(t, b, j.ID)
		if err := b.InitJob(js.ID, 1, nil); err != nil {
			t.Fatalf("InitJobSet: %v", err)
		}
		if err := b.PushPartialJob(j, false); err != nil {
			t.Fatalf("PushJob: %v", err)
		}
	}

	var wg sync.WaitGroup
	for i, id := range jobIDs {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			// Done() is a no-op unless the job is in the "taken" list (matches
			// TestRedisBackend_DoneOnUnknownJobIsNoop), so it must actually be
			// Take()n first - a real processor would always go through Take().
			jt := jobType + ":worker-" + strconv.Itoa(i)
			taken, err := b.Take(jt, "test")
			if err != nil || taken == nil {
				t.Errorf("Take: %v, %+v", err, taken)
				return
			}
			if err := b.UpdatePartialJob(id, 1); err != nil {
				t.Errorf("UpdateJob: %v", err)
				return
			}
			if err := b.UpdateJob(js.ID, 1, nil); err != nil {
				t.Errorf("UpdateJobSet: %v", err)
				return
			}
			if err := b.Done(id); err != nil {
				t.Errorf("Done: %v", err)
			}
		}(i, id)
	}
	wg.Wait()

	taken, err := b.Take(jobType+":cleanup", "test")
	if err != nil {
		t.Fatalf("Take(cleanup): %v", err)
	}
	if taken == nil || taken.ID != js.Cleanup.ID {
		t.Fatalf("expected cleanup to have been pushed, got %+v", taken)
	}
	cleanupPartialJob(t, b, taken.ID)

	again, err := b.Take(jobType+":cleanup", "test")
	if err != nil {
		t.Fatalf("Take(cleanup) again: %v", err)
	}
	if again != nil {
		t.Errorf("expected cleanup to be pushed exactly once, found a duplicate: %+v", again)
	}
}

// TestManyProcessorsUpdatingSameJobSetConcurrently is the scenario
// described directly: a single Job is only ever processed by one
// processor, but many processors (goroutines, via a real Runner with
// Concurrency > 1) run in parallel and all update the same JobSet. Each
// Job targets a different progressDetails counter via UpdateTargets, and
// every counter must end up exactly correct despite the concurrency.
func TestManyProcessorsUpdatingSameJobSetConcurrently(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("many-processors")

	js := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	js.ProgressDetails = json.RawMessage(`{"counters":{"a":0,"b":0,"c":0}}`)
	// A JobSet with no Cleanup has its progressDetails cleared the instant it
	// finishes successfully (clearProgressDetailsOnSuccess), so a Cleanup
	// step is used here to snapshot progressDetails into an output -
	// matching how a real cleanup processor would surface the final numbers
	// - before that clearing would otherwise erase the evidence this test
	// needs to check.
	js.Cleanup = NewPartialJob(uuid.NewString(), jobType+":cleanup", 1000, js.ID)
	cleanupJob(t, b, js.ID)
	cleanupPartialJob(t, b, js.Cleanup.ID)
	if err := b.PushJob(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}

	const jobsPerCounter = 15
	counters := []string{"a", "b", "c"}
	total := 0
	for _, c := range counters {
		for i := 0; i < jobsPerCounter; i++ {
			job := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, js.ID)
			job.Total = 1
			job.UpdateTargets = []ProgressUpdate{{Path: "counters." + c, Op: ProgressOpAdd}}
			cleanupPartialJob(t, b, job.ID)
			if err := b.InitJob(js.ID, 1, nil); err != nil {
				t.Fatalf("InitJobSet: %v", err)
			}
			if err := b.PushPartialJob(job, false); err != nil {
				t.Fatalf("PushJob: %v", err)
			}
			total++
		}
	}

	var completed int32
	var cleaned int32
	r := NewRunner(b, "test")
	r.Concurrency = 8 // many "processors" running in parallel
	r.PollInterval = 10 * time.Millisecond
	r.Register(&funcProcessor{jobType: jobType + ":worker", priority: 1000, process: func(p *PartialJob, _ *Job, backend Backend) JobResult {
		time.Sleep(5 * time.Millisecond) // simulate a bit of real work
		if err := backend.UpdatePartialJob(p.ID, 1); err != nil {
			return Error(err.Error())
		}
		atomic.AddInt32(&completed, 1)
		return Success()
	}})
	r.Register(&funcProcessor{jobType: jobType + ":cleanup", priority: 1000, process: func(_ *PartialJob, job *Job, backend Backend) JobResult {
		if err := backend.SetOutput(job.ID, "countersSnapshot", OutputValue{Value: json.RawMessage(job.ProgressDetails)}); err != nil {
			return Error(err.Error())
		}
		atomic.AddInt32(&cleaned, 1)
		return Success()
	}})

	runRunnerUntil(t, r, 15*time.Second, func() bool { return atomic.LoadInt32(&cleaned) > 0 })

	if got := int(atomic.LoadInt32(&completed)); got != total {
		t.Fatalf("expected all %d jobs to complete, only %d did", total, got)
	}

	final, err := b.GetJob(js.ID)
	if err != nil {
		t.Fatalf("GetSet: %v", err)
	}
	if final.Current != total {
		t.Errorf("JobSet.Current = %d, want %d - lost updates under concurrency", final.Current, total)
	}

	snapshot, ok := final.Outputs["countersSnapshot"]
	if !ok {
		t.Fatalf("expected countersSnapshot output to have been written by cleanup")
	}
	// snapshot.Value comes back from GetSet's JSON unmarshal as a generic
	// map[string]interface{} (it's declared as `any`) - round-trip it
	// through JSON once more to decode it into a typed struct.
	raw, err := json.Marshal(snapshot.Value)
	if err != nil {
		t.Fatalf("marshal snapshot value: %v", err)
	}
	var details struct {
		Counters map[string]int `json:"counters"`
	}
	if err := json.Unmarshal(raw, &details); err != nil {
		t.Fatalf("unmarshal progressDetails snapshot: %v", err)
	}
	for _, c := range counters {
		if details.Counters[c] != jobsPerCounter {
			t.Errorf("counters.%s = %d, want %d", c, details.Counters[c], jobsPerCounter)
		}
	}
}

// TestConcurrentMixedOutcomesKeepTotalConsistent runs a mix of succeeding
// and permanently failing sub-Jobs concurrently against the same JobSet and
// checks that total/current end up numerically consistent regardless of
// interleaving: successes contribute their full progress, failures
// contribute only what they actually completed (with the rest subtracted
// from total by onJobPermanentlyFailed).
func TestConcurrentMixedOutcomesKeepTotalConsistent(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("mixed-outcomes")

	js := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	cleanupJob(t, b, js.ID)
	if err := b.PushJob(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}
	if err := b.StartJob(js.ID); err != nil {
		t.Fatalf("StartJobSet: %v", err)
	}

	type plan struct {
		total, progress int
		fail            bool
	}
	const numOK = 15
	const numFail = 10
	var plans []plan
	for i := 0; i < numOK; i++ {
		plans = append(plans, plan{total: 2, progress: 2, fail: false})
	}
	for i := 0; i < numFail; i++ {
		plans = append(plans, plan{total: 3, progress: 1, fail: true})
	}

	wantFinal := numOK*2 + numFail*1 // failed jobs' unfinished share gets subtracted from total

	var wg sync.WaitGroup
	for i, p := range plans {
		wg.Add(1)
		go func(i int, p plan) {
			defer wg.Done()

			// Unique type per goroutine so each one's Take() can only ever
			// return its own job - the point under test is concurrent
			// writes to the shared JobSet, not queue contention between
			// unrelated jobs stealing each other's work.
			jt := jobType + ":worker-" + strconv.Itoa(i)
			job := NewPartialJob(uuid.NewString(), jt, 1000, js.ID)
			job.Total = p.total
			cleanupPartialJob(t, b, job.ID)
			if err := b.InitJob(js.ID, p.total, nil); err != nil {
				t.Errorf("InitJobSet: %v", err)
				return
			}
			if err := b.PushPartialJob(job, false); err != nil {
				t.Errorf("PushJob: %v", err)
				return
			}
			taken, err := b.Take(jt, "test")
			if err != nil || taken == nil {
				t.Errorf("Take: %v, %+v", err, taken)
				return
			}
			if err := b.UpdatePartialJob(taken.ID, p.progress); err != nil {
				t.Errorf("UpdateJob: %v", err)
				return
			}
			if err := b.UpdateJob(js.ID, p.progress, nil); err != nil {
				t.Errorf("UpdateJobSet: %v", err)
				return
			}
			if p.fail {
				if err := b.Error(taken.ID, "boom", false); err != nil {
					t.Errorf("Error: %v", err)
				}
				t.Cleanup(func() { b.client.LRem(context.Background(), b.keyFailed, 0, taken.ID) })
			} else if err := b.Done(taken.ID); err != nil {
				t.Errorf("Done: %v", err)
			}
		}(i, p)
	}
	wg.Wait()

	final, err := b.GetJob(js.ID)
	if err != nil {
		t.Fatalf("GetSet: %v", err)
	}
	if final.Total != wantFinal {
		t.Errorf("Total = %d, want %d", final.Total, wantFinal)
	}
	if final.Current != wantFinal {
		t.Errorf("Current = %d, want %d", final.Current, wantFinal)
	}
	if final.Total != final.Current {
		t.Errorf("Total (%d) and Current (%d) should match exactly once every job is accounted for", final.Total, final.Current)
	}
	if final.Status() != StatusFailed {
		t.Errorf("Status() = %s, want failed (some jobs failed)", final.Status())
	}
}

// TestConcurrentPermanentFailuresKeepAllErrors permanently fails many
// sub-Jobs of the same JobSet at the same time. onJobPermanentlyFailed calls
// mergeErrors for each one, which used to be a GET-then-SET read-modify-write
// on JobSet.errors - under real concurrency, two goroutines could both read
// the same stale error list before either wrote back, so one goroutine's
// error message would silently overwrite the other's instead of both being
// appended (a lost-update race distinct from the numeric total/current
// races the other tests here cover). mergeErrors now appends via the atomic
// JSON.ARRAPPEND command instead, which this test guards against regressing.
func TestConcurrentPermanentFailuresKeepAllErrors(t *testing.T) {
	b := requireRedis(t)
	jobType := uniqueType("concurrent-failures")

	js := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	cleanupJob(t, b, js.ID)
	if err := b.PushJob(js); err != nil {
		t.Fatalf("PushJobSet: %v", err)
	}
	if err := b.StartJob(js.ID); err != nil {
		t.Fatalf("StartJobSet: %v", err)
	}

	const numJobs = 20
	wantMessages := make([]string, numJobs)

	var wg sync.WaitGroup
	for i := 0; i < numJobs; i++ {
		msg := "boom-" + strconv.Itoa(i)
		wantMessages[i] = msg

		wg.Add(1)
		go func(i int, msg string) {
			defer wg.Done()

			// Unique type per goroutine so Take() can only ever return this
			// goroutine's own job (see TestConcurrentMixedOutcomesKeepTotalConsistent).
			jt := jobType + ":worker-" + strconv.Itoa(i)
			job := NewPartialJob(uuid.NewString(), jt, 1000, js.ID)
			job.Total = 1
			cleanupPartialJob(t, b, job.ID)
			if err := b.InitJob(js.ID, 1, nil); err != nil {
				t.Errorf("InitJobSet: %v", err)
				return
			}
			if err := b.PushPartialJob(job, false); err != nil {
				t.Errorf("PushJob: %v", err)
				return
			}
			taken, err := b.Take(jt, "test")
			if err != nil || taken == nil {
				t.Errorf("Take: %v, %+v", err, taken)
				return
			}
			if err := b.Error(taken.ID, msg, false); err != nil {
				t.Errorf("Error: %v", err)
				return
			}
			t.Cleanup(func() { b.client.LRem(context.Background(), b.keyFailed, 0, taken.ID) })
		}(i, msg)
	}
	wg.Wait()

	final, err := b.GetJob(js.ID)
	if err != nil {
		t.Fatalf("GetSet: %v", err)
	}
	if final.Total != 0 {
		t.Errorf("Total = %d, want 0 (every job's unfinished share subtracted)", final.Total)
	}
	if final.Current != 0 {
		t.Errorf("Current = %d, want 0", final.Current)
	}
	if final.Status() != StatusFailed {
		t.Errorf("Status() = %s, want failed", final.Status())
	}
	if len(final.Errors) != numJobs {
		t.Fatalf("len(Errors) = %d, want %d - a concurrent error message was lost", len(final.Errors), numJobs)
	}
	seen := make(map[string]bool, numJobs)
	for _, e := range final.Errors {
		seen[e] = true
	}
	for _, want := range wantMessages {
		if !seen[want] {
			t.Errorf("missing error message %q in final Errors: %v", want, final.Errors)
		}
	}
}
