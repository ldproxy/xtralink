package jobs

import (
	"testing"

	"github.com/ldproxy/xtralink/model"
)

// A sequenced Job's setup and cleanup are outside the sequence: they must
// neither claim a slot nor wait for one. Without that, a sequenced Job with
// a setup deadlocks - setup takes slot 0, every ordinary step waits on a
// Current that only ordinary steps advance, and none of them can start.
func TestSequencedJobWithSetupAndCleanupRunsToCompletion(t *testing.T) {
	backend := NewMemoryBackend()

	job := NewJob("job-1", "pipeline", 1000, "", nil)
	job.Sequence = &model.JobSequence{Current: 0, Remaining: 0}
	job.Setup = NewPartialJob("setup-1", "prepare", 1000, job.Id)
	job.Cleanup = NewPartialJob("cleanup-1", "tidy", 1000, job.Id)

	if err := backend.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	// The Runner marks a Job started when it takes the Job's first
	// PartialJob (s. runner.go's StartJob call); a Job that never started
	// can never be done, and cleanup only runs once it is.
	if err := backend.StartJob(job.Id); err != nil {
		t.Fatalf("StartJob: %v", err)
	}

	setup, err := backend.Take("prepare", "test")
	if err != nil || setup == nil {
		t.Fatalf("Take(prepare): %v, %+v - setup must be takeable at once", err, setup)
	}
	if setup.Sequence != nil {
		t.Errorf("setup claimed sequence slot %d, want none", *setup.Sequence)
	}

	for _, kind := range []string{"step-a", "step-b"} {
		partialJob := NewPartialJob("partial-"+kind, kind, 1000, job.Id)
		partialJob.Progress.Total = 1
		if err := backend.InitJob(job.Id, 1, nil); err != nil {
			t.Fatalf("InitJob: %v", err)
		}
		if err := backend.PushPartialJob(partialJob, false); err != nil {
			t.Fatalf("PushPartialJob(%s): %v", kind, err)
		}
	}

	if err := backend.Done(setup.Id); err != nil {
		t.Fatalf("Done(setup): %v", err)
	}

	stepA, err := backend.Take("step-a", "test")
	if err != nil || stepA == nil {
		t.Fatalf("Take(step-a): %v, %+v - the first step must be takeable after setup", err, stepA)
	}
	if stepA.Sequence == nil || *stepA.Sequence != 0 {
		t.Fatalf("step-a: Sequence = %v, want slot 0", stepA.Sequence)
	}
	if taken, err := backend.Take("step-b", "test"); err != nil || taken != nil {
		t.Fatalf("Take(step-b) before step-a is done = %+v, %v, want nil, nil", taken, err)
	}
	if err := backend.UpdatePartialJob(stepA.Id, 1); err != nil {
		t.Fatalf("UpdatePartialJob(step-a): %v", err)
	}
	if err := backend.Done(stepA.Id); err != nil {
		t.Fatalf("Done(step-a): %v", err)
	}

	stepB, err := backend.Take("step-b", "test")
	if err != nil || stepB == nil {
		t.Fatalf("Take(step-b) after step-a is done: %v, %+v", err, stepB)
	}
	if err := backend.UpdatePartialJob(stepB.Id, 1); err != nil {
		t.Fatalf("UpdatePartialJob(step-b): %v", err)
	}
	if err := backend.Done(stepB.Id); err != nil {
		t.Fatalf("Done(step-b): %v", err)
	}

	cleanup, err := backend.Take("tidy", "test")
	if err != nil || cleanup == nil {
		t.Fatalf("Take(tidy): %v, %+v - cleanup must be takeable once the last step finished", err, cleanup)
	}
	if cleanup.Sequence != nil {
		t.Errorf("cleanup claimed sequence slot %d, want none", *cleanup.Sequence)
	}
}

func TestIsSetupOrCleanup(t *testing.T) {
	job := NewJob("job-1", "pipeline", 1000, "", nil)
	job.Setup = NewPartialJob("setup-1", "prepare", 1000, job.Id)
	job.Cleanup = NewPartialJob("cleanup-1", "tidy", 1000, job.Id)

	cases := map[string]bool{"setup-1": true, "cleanup-1": true, "ordinary-1": false}
	for id, want := range cases {
		if got := isSetupOrCleanup(job, id); got != want {
			t.Errorf("isSetupOrCleanup(%q) = %v, want %v", id, got, want)
		}
	}

	bare := NewJob("job-2", "pipeline", 1000, "", nil)
	if isSetupOrCleanup(bare, "anything") {
		t.Error("a Job with no setup or cleanup should match nothing")
	}
}
