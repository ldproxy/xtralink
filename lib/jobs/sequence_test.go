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

// A sequenced Job is a pipeline, so a permanently failed part stops it: the
// next slot never opens and the Job ends failed, rather than later parts
// running against work that never happened.
func TestSequencedJobStopsOnAPermanentlyFailedPart(t *testing.T) {
	backend := NewMemoryBackend()

	job := NewJob("job-1", "pipeline", 1000, "", nil)
	job.Sequence = &model.JobSequence{Current: 0, Remaining: 0}
	if err := backend.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	if err := backend.StartJob(job.Id); err != nil {
		t.Fatalf("StartJob: %v", err)
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

	stepA, err := backend.Take("step-a", "test")
	if err != nil || stepA == nil {
		t.Fatalf("Take(step-a): %v, %+v", err, stepA)
	}
	// retry=false: a workflow-backed part always fails permanently
	// (s. app/workflows.WorkflowJobProcessor, which returns model.Error).
	if err := backend.Error(stepA.Id, "step-a blew up", false); err != nil {
		t.Fatalf("Error(step-a): %v", err)
	}

	if taken, err := backend.Take("step-b", "test"); err != nil || taken != nil {
		t.Fatalf("Take(step-b) after step-a failed = %+v, %v, want nil, nil - the pipeline must stop", taken, err)
	}

	stored, err := backend.GetJob(job.Id)
	if err != nil || stored == nil {
		t.Fatalf("GetJob: %v, %+v", err, stored)
	}
	if stored.GetStatus() != model.StatusFAILED {
		t.Errorf("job status = %v, want FAILED", stored.GetStatus())
	}
	if stored.FinishedAt <= 0 {
		t.Error("expected the job to be finished rather than left running")
	}
	if len(stored.Errors) != 1 || stored.Errors[0] != "step-a blew up" {
		t.Errorf("job errors = %v, want the part's own error once", stored.Errors)
	}
	if stored.Progress.Current != 0 || stored.Progress.Total != 2 {
		t.Errorf("progress = %+v, want 0/2 - the second part never ran", stored.Progress)
	}
}

// An unsequenced Job is a fan-out of independent parts, so one failing does
// not stop the others - and it still spawns its follow-ups, which is the
// gap pushFollowUps documents: most of the work did happen, and nothing
// here can tell a follow-up that tolerates incomplete input from one that
// does not.
func TestUnsequencedJobKeepsGoingAfterAFailedPart(t *testing.T) {
	backend := NewMemoryBackend()

	followUp := NewJob("follow-2", "notify", 1000, "", nil)
	job := NewJob("job-2", "fanout", 1000, "", nil)
	job.FollowUps = []model.Job{*followUp}
	if err := backend.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	if err := backend.StartJob(job.Id); err != nil {
		t.Fatalf("StartJob: %v", err)
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

	stepA, _ := backend.Take("step-a", "test")
	if stepA == nil {
		t.Fatal("Take(step-a) returned nothing")
	}
	if err := backend.Error(stepA.Id, "step-a blew up", false); err != nil {
		t.Fatalf("Error(step-a): %v", err)
	}

	stepB, err := backend.Take("step-b", "test")
	if err != nil || stepB == nil {
		t.Fatalf("Take(step-b): %v, %+v - an independent part must still run", err, stepB)
	}
	if err := backend.UpdatePartialJob(stepB.Id, 1); err != nil {
		t.Fatalf("UpdatePartialJob(step-b): %v", err)
	}
	if err := backend.Done(stepB.Id); err != nil {
		t.Fatalf("Done(step-b): %v", err)
	}

	stored, _ := backend.GetJob(job.Id)
	if stored.GetStatus() != model.StatusFAILED {
		t.Errorf("job status = %v, want FAILED - one part did fail", stored.GetStatus())
	}
	if stored.Progress.Current != stored.Progress.Total {
		t.Errorf("progress = %+v, want every part accounted for", stored.Progress)
	}

	if pushed, err := backend.GetJob(followUp.Id); err != nil || pushed == nil {
		t.Errorf("follow-up = %+v, %v - a fan-out spawns follow-ups even with a failed part", pushed, err)
	}
}

// Cleanup is what undoes a half-finished Job, so it runs even when the
// pipeline stopped at a failed part. Follow-ups are the opposite: further
// work predicated on the Job having produced something.
func TestSequencedJobRunsCleanupButNoFollowUpsAfterAFailedPart(t *testing.T) {
	backend := NewMemoryBackend()

	followUp := NewJob("follow-1", "notify", 1000, "", nil)
	job := NewJob("job-1", "pipeline", 1000, "", nil)
	job.Sequence = &model.JobSequence{Current: 0, Remaining: 0}
	job.Cleanup = NewPartialJob("cleanup-1", "tidy", 1000, job.Id)
	job.FollowUps = []model.Job{*followUp}

	if err := backend.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	if err := backend.StartJob(job.Id); err != nil {
		t.Fatalf("StartJob: %v", err)
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

	stepA, err := backend.Take("step-a", "test")
	if err != nil || stepA == nil {
		t.Fatalf("Take(step-a): %v, %+v", err, stepA)
	}
	if err := backend.Error(stepA.Id, "step-a blew up", false); err != nil {
		t.Fatalf("Error(step-a): %v", err)
	}

	cleanup, err := backend.Take("tidy", "test")
	if err != nil || cleanup == nil {
		t.Fatalf("Take(tidy): %v, %+v - cleanup must run after a failed part", err, cleanup)
	}
	if err := backend.Done(cleanup.Id); err != nil {
		t.Fatalf("Done(cleanup): %v", err)
	}

	if pushed, err := backend.GetJob(followUp.Id); err != nil || pushed != nil {
		t.Errorf("follow-up = %+v, %v - a failed Job must not spawn follow-up work", pushed, err)
	}

	stored, _ := backend.GetJob(job.Id)
	if stored.GetStatus() != model.StatusFAILED {
		t.Errorf("job status = %v, want FAILED", stored.GetStatus())
	}
	if taken, err := backend.Take("step-b", "test"); err != nil || taken != nil {
		t.Fatalf("Take(step-b) = %+v, %v - the pipeline must still be stopped", taken, err)
	}
}

// The same for a failed setup: nothing of the Job ran, but cleanup still
// gets its chance at whatever the setup left behind.
func TestFailedSetupStillRunsCleanup(t *testing.T) {
	backend := NewMemoryBackend()

	job := NewJob("job-2", "pipeline", 1000, "", nil)
	job.Setup = NewPartialJob("setup-1", "prepare", 1000, job.Id)
	job.Cleanup = NewPartialJob("cleanup-1", "tidy", 1000, job.Id)
	if err := backend.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	setup, err := backend.Take("prepare", "test")
	if err != nil || setup == nil {
		t.Fatalf("Take(prepare): %v, %+v", err, setup)
	}
	if err := backend.Error(setup.Id, "setup blew up", false); err != nil {
		t.Fatalf("Error(setup): %v", err)
	}

	cleanup, err := backend.Take("tidy", "test")
	if err != nil || cleanup == nil {
		t.Fatalf("Take(tidy): %v, %+v - cleanup must run after a failed setup", err, cleanup)
	}

	stored, _ := backend.GetJob(job.Id)
	if stored.GetStatus() != model.StatusFAILED {
		t.Errorf("job status = %v, want FAILED", stored.GetStatus())
	}
}
