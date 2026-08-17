package jobs

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ldproxy/xtralink/model"
)

// The tests in this file exist to prove Runner/Backend/JobProcessor
// generalize to a job shape with no setup/cleanup and no progressDetails at
// all - the structural opposite of the tile-seeding shape in
// tileseeding_demo_test.go. Previously a `-tags demo` CLI command
// (app/jobs/counterdemo), now just a test since there's no need for it to
// ship in the binary.

const counterDemoType = "demo-counter"

// counterProcessor counts from 1 to PartialJob.Total, one step at a time,
// reporting each via a plain UpdatePartialJob with no updates descriptor
// (so PartialJob.current and Job.current advance, but no progressDetails
// path does) - a valid alternative to the declarative
// PartialJob.ProgressUpdates mechanism tileSeedingSetupProcessor uses.
func counterProcessor(
	stepDuration time.Duration,
	// failAt, if > 0, makes the processor return a permanent error once it
	// reaches this step, to also exercise onPartialJobPermanentlyFailed
	// without any setup/cleanup/progressDetails involved.
	failAt int) ProcessFunc {
	return func(partialJob *model.PartialJob, job *model.Job, backend Backend) model.JobResult {
		for i := 1; i <= partialJob.Progress.Total; i++ {
			time.Sleep(stepDuration)

			if failAt > 0 && i == failAt {
				return model.Error(fmt.Sprintf("simulated failure at step %d/%d", i, partialJob.Progress.Total))
			}

			if err := backend.UpdatePartialJob(partialJob.Id, 1); err != nil {
				return model.Error(fmt.Sprintf("step %d/%d: %v", i, partialJob.Progress.Total, err))
			}
		}

		return model.Success()
	}
}

// runCounterDemo pushes a Job with no Setup/Cleanup - just a single
// PartialJob, pushed directly via Backend.PushPartialJob, since PushJob
// only auto-pushes a Setup PartialJob (which this job type doesn't have) -
// and drives it to completion with a Runner registered with
// counterProcessor.
func runCounterDemo(t *testing.T, steps, failAt int, stepDuration, timeout time.Duration) *model.Job {
	t.Helper()
	b := NewMemoryBackend()

	job := NewJob(uuid.NewString(), counterDemoType, 1000, "Counter demo", nil)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	partialJob := NewPartialJob(uuid.NewString(), counterDemoType, job.Priority, job.Id)
	partialJob.Progress.Total = steps
	if err := b.InitJob(job.Id, steps, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	if err := b.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	r := NewRunner(b, "test")
	r.PollInterval = 5 * time.Millisecond
	r.Register(&JobProcessor{Kind: counterDemoType, Priority: 1000, Process: counterProcessor(stepDuration, failAt)})

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	runnerDone := make(chan error, 1)
	go func() { runnerDone <- r.Run(ctx) }()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		current, err := b.GetJob(job.Id)
		if err != nil {
			t.Fatalf("GetJob: %v", err)
		}
		if current != nil && current.FinishedAt > 0 {
			cancel()
			<-runnerDone
			return current
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	<-runnerDone
	t.Fatalf("timed out after %s waiting for job %s to finish", timeout, job.Id)
	return nil
}

func TestCounterDemo_CompletesAllSteps(t *testing.T) {
	final := runCounterDemo(t, 5, 0, time.Millisecond, 2*time.Second)

	if final.Status != model.StatusSUCCESSFUL {
		t.Errorf("Status = %s, want successful (errors=%v)", final.Status, final.Errors)
	}
	if final.Progress.Current != 5 || final.Progress.Total != 5 {
		t.Errorf("Current/Total = %d/%d, want 5/5", final.Progress.Current, final.Progress.Total)
	}
}

func TestCounterDemo_PermanentFailureStopsJob(t *testing.T) {
	final := runCounterDemo(t, 5, 3, time.Millisecond, 2*time.Second)

	if final.Status != model.StatusFAILED {
		t.Errorf("Status = %s, want failed (errors=%v)", final.Status, final.Errors)
	}
	if final.Progress.Current != 5 || final.Progress.Total != 5 {
		t.Errorf("Current/Total = %d/%d, want 5/5", final.Progress.Current, final.Progress.Total)
	}
}
