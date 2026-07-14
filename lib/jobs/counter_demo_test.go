package jobs

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The tests in this file exist to prove Runner/Backend/JobProcessor
// generalize to a job shape with no setup/cleanup and no progressDetails at
// all - the structural opposite of the tile-seeding shape in
// tileseeding_demo_test.go. Previously a `-tags demo` CLI command
// (app/jobs/counterdemo), now just a test since there's no need for it to
// ship in the binary.

const counterDemoType = "demo-counter"

// counterProcessor counts from 1 to PartialJob.Total, one step at a time,
// updating PartialJob.current/Job.current directly via
// UpdatePartialJob/UpdateJob with no updates descriptor - a valid
// alternative to the declarative PartialJob.UpdateTargets mechanism
// tileSeedingSetupProcessor uses.
type counterProcessor struct {
	stepDuration time.Duration
	// failAt, if > 0, makes the processor return a permanent error once it
	// reaches this step, to also exercise onPartialJobPermanentlyFailed
	// without any setup/cleanup/progressDetails involved.
	failAt int
}

func (counterProcessor) JobType() string { return counterDemoType }
func (counterProcessor) Priority() int   { return 1000 }

func (p counterProcessor) Process(partialJob *PartialJob, job *Job, backend Backend) JobResult {
	for i := 1; i <= partialJob.Total; i++ {
		time.Sleep(p.stepDuration)

		if p.failAt > 0 && i == p.failAt {
			return Error(fmt.Sprintf("simulated failure at step %d/%d", i, partialJob.Total))
		}

		if err := backend.UpdatePartialJob(partialJob.ID, 1); err != nil {
			return Error(fmt.Sprintf("step %d/%d: %v", i, partialJob.Total, err))
		}
		if partialJob.PartOf != "" {
			if err := backend.UpdateJob(partialJob.PartOf, 1, nil); err != nil {
				return Error(fmt.Sprintf("step %d/%d: %v", i, partialJob.Total, err))
			}
		}
	}

	return Success()
}

// runCounterDemo pushes a Job with no Setup/Cleanup - just a single
// PartialJob, pushed directly via Backend.PushPartialJob, since PushJob
// only auto-pushes a Setup PartialJob (which this job type doesn't have) -
// and drives it to completion with a Runner registered with
// counterProcessor.
func runCounterDemo(t *testing.T, steps, failAt int, stepDuration, timeout time.Duration) *Job {
	t.Helper()
	b := NewMemoryBackend()

	job := NewJob(uuid.NewString(), counterDemoType, 1000, "Counter demo", nil)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	partialJob := NewPartialJob(uuid.NewString(), counterDemoType, job.Priority, job.ID)
	partialJob.Total = steps
	if err := b.InitJob(job.ID, steps, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	if err := b.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	r := NewRunner(b, "test")
	r.PollInterval = 5 * time.Millisecond
	r.Register(counterProcessor{stepDuration: stepDuration, failAt: failAt})

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	runnerDone := make(chan error, 1)
	go func() { runnerDone <- r.Run(ctx) }()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		current, err := b.GetJob(job.ID)
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
	t.Fatalf("timed out after %s waiting for job %s to finish", timeout, job.ID)
	return nil
}

func TestCounterDemo_CompletesAllSteps(t *testing.T) {
	final := runCounterDemo(t, 5, 0, time.Millisecond, 2*time.Second)

	if final.Status() != StatusSuccessful {
		t.Errorf("Status() = %s, want successful (errors=%v)", final.Status(), final.Errors)
	}
	if final.Current != 5 || final.Total != 5 {
		t.Errorf("Current/Total = %d/%d, want 5/5", final.Current, final.Total)
	}
}

func TestCounterDemo_PermanentFailureStopsJob(t *testing.T) {
	final := runCounterDemo(t, 5, 3, time.Millisecond, 2*time.Second)

	if final.Status() != StatusFailed {
		t.Errorf("Status() = %s, want failed (errors=%v)", final.Status(), final.Errors)
	}
	// Steps 1-2 succeeded before step 3 failed permanently; the remaining
	// 3 (5-2) unfinished steps must have been subtracted from Total by
	// onPartialJobPermanentlyFailed, so Total should end up matching
	// Current exactly.
	if final.Current != 2 || final.Total != 2 {
		t.Errorf("Current/Total = %d/%d, want 2/2", final.Current, final.Total)
	}
}
