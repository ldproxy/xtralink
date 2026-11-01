package jobs

import (
	"testing"

	"github.com/ldproxy/xtralink/model"
)

// pushConfigured pushes the Job cfg describes, the way every caller of the
// queue builds one. Both backends are covered via forEachBackend, whose
// Redis namespace is private to the test and cleaned up afterwards.
func pushConfigured(t *testing.T, b Backend, cfg model.JobConfiguration) model.Job {
	t.Helper()
	job := NewJobFromConfiguration(cfg)
	if err := b.PushJob(&job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	return job
}

func TestPartials_QueuedOnPushWithoutSetup(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b Backend) {
		partA, partB := uniqueType("part-a"), uniqueType("part-b")
		job := pushConfigured(t, b, model.JobConfiguration{
			Kind: uniqueType("job"), Priority: 700, Partials: []string{partA, partB},
		})

		if got := requireJob(t, b, job.Id).Progress.Total; got != 2 {
			t.Errorf("Job total = %d, want 2 (one per partial)", got)
		}
		for _, kind := range []string{partA, partB} {
			partial := mustTake(t, b, kind)
			if partial.PartOf != job.Id || partial.Priority != 700 || partial.Progress.Total != 1 {
				t.Errorf("%s: PartOf=%q Priority=%d Total=%d, want %q/700/1",
					kind, partial.PartOf, partial.Priority, partial.Progress.Total, job.Id)
			}
		}
	})
}

func TestPartials_QueuedOnlyOnceSetupSucceeds(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b Backend) {
		jobKind, part := uniqueType("job"), uniqueType("part")
		job := pushConfigured(t, b, model.JobConfiguration{
			Kind: jobKind, Setup: true, Partials: []string{part},
		})

		if n := openCount(t, b, part); n != 0 {
			t.Fatalf("%d partials queued before setup ran, want 0", n)
		}

		setup := mustTake(t, b, SetupKind(jobKind))
		if n := openCount(t, b, part); n != 0 {
			t.Fatalf("%d partials queued while setup runs, want 0", n)
		}
		if err := b.Done(setup.Id); err != nil {
			t.Fatalf("Done(setup): %v", err)
		}

		if got := requireJob(t, b, job.Id).Progress.Total; got != 1 {
			t.Errorf("Job total = %d, want 1", got)
		}
		partial := mustTake(t, b, part)
		if err := b.StartJob(job.Id); err != nil {
			t.Fatalf("StartJob: %v", err)
		}
		if err := b.UpdatePartialJob(partial.Id, 1); err != nil {
			t.Fatalf("UpdatePartialJob: %v", err)
		}
		if err := b.Done(partial.Id); err != nil {
			t.Fatalf("Done(partial): %v", err)
		}
		if finished := requireJob(t, b, job.Id); finished.Status != model.StatusSUCCESSFUL {
			t.Errorf("Job status = %s, want SUCCESSFUL", finished.Status)
		}
	})
}

func TestPartials_NeverQueuedWhenSetupFails(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b Backend) {
		jobKind, part := uniqueType("job"), uniqueType("part")
		job := pushConfigured(t, b, model.JobConfiguration{
			Kind: jobKind, Setup: true, Partials: []string{part},
		})

		setup := mustTake(t, b, SetupKind(jobKind))
		if err := b.Error(setup.Id, "setup broke", false); err != nil {
			t.Fatalf("Error(setup): %v", err)
		}

		if n := openCount(t, b, part); n != 0 {
			t.Errorf("%d partials queued after setup failed, want 0", n)
		}
		failed := requireJob(t, b, job.Id)
		if failed.Status != model.StatusFAILED || failed.Progress.Total != 0 {
			t.Errorf("Job status=%s total=%d, want FAILED/0", failed.Status, failed.Progress.Total)
		}
	})
}

func TestPartials_NeverQueuedWhenDismissedDuringSetup(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b Backend) {
		jobKind, part := uniqueType("job"), uniqueType("part")
		job := pushConfigured(t, b, model.JobConfiguration{
			Kind: jobKind, Setup: true, Partials: []string{part},
		})

		setup := mustTake(t, b, SetupKind(jobKind))
		mustCancel(t, b, job.Id)
		if err := b.Done(setup.Id); err != nil {
			t.Fatalf("Done(setup): %v", err)
		}

		if n := openCount(t, b, part); n != 0 {
			t.Errorf("%d partials queued for a dismissed Job, want 0", n)
		}
		if dismissed := requireJob(t, b, job.Id); dismissed.FinishedAt <= 0 {
			t.Error("a Job dismissed during setup must finish once setup does")
		}
	})
}

func TestPartials_SequentialAfterSetupRunInOrder(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b Backend) {
		jobKind, first, second := uniqueType("job"), uniqueType("first"), uniqueType("second")
		job := pushConfigured(t, b, model.JobConfiguration{
			Kind: jobKind, Setup: true, Sequential: true, Partials: []string{first, second},
		})
		if job.Sequence == nil {
			t.Fatal("Sequential must give the Job a sequence")
		}

		setup := mustTake(t, b, SetupKind(jobKind))
		if err := b.Done(setup.Id); err != nil {
			t.Fatalf("Done(setup): %v", err)
		}

		if early, err := b.Take(second, "test"); err != nil || early != nil {
			t.Fatalf("Take(second) before first is done = %+v, %v, want nil", early, err)
		}
		firstPartial := mustTake(t, b, first)
		if firstPartial.Sequence == nil || *firstPartial.Sequence != 0 {
			t.Errorf("first: Sequence = %v, want 0", firstPartial.Sequence)
		}
		if err := b.Done(firstPartial.Id); err != nil {
			t.Fatalf("Done(first): %v", err)
		}
		secondPartial := mustTake(t, b, second)
		if secondPartial.Sequence == nil || *secondPartial.Sequence != 1 {
			t.Errorf("second: Sequence = %v, want 1", secondPartial.Sequence)
		}
	})
}

func TestPartials_FollowUpQueuesItsOwnPartials(t *testing.T) {
	forEachBackend(t, func(t *testing.T, b Backend) {
		part, followUpPart := uniqueType("part"), uniqueType("follow-up-part")
		job := pushConfigured(t, b, model.JobConfiguration{
			Kind:     uniqueType("job"),
			Partials: []string{part},
			FollowUps: []model.JobConfiguration{
				{Kind: uniqueType("follow-up"), Partials: []string{followUpPart}},
			},
		})
		followUpID := job.FollowUps[0].Id

		if n := openCount(t, b, followUpPart); n != 0 {
			t.Fatalf("%d follow-up partials queued before the Job finished, want 0", n)
		}

		partial := mustTake(t, b, part)
		if err := b.StartJob(job.Id); err != nil {
			t.Fatalf("StartJob: %v", err)
		}
		if err := b.UpdatePartialJob(partial.Id, 1); err != nil {
			t.Fatalf("UpdatePartialJob: %v", err)
		}
		if err := b.Done(partial.Id); err != nil {
			t.Fatalf("Done(partial): %v", err)
		}

		followUpPartial := mustTake(t, b, followUpPart)
		if followUpPartial.PartOf != followUpID {
			t.Errorf("follow-up partial PartOf = %q, want %q", followUpPartial.PartOf, followUpID)
		}
		if got := requireJob(t, b, followUpID).Progress.Total; got != 1 {
			t.Errorf("follow-up total = %d, want 1", got)
		}
	})
}
