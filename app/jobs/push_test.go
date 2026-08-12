package jobs

import (
	"errors"
	"testing"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/jobs"
)

func TestPush_RejectsInvalidJSON(t *testing.T) {
	backend := &fakeBackend{}
	appCtx := &app.AppContext{Jobs: backend}

	if _, err := Push(appCtx, "demo", "label", 1000, "{not json"); err == nil {
		t.Fatal("expected an error for invalid JSON inputs")
	}
	if backend.pushedJob != nil {
		t.Error("expected PushJob not to be called for invalid inputs")
	}
}

func TestPush_BuildsAndPushesJob(t *testing.T) {
	backend := &fakeBackend{}
	appCtx := &app.AppContext{Jobs: backend}

	job, err := Push(appCtx, "demo", "my-label", 500, `{"foo":"bar"}`)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if job.Kind != "demo" || job.Label != "my-label" || job.Priority != 500 {
		t.Errorf("unexpected Job fields: %+v", job)
	}
	if job.Inputs == nil || job.Inputs["foo"] != "bar" {
		t.Errorf("Inputs = %v, want {\"foo\":\"bar\"}", job.Inputs)
	}
	if backend.pushedJob != job {
		t.Error("expected PushJob to have been called with the returned Job")
	}
}

func TestPush_EmptyInputsStaysNil(t *testing.T) {
	backend := &fakeBackend{}
	appCtx := &app.AppContext{Jobs: backend}

	job, err := Push(appCtx, "demo", "", 1000, "")
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if job.Inputs != nil {
		t.Errorf("expected nil Inputs for empty inputsRaw, got %v", job.Inputs)
	}
}

func TestPush_WrapsBackendError(t *testing.T) {
	backend := &fakeBackend{pushJobErr: errors.New("boom")}
	appCtx := &app.AppContext{Jobs: backend}

	if _, err := Push(appCtx, "demo", "", 1000, ""); err == nil {
		t.Fatal("expected an error to be returned")
	}
}

func TestPush_MatchingJobDefinitionBuildsSinglePartialJob(t *testing.T) {
	backend := jobs.NewMemoryBackend()
	appCtx := &app.AppContext{
		Jobs: backend,
		Settings: &app.Settings{
			JobDefinitions: []app.JobDefinition{
				{Id: "nba-transformation", Workflow: "nba-transform"},
			},
		},
	}

	job, err := Push(appCtx, "nba-transformation", "my label", 500, "")
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	// A single-step Job runs unsequenced (the Push default), so it carries
	// no JobSequence and its PartialJob no Sequence slot.
	if job.Sequence != nil {
		t.Errorf("expected no sequencing on a single-step Job, got %+v", job.Sequence)
	}

	step, err := backend.Take("nba-transformation", "test")
	if err != nil || step == nil {
		t.Fatalf("Take(nba-transformation): %v, %+v", err, step)
	}
	if step.PartOf != job.Id {
		t.Errorf("step: PartOf=%q, want %q", step.PartOf, job.Id)
	}
	if step.Sequence != nil {
		t.Errorf("step: Sequence=%d, want unset", *step.Sequence)
	}
	if step.Progress.Total != 1 {
		t.Errorf("step: Progress.Total = %d, want 1", step.Progress.Total)
	}
}

// TestPushPipeline_SequentialAssignsSequenceSlots covers the multi-step,
// parallel=false shape the job:push workflow action builds: each step gets
// its Sequence slot in defs order, so step 1 only becomes takeable once
// step 0 is done.
func TestPushPipeline_SequentialAssignsSequenceSlots(t *testing.T) {
	backend := jobs.NewMemoryBackend()
	appCtx := &app.AppContext{Jobs: backend, Settings: &app.Settings{}}

	defs := []app.JobDefinition{
		{Id: "step-a", Workflow: "wf-a"},
		{Id: "step-b", Workflow: "wf-b"},
	}

	job, err := PushPipeline(appCtx, "pipeline", "", 1000, "", defs, false)
	if err != nil {
		t.Fatalf("PushPipeline: %v", err)
	}
	if job.Sequence == nil {
		t.Fatal("expected parallel=false to opt the Job into sequencing")
	}

	if taken, err := backend.Take("step-b", "test"); err != nil || taken != nil {
		t.Fatalf("Take(step-b) before step-a is done = %+v, %v, want nil, nil", taken, err)
	}

	stepA, err := backend.Take("step-a", "test")
	if err != nil || stepA == nil {
		t.Fatalf("Take(step-a): %v, %+v", err, stepA)
	}
	if stepA.Sequence == nil || *stepA.Sequence != 0 {
		t.Errorf("step-a: Sequence = %v, want 0", stepA.Sequence)
	}
	if err := backend.Done(stepA.Id); err != nil {
		t.Fatalf("Done(step-a): %v", err)
	}

	stepB, err := backend.Take("step-b", "test")
	if err != nil || stepB == nil {
		t.Fatalf("Take(step-b) after step-a is done: %v, %+v", err, stepB)
	}
	if stepB.Sequence == nil || *stepB.Sequence != 1 {
		t.Errorf("step-b: Sequence = %v, want 1", stepB.Sequence)
	}
}

func TestPush_UnknownTypeStaysBareJob(t *testing.T) {
	backend := jobs.NewMemoryBackend()
	appCtx := &app.AppContext{Jobs: backend, Settings: &app.Settings{}}

	job, err := Push(appCtx, "ad-hoc-type", "", 1000, "")
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if job.Id == "" {
		t.Error("expected a valid job")
	}
	if taken, err := backend.Take("ad-hoc-type", "test"); err != nil || taken != nil {
		t.Fatalf("expected no PartialJob queued for a bare job type, got %+v, %v", taken, err)
	}
}
