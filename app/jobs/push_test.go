package jobs

import (
	"errors"
	"testing"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/jobs"
	"github.com/ldproxy/xtralink/model"
)

func TestParseInputs_RejectsInvalidJSON(t *testing.T) {
	if _, err := ParseInputs("{not json"); err == nil {
		t.Fatal("expected an error for invalid JSON inputs")
	}
}

func TestParseInputs_EmptyStaysNil(t *testing.T) {
	inputs, err := ParseInputs("")
	if err != nil {
		t.Fatalf("ParseInputs: %v", err)
	}
	if inputs != nil {
		t.Errorf("expected nil for an empty inputs string, got %v", inputs)
	}
}

func TestPush_RequiresAKind(t *testing.T) {
	backend := &fakeBackend{}
	appCtx := &app.AppContext{Jobs: backend}

	if _, err := Push(appCtx, PushRequest{}); err == nil {
		t.Fatal("expected an error for a missing kind")
	}
	if backend.pushedJob != nil {
		t.Error("expected PushJob not to be called without a kind")
	}
}

func TestPush_BuildsAndPushesJob(t *testing.T) {
	backend := &fakeBackend{}
	appCtx := &app.AppContext{Jobs: backend}

	job, err := Push(appCtx, PushRequest{JobConfiguration: model.JobConfiguration{
		Kind:     "demo",
		Label:    "my-label",
		Priority: 500,
		Inputs:   map[string]any{"foo": "bar"},
	}})
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

func TestPush_CarriesEveryRequestedJobField(t *testing.T) {
	backend := jobs.NewMemoryBackend()
	appCtx := &app.AppContext{Jobs: backend, Settings: &app.Settings{}}
	ttl := 900

	job, err := Push(appCtx, PushRequest{JobConfiguration: model.JobConfiguration{
		Kind:        "demo",
		Priority:    500,
		Inputs:      map[string]any{"foo": "bar"},
		Context:     map[string]any{"trace": "abc"},
		TtlSeconds:  &ttl,
		Setup:       true,
		Cleanup:     true,
		Description: "what this job is for",
		FollowUps: []model.JobConfiguration{
			{Kind: "notify", Label: "tell someone", Inputs: map[string]any{"to": "ops"}},
		},
	}})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	if job.Inputs["foo"] != "bar" {
		t.Errorf("Inputs = %v", job.Inputs)
	}
	if job.Context["trace"] != "abc" {
		t.Errorf("Context = %v", job.Context)
	}
	if job.TtlSeconds == nil || *job.TtlSeconds != 900 {
		t.Errorf("TtlSeconds = %v, want 900", job.TtlSeconds)
	}
	if job.Description != "what this job is for" {
		t.Errorf("Description = %q", job.Description)
	}
	// The kinds follow from the Job's own kind, so a boolean is all the
	// configuration needed (s. lib/jobs.SetupKind).
	if job.Setup == nil || job.Setup.Kind != "demo:setup" {
		t.Errorf("Setup = %+v, want kind demo:setup", job.Setup)
	}
	if job.Cleanup == nil || job.Cleanup.Kind != "demo:cleanup" {
		t.Errorf("Cleanup = %+v, want kind demo:cleanup", job.Cleanup)
	}
	if job.Setup.PartOf != job.Id || job.Cleanup.PartOf != job.Id {
		t.Errorf("setup/cleanup must belong to the Job: %q/%q, want %q", job.Setup.PartOf, job.Cleanup.PartOf, job.Id)
	}
	if len(job.FollowUps) != 1 || job.FollowUps[0].Kind != "notify" {
		t.Fatalf("FollowUps = %+v, want one notify job", job.FollowUps)
	}
	if job.FollowUps[0].Inputs["to"] != "ops" {
		t.Errorf("follow-up inputs = %v", job.FollowUps[0].Inputs)
	}
}

func TestPush_NoInputsStaysNil(t *testing.T) {
	backend := &fakeBackend{}
	appCtx := &app.AppContext{Jobs: backend}

	job, err := Push(appCtx, PushRequest{JobConfiguration: model.JobConfiguration{Kind: "demo"}})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if job.Inputs != nil {
		t.Errorf("expected nil Inputs when none were given, got %v", job.Inputs)
	}
}

func TestPush_WrapsBackendError(t *testing.T) {
	backend := &fakeBackend{pushJobErr: errors.New("boom")}
	appCtx := &app.AppContext{Jobs: backend}

	if _, err := Push(appCtx, PushRequest{JobConfiguration: model.JobConfiguration{Kind: "demo"}}); err == nil {
		t.Fatal("expected an error to be returned")
	}
}

func TestPush_MatchingJobDefinitionBuildsSinglePartialJob(t *testing.T) {
	backend := jobs.NewMemoryBackend()
	appCtx := &app.AppContext{
		Jobs: backend,
		Settings: &app.Settings{
			JobDefinitions: []app.JobDefinition{
				{Kind: "nba-transformation", Workflow: "nba-transform"},
			},
		},
	}

	job, err := Push(appCtx, PushRequest{JobConfiguration: model.JobConfiguration{
		Kind: "nba-transformation", Label: "my label", Priority: 500,
	}})
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

// TestPush_SequentialAssignsSequenceSlots covers the multi-step,
// sequential shape the job:push workflow action builds: each step gets
// its Sequence slot in Partials order, so step 1 only becomes takeable once
// step 0 is done.
func TestPush_SequentialAssignsSequenceSlots(t *testing.T) {
	backend := jobs.NewMemoryBackend()
	appCtx := &app.AppContext{Jobs: backend, Settings: &app.Settings{}}

	kinds := []string{"step-a", "step-b"}

	job, err := Push(appCtx, PushRequest{
		JobConfiguration: model.JobConfiguration{Kind: "pipeline"},
		Partials:         kinds,
		Sequential:       true,
	})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if job.Sequence == nil {
		t.Fatal("expected Sequential to opt the Job into sequencing")
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

	job, err := Push(appCtx, PushRequest{JobConfiguration: model.JobConfiguration{Kind: "ad-hoc-type"}})
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
