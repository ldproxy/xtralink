package actions

import (
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/jobs"
	"github.com/ldproxy/xtralink/lib/workflows"
)

func TestJobPushAction_PushesJobWithInputs(t *testing.T) {
	targetDir := t.TempDir()
	appCtx, backend := newTestAppContext(t, targetDir)

	action := &JobPushAction{AppCtx: appCtx}
	result, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"type": "nba-apply",
		"inputs": []any{
			map[string]any{"name": "package", "value": "s3://bucket"},
			map[string]any{"name": "file", "value": "a.zip"},
		},
	}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(result.Outputs) != 1 {
		t.Fatalf("expected exactly 1 output set, got %+v", result.Outputs)
	}

	if backend.pushedJob == nil {
		t.Fatal("expected a Job to have been pushed")
	}
	if backend.pushedJob.Type != "nba-apply" {
		t.Errorf("Type = %q, want nba-apply", backend.pushedJob.Type)
	}

	var inputs map[string]string
	if err := json.Unmarshal(backend.pushedJob.Inputs, &inputs); err != nil {
		t.Fatalf("unmarshal inputs: %v", err)
	}
	if inputs["package"] != "s3://bucket" || inputs["file"] != "a.zip" {
		t.Errorf("inputs = %+v", inputs)
	}
}

func TestJobPushAction_MissingTypeIsError(t *testing.T) {
	targetDir := t.TempDir()
	appCtx, _ := newTestAppContext(t, targetDir)

	action := &JobPushAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{}}); err == nil {
		t.Fatal("expected an error for a missing type param")
	}
}

func TestJobPushAction_NoInputsIsFine(t *testing.T) {
	targetDir := t.TempDir()
	appCtx, backend := newTestAppContext(t, targetDir)

	action := &JobPushAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{"type": "demo"}}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if backend.pushedJob == nil {
		t.Fatal("expected a Job to have been pushed")
	}
}

// jobDefinitionsAppCtx builds a real *app.AppContext around a real
// jobs.MemoryBackend (not fakeBackend, which never records PartialJobs) and
// Settings with the given JobDefinitions - needed for `partials:`, which
// resolves each referenced type via Settings.GetJobStep.
func jobDefinitionsAppCtx(defs []app.JobDefinition) (*app.AppContext, *jobs.MemoryBackend) {
	backend := jobs.NewMemoryBackend()
	return &app.AppContext{
		Logger:   zerolog.Nop(),
		Jobs:     backend,
		Settings: &app.Settings{JobDefinitions: defs},
	}, backend
}

func nbaPipelineDefs() []app.JobDefinition {
	return []app.JobDefinition{{
		Id: "nba-pipeline",
		Steps: []app.JobStepDefinition{
			{Id: "nba-transformation", Workflow: "nba-transform"},
			{Id: "nba-transaction-step", Workflow: "nba-transaction"},
		},
	}}
}

func TestJobPushAction_PartialsBuildsMultiStepPipeline(t *testing.T) {
	appCtx, backend := jobDefinitionsAppCtx(nbaPipelineDefs())

	action := &JobPushAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"type": "nba-apply",
		"partials": []any{
			map[string]any{"type": "nba-transformation"},
			map[string]any{"type": "nba-transaction-step"},
		},
	}}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	jobsList, err := backend.GetJobs()
	if err != nil || len(jobsList) != 1 {
		t.Fatalf("GetJobs: %v, %+v", err, jobsList)
	}
	job := jobsList[0]
	if job.Type != "nba-apply" || !job.Parallel {
		t.Errorf("unexpected Job: type=%q parallel=%v, want nba-apply/true", job.Type, job.Parallel)
	}

	step0, err := backend.Take("nba-transformation", "test")
	if err != nil || step0 == nil {
		t.Fatalf("Take(nba-transformation): %v, %+v", err, step0)
	}
	if step0.PartOf != job.ID || step0.Sequence != 0 {
		t.Errorf("step0: PartOf=%q Sequence=%d, want %q/0", step0.PartOf, step0.Sequence, job.ID)
	}

	// Parallel defaults to true (no `parallel: false` given), so step1 must
	// be immediately takeable too, not gated behind step0.
	step1, err := backend.Take("nba-transaction-step", "test")
	if err != nil || step1 == nil {
		t.Fatalf("Take(nba-transaction-step): %v, %+v", err, step1)
	}
	if step1.Sequence != 1 {
		t.Errorf("step1.Sequence = %d, want 1", step1.Sequence)
	}
}

func TestJobPushAction_PartialsSequentialGatesLaterSteps(t *testing.T) {
	appCtx, backend := jobDefinitionsAppCtx(nbaPipelineDefs())

	action := &JobPushAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"type":     "nba-apply",
		"parallel": false,
		"partials": []any{
			map[string]any{"type": "nba-transformation"},
			map[string]any{"type": "nba-transaction-step"},
		},
	}}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if taken, err := backend.Take("nba-transaction-step", "test"); err != nil || taken != nil {
		t.Fatalf("Take(nba-transaction-step) before nba-transformation is done = %+v, %v, want nil, nil", taken, err)
	}
	if taken, err := backend.Take("nba-transformation", "test"); err != nil || taken == nil {
		t.Fatalf("Take(nba-transformation): %v, %+v", err, taken)
	}
}

func TestJobPushAction_PartialsUnknownTypeIsError(t *testing.T) {
	appCtx, backend := jobDefinitionsAppCtx(nbaPipelineDefs())

	action := &JobPushAction{AppCtx: appCtx}
	_, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"type": "nba-apply",
		"partials": []any{
			map[string]any{"type": "does-not-exist"},
		},
	}})
	if err == nil {
		t.Fatal("expected an error for a partials entry referencing an unknown step id")
	}
	if jobsList, _ := backend.GetJobs(); len(jobsList) != 0 {
		t.Errorf("expected no Job to have been pushed, got %+v", jobsList)
	}
}

func TestJobPushAction_PartialsEmptyListIsError(t *testing.T) {
	appCtx, _ := jobDefinitionsAppCtx(nbaPipelineDefs())

	action := &JobPushAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"type":     "nba-apply",
		"partials": []any{},
	}}); err == nil {
		t.Fatal("expected an error for an empty partials list")
	}
}
