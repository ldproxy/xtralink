package actions

import (
	"strings"
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
		"kind":   "nba-apply",
		"inputs": map[string]any{"package": "s3://bucket", "file": "a.zip"},
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
	if backend.pushedJob.Kind != "nba-apply" {
		t.Errorf("Type = %q, want nba-apply", backend.pushedJob.Kind)
	}

	inputs := backend.pushedJob.Inputs
	if inputs["package"] != "s3://bucket" || inputs["file"] != "a.zip" {
		t.Errorf("inputs = %+v", inputs)
	}
}

func TestJobPushAction_MissingKindIsError(t *testing.T) {
	targetDir := t.TempDir()
	appCtx, _ := newTestAppContext(t, targetDir)

	action := &JobPushAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{}}); err == nil {
		t.Fatal("expected an error for a missing kind param")
	}
}

func TestJobPushAction_NoInputsIsFine(t *testing.T) {
	targetDir := t.TempDir()
	appCtx, backend := newTestAppContext(t, targetDir)

	action := &JobPushAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{"kind": "demo"}}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if backend.pushedJob == nil {
		t.Fatal("expected a Job to have been pushed")
	}
}

// jobDefinitionsAppCtx builds a real *app.AppContext around a real
// jobs.MemoryBackend (not fakeBackend, which never records PartialJobs) and
// Settings with the given JobDefinitions - needed for `partials:`, which
// resolves each referenced kind via Settings.GetJobDefinition.
func jobDefinitionsAppCtx(defs []app.JobDefinition) (*app.AppContext, *jobs.MemoryBackend) {
	backend := jobs.NewMemoryBackend()
	return &app.AppContext{
		Logger:   zerolog.Nop(),
		Jobs:     backend,
		Settings: &app.Settings{JobDefinitions: defs},
	}, backend
}

func nbaPipelineDefs() []app.JobDefinition {
	return []app.JobDefinition{
		{Kind: "nba-transformation", Workflow: "nba-transform"},
		{Kind: "nba-transaction-step", Workflow: "nba-transaction"},
	}
}

func TestJobPushAction_PartialsBuildsMultiPartPipeline(t *testing.T) {
	appCtx, backend := jobDefinitionsAppCtx(nbaPipelineDefs())

	action := &JobPushAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"kind": "nba-apply",
		"partials": []any{
			map[string]any{"kind": "nba-transformation"},
			map[string]any{"kind": "nba-transaction-step"},
		},
	}}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	jobsList, err := backend.GetJobs()
	if err != nil || len(jobsList) != 1 {
		t.Fatalf("GetJobs: %v, %+v", err, jobsList)
	}
	job := jobsList[0]
	// Sequencing is opt-in (no `sequential: true` given), so the Job opts
	// out of it entirely and its steps carry no Sequence slot.
	if job.Kind != "nba-apply" || job.Sequence != nil {
		t.Errorf("unexpected Job: kind=%q sequence=%+v, want nba-apply/nil", job.Kind, job.Sequence)
	}

	step0, err := backend.Take("nba-transformation", "test")
	if err != nil || step0 == nil {
		t.Fatalf("Take(nba-transformation): %v, %+v", err, step0)
	}
	if step0.PartOf != job.Id {
		t.Errorf("step0: PartOf=%q, want %q", step0.PartOf, job.Id)
	}

	// ... so step1 must be immediately takeable too, not gated behind step0.
	step1, err := backend.Take("nba-transaction-step", "test")
	if err != nil || step1 == nil {
		t.Fatalf("Take(nba-transaction-step): %v, %+v", err, step1)
	}
	if step1.PartOf != job.Id {
		t.Errorf("step1: PartOf=%q, want %q", step1.PartOf, job.Id)
	}
}

func TestJobPushAction_PartialsSequentialGatesLaterPartials(t *testing.T) {
	appCtx, backend := jobDefinitionsAppCtx(nbaPipelineDefs())

	action := &JobPushAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"kind":       "nba-apply",
		"sequential": true,
		"partials": []any{
			map[string]any{"kind": "nba-transformation"},
			map[string]any{"kind": "nba-transaction-step"},
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

func TestJobPushAction_PartialsUnknownKindIsError(t *testing.T) {
	appCtx, backend := jobDefinitionsAppCtx(nbaPipelineDefs())

	action := &JobPushAction{AppCtx: appCtx}
	_, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"kind": "nba-apply",
		"partials": []any{
			map[string]any{"kind": "does-not-exist"},
		},
	}})
	if err == nil {
		t.Fatal("expected an error for a partials entry referencing an unknown kind")
	}
	if jobsList, _ := backend.GetJobs(); len(jobsList) != 0 {
		t.Errorf("expected no Job to have been pushed, got %+v", jobsList)
	}
}

func TestJobPushAction_PartialsEmptyListIsError(t *testing.T) {
	appCtx, _ := jobDefinitionsAppCtx(nbaPipelineDefs())

	action := &JobPushAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"kind":     "nba-apply",
		"partials": []any{},
	}}); err == nil {
		t.Fatal("expected an error for an empty partials list")
	}
}

func TestJobPushAction_CarriesEveryJobField(t *testing.T) {
	appCtx, backend := jobDefinitionsAppCtx(nbaPipelineDefs())

	action := &JobPushAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"kind":       "nba-apply",
		"label":      "a readable label",
		"priority":   500,
		"inputs":     map[string]any{"file": "a.zip"},
		"context":    map[string]any{"trace": "abc"},
		"ttlSeconds": 900,
		"setup":      true,
		"cleanup":    true,
		"followUps": []any{
			map[string]any{"kind": "notify", "label": "tell ops", "inputs": map[string]any{"to": "ops"}},
		},
	}}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	jobsList, err := backend.GetJobs()
	if err != nil || len(jobsList) != 1 {
		t.Fatalf("GetJobs: %v, %+v", err, jobsList)
	}
	job := jobsList[0]

	if job.Label != "a readable label" || job.Priority != 500 {
		t.Errorf("label/priority = %q/%d", job.Label, job.Priority)
	}
	if job.Inputs["file"] != "a.zip" {
		t.Errorf("Inputs = %v", job.Inputs)
	}
	if job.Context["trace"] != "abc" {
		t.Errorf("Context = %v", job.Context)
	}
	if job.TtlSeconds == nil || *job.TtlSeconds != 900 {
		t.Errorf("TtlSeconds = %v, want 900", job.TtlSeconds)
	}
	// A boolean is all setup/cleanup need: their kinds follow from the Job's
	// own kind by convention (s. lib/jobs.SetupKind).
	if job.Setup == nil || job.Setup.Kind != "nba-apply:setup" {
		t.Errorf("Setup = %+v, want kind nba-apply:setup", job.Setup)
	}
	if job.Cleanup == nil || job.Cleanup.Kind != "nba-apply:cleanup" {
		t.Errorf("Cleanup = %+v, want kind nba-apply:cleanup", job.Cleanup)
	}
	if len(job.FollowUps) != 1 || job.FollowUps[0].Kind != "notify" {
		t.Fatalf("FollowUps = %+v", job.FollowUps)
	}
	if job.FollowUps[0].Inputs["to"] != "ops" {
		t.Errorf("follow-up inputs = %v", job.FollowUps[0].Inputs)
	}
}

func TestJobPushAction_InputsMustBeAMap(t *testing.T) {
	targetDir := t.TempDir()
	appCtx, _ := newTestAppContext(t, targetDir)

	action := &JobPushAction{AppCtx: appCtx}
	// The pre-map shape: a list of {name, value} entries.
	_, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"kind":   "demo",
		"inputs": []any{map[string]any{"name": "file", "value": "a.zip"}},
	}})
	if err == nil {
		t.Fatal("expected an error for a list of name/value pairs")
	}
	if !strings.Contains(err.Error(), "inputs") {
		t.Errorf("error %q should name the inputs parameter", err.Error())
	}
}

func TestJobPushAction_TtlSecondsMustBeANumber(t *testing.T) {
	targetDir := t.TempDir()
	appCtx, _ := newTestAppContext(t, targetDir)

	action := &JobPushAction{AppCtx: appCtx}
	_, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"kind":       "demo",
		"ttlSeconds": "not a number",
	}})
	if err == nil {
		t.Fatal("expected an error for a non-numeric ttlSeconds")
	}
}

func TestJobPushAction_SetupAndCleanupMustBeBooleans(t *testing.T) {
	appCtx, _ := jobDefinitionsAppCtx(nbaPipelineDefs())
	action := &JobPushAction{AppCtx: appCtx}

	// The pre-boolean shape: a JobDefinition id naming the step to run.
	for _, key := range []string{"setup", "cleanup"} {
		t.Run(key, func(t *testing.T) {
			_, err := action.Run(&workflows.StepContext{Params: map[string]any{
				"kind": "nba-apply",
				key:    "nba-transformation",
			}})
			if err == nil {
				t.Fatal("expected an error for a non-boolean value")
			}
			if !strings.Contains(err.Error(), key) {
				t.Errorf("error %q should name the %s parameter", err.Error(), key)
			}
		})
	}
}

// A sequenced Job's setup and cleanup sit outside the sequence. Without
// the exemption in lib/jobs.isSetupOrCleanup, setup would claim slot 0 and
// leave every ordinary step waiting on a Current that nothing advances.
func TestJobPushAction_SequentialWithSetupAndCleanupStillRuns(t *testing.T) {
	appCtx, backend := jobDefinitionsAppCtx(nbaPipelineDefs())

	action := &JobPushAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"kind":       "nba-apply",
		"sequential": true,
		"setup":      true,
		"partials": []any{
			map[string]any{"kind": "nba-transaction-step"},
		},
	}}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	setup, err := backend.Take("nba-apply:setup", "test")
	if err != nil || setup == nil {
		t.Fatalf("Take(setup): %v, %+v", err, setup)
	}
	if setup.Sequence != nil {
		t.Errorf("setup claimed sequence slot %d, want none", *setup.Sequence)
	}
	if err := backend.Done(setup.Id); err != nil {
		t.Fatalf("Done(setup): %v", err)
	}

	step, err := backend.Take("nba-transaction-step", "test")
	if err != nil || step == nil {
		t.Fatalf("Take(step) after setup finished: %v, %+v - the first step must be takeable", err, step)
	}
	if step.Sequence == nil || *step.Sequence != 0 {
		t.Errorf("step: Sequence = %v, want slot 0", step.Sequence)
	}
}
