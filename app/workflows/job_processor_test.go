package workflows

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/ldproxy/xtralink/app"
	appjobs "github.com/ldproxy/xtralink/app/jobs"
	"github.com/ldproxy/xtralink/lib/drivers"
	"github.com/ldproxy/xtralink/lib/jobs"
	"github.com/ldproxy/xtralink/lib/lock"
	"github.com/ldproxy/xtralink/lib/workflows"
	"github.com/ldproxy/xtralink/model"
)

// TestWorkflowJobProcessor_TwoStepPipelineWithImplicitAndExplicitInputs runs
// the concept's nba-transformation/nba-transaction example end to end:
// step 1 (implicit input mapping - no `parameters:`, nothing to map since
// nba-transform declares no params) finds a file and exposes its path as an
// output; step 2 (explicit input mapping via `${parent.outputs.foo}`, the
// shared Job's own output written by step 1) has no steps of its own and
// simply relays that value into its own output. Proves: implicit vs.
// explicit input-mapping mode selection, ${parent.outputs...} resolving
// against the shared Job (not a separate mechanism), and the output
// mapping writing into the one Job both PartialJobs belong to.
func TestWorkflowJobProcessor_TwoStepPipelineWithImplicitAndExplicitInputs(t *testing.T) {
	targetDir := t.TempDir()
	fooRemote := t.TempDir()
	if err := os.WriteFile(filepath.Join(fooRemote, "a.zip"), []byte("a"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	config := `
settings:
  targetDir: ` + targetDir + `
packages:
  - id: foo
    type: FS
    url: ` + fooRemote + `

workflows:
  - id: nba-transform
    steps:
      - action: pkg:pull
        pkg: foo
      - id: found
        action: pkg:find_any
        pkg: foo
        path: "*.zip"
  - id: nba-transaction
    parameters:
      - name: foo
        type: string
        required: true
    steps: []

jobs:
  - kind: nba-transformation
    workflow: nba-transform
    outputs:
      foo: ${outputs.found.path}
  - kind: nba-transaction-step
    workflow: nba-transaction
    parameters:
      foo: ${parent.outputs.foo}
    outputs:
      bar: ${parameters.foo}
`
	configPath := filepath.Join(t.TempDir(), ".xtrasync.yml")
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatalf("WriteFile config: %v", err)
	}

	settings, err := app.LoadSettings(configPath)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}

	backend := jobs.NewMemoryBackend()
	appCtx := &app.AppContext{
		Logger:   zerolog.Nop(),
		Settings: settings,
		Drivers:  drivers.NewFactory(),
		Jobs:     backend,
		Locks:    lock.NoopLocker{},
	}

	// A multi-step Job is only ever built ad-hoc these days (s.
	// job:push's `partials:`, app/workflows/actions/job_push.go) - there is
	// no more pre-declared multi-step pipeline in jobs itself, so
	// this test builds the same shape PushPipeline directly, exactly like
	// that action does.
	def1, err := settings.GetJobDefinition("nba-transformation")
	if err != nil {
		t.Fatalf("GetJobDefinition(nba-transformation): %v", err)
	}
	def2, err := settings.GetJobDefinition("nba-transaction-step")
	if err != nil {
		t.Fatalf("GetJobDefinition(nba-transaction-step): %v", err)
	}
	job, err := appjobs.Push(appCtx, appjobs.PushRequest{
		JobConfiguration: model.JobConfiguration{Kind: "nba-apply", Priority: 1000},
		Partials:         []app.JobDefinition{*def1, *def2},
		Sequential:       true,
	})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if job.Sequence == nil {
		t.Error("expected Sequential to opt the Job into sequencing")
	}

	step1, err := WorkflowJobProcessor(appCtx, "nba-transformation")
	if err != nil {
		t.Fatalf("NewWorkflowJobProcessor(nba-transformation): %v", err)
	}
	step2, err := WorkflowJobProcessor(appCtx, "nba-transaction-step")
	if err != nil {
		t.Fatalf("NewWorkflowJobProcessor(nba-transaction-step): %v", err)
	}

	r := jobs.NewRunner(backend, "test")
	r.PollInterval = 5 * time.Millisecond
	var runnerErrs []error
	r.OnError = func(err error) { runnerErrs = append(runnerErrs, err) }
	r.Register(step1)
	r.Register(step2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runnerDone := make(chan error, 1)
	go func() { runnerDone <- r.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	var final *model.Job
	for time.Now().Before(deadline) {
		current, err := backend.GetJob(job.Id)
		if err != nil {
			t.Fatalf("GetJob: %v", err)
		}
		if current != nil && current.FinishedAt > 0 {
			final = current
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-runnerDone

	for _, err := range runnerErrs {
		t.Errorf("runner error: %v", err)
	}
	if final == nil {
		t.Fatal("timed out waiting for the job to finish")
	}
	if final.Status != model.StatusSUCCESSFUL {
		t.Fatalf("Status = %s, want successful (errors=%v)", final.Status, final.Errors)
	}
	if final.Progress.Current != 2 || final.Progress.Total != 2 {
		t.Errorf("Current/Total = %d/%d, want 2/2 (one per step)", final.Progress.Current, final.Progress.Total)
	}

	// Outputs is an opaque map the backend round-trips through JSON, so each
	// entry comes back as a generic OutputValue-shaped map - outputValues
	// unwraps exactly that (the same way ${parent.outputs...} resolution
	// sees it).
	outs := outputValues(final.Outputs)
	if outs["foo"] != "a.zip" {
		t.Errorf("Outputs[foo] = %+v, want value a.zip", outs["foo"])
	}
	if outs["bar"] != "a.zip" {
		t.Errorf("Outputs[bar] = %+v, want value a.zip (relayed via ${parent.outputs.foo} -> params.foo)", outs["bar"])
	}
}

// TestWorkflowJobProcessor_MissingRequiredParamIsError confirms explicit
// mode still enforces the referenced Workflow's required params - a
// parameters mapping that doesn't cover one is a runtime error, not a
// silent gap.
func TestWorkflowJobProcessor_MissingRequiredParamIsError(t *testing.T) {
	targetDir := t.TempDir()
	config := `
settings:
  targetDir: ` + targetDir + `
packages:
  - id: foo
    type: FS
    url: ` + t.TempDir() + `

workflows:
  - id: needs-param
    parameters:
      - name: required-thing
        type: string
        required: true
    steps: []

jobs:
  - kind: step-a
    workflow: needs-param
    parameters:
      unrelated: "value"
`
	configPath := filepath.Join(t.TempDir(), ".xtrasync.yml")
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatalf("WriteFile config: %v", err)
	}

	settings, err := app.LoadSettings(configPath)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}

	backend := jobs.NewMemoryBackend()
	appCtx := &app.AppContext{
		Logger:   zerolog.Nop(),
		Settings: settings,
		Drivers:  drivers.NewFactory(),
		Jobs:     backend,
		Locks:    lock.NoopLocker{},
	}

	job := jobs.NewJob("job-1", "pipeline", 1000, "", nil)
	if err := backend.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	partialJob := jobs.NewPartialJob("partial-1", "step-a", 1000, job.Id)
	if err := backend.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	processor, err := WorkflowJobProcessor(appCtx, "step-a")
	if err != nil {
		t.Fatalf("NewWorkflowJobProcessor: %v", err)
	}

	taken, err := backend.Take("step-a", "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}
	gotJob, err := backend.GetJob(job.Id)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	result := processor.Process(taken, gotJob, backend)
	if !result.IsFailure() {
		t.Fatal("expected a failure result for a missing required parameter")
	}
}

// TestWorkflowJobProcessor_NilJobFailsCleanly is a regression test: found
// via manual verification, where a PartialJob whose parent Job had already
// been deleted (a legitimate scenario, s. tileSeedingSetupProcessor's same
// guard) made the Runner pass job=nil into Process, which then panicked
// deep inside resolveImplicitParams instead of failing the PartialJob
// cleanly.
func TestWorkflowJobProcessor_NilJobFailsCleanly(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{
		Workflows:      []workflows.Workflow{{Id: "wf"}},
		JobDefinitions: []app.JobDefinition{{Kind: "step-a", Workflow: "wf"}},
	}}
	processor, err := WorkflowJobProcessor(appCtx, "step-a")
	if err != nil {
		t.Fatalf("NewWorkflowJobProcessor: %v", err)
	}

	partialJob := jobs.NewPartialJob("partial-1", "step-a", 1000, "missing-job-id")
	result := processor.Process(partialJob, nil, jobs.NewMemoryBackend())
	if !result.IsFailure() {
		t.Fatal("expected a failure result instead of a panic when job is nil")
	}
}

// The Job's id is the one thing a workflow cannot work out for itself, so
// an explicit parameter mapping can name it as ${parent.id}.
func TestWorkflowJobProcessor_ExplicitParamsCanReferenceTheJobId(t *testing.T) {
	config := `
settings:
  targetDir: ` + t.TempDir() + `
packages:
  - id: foo
    type: FS
    url: ` + t.TempDir() + `

workflows:
  - id: echoes-the-job
    parameters:
      - name: job
        type: string
        required: true
    steps:
      - id: only
        action: pkg:pull
        pkg: foo

jobs:
  - kind: step-a
    workflow: echoes-the-job
    parameters:
      job: ${parent.id}
    outputs:
      seen: ${parameters.job}
`
	configPath := filepath.Join(t.TempDir(), ".xtrasync.yml")
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatalf("WriteFile config: %v", err)
	}
	settings, err := app.LoadSettings(configPath)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}

	backend := jobs.NewMemoryBackend()
	appCtx := &app.AppContext{
		Logger:   zerolog.Nop(),
		Settings: settings,
		Drivers:  drivers.NewFactory(),
		Jobs:     backend,
		Locks:    lock.NoopLocker{},
	}

	job := jobs.NewJob("job-42", "pipeline", 1000, "", nil)
	if err := backend.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	partialJob := jobs.NewPartialJob("partial-1", "step-a", 1000, job.Id)
	partialJob.Progress.Total = 1
	if err := backend.InitJob(job.Id, 1, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	if err := backend.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	processor, err := WorkflowJobProcessor(appCtx, "step-a")
	if err != nil {
		t.Fatalf("WorkflowJobProcessor: %v", err)
	}
	taken, err := backend.Take("step-a", "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}

	if result := processor.Process(taken, job, backend); !result.IsSuccess() {
		t.Fatalf("Process: %+v", result)
	}

	stored, err := backend.GetJob(job.Id)
	if err != nil || stored == nil {
		t.Fatalf("GetJob: %v, %+v", err, stored)
	}
	outs := outputValues(stored.Outputs)
	if outs["seen"] != "job-42" {
		t.Errorf("Outputs[seen] = %+v, want job-42 (relayed via ${parent.id} -> params.job)", outs["seen"])
	}
}

// An explicit parameter mapping stops the Job's Inputs being auto-filled by
// name, so ${parent.inputs...} is how it picks out the ones it wants.
func TestWorkflowJobProcessor_ExplicitParamsCanPickOutJobInputs(t *testing.T) {
	appCtx, backend := parentVarsAppCtx(t, `
    parameters:
      job: ${parent.id}
      wanted: ${parent.inputs.wanted}
    outputs:
      seen: ${parameters.wanted}
`)

	job := jobs.NewJob("job-7", "pipeline", 1000, "", map[string]any{
		"wanted":  "keep-me",
		"ignored": "drop-me",
	})
	runOnePartialJob(t, appCtx, backend, job)

	stored, err := backend.GetJob(job.Id)
	if err != nil || stored == nil {
		t.Fatalf("GetJob: %v, %+v", err, stored)
	}
	if outs := outputValues(stored.Outputs); outs["seen"] != "keep-me" {
		t.Errorf("Outputs[seen] = %+v, want keep-me (via ${parent.inputs.wanted})", outs["seen"])
	}
}

// Both of a definition's mappings resolve against the same vocabulary, so
// an output mapping can name ${parent...} exactly as the input mapping does.
func TestWorkflowJobProcessor_OutputMappingSeesParentToo(t *testing.T) {
	appCtx, backend := parentVarsAppCtx(t, `
    parameters:
      job: ${parent.id}
      wanted: ${parent.inputs.wanted}
    outputs:
      forJob: ${parent.id}
      relayed: ${parent.inputs.wanted}
      fromStep: ${outputs.only.path}
`)

	job := jobs.NewJob("job-9", "pipeline", 1000, "", map[string]any{"wanted": "keep-me"})
	wantPath := appCtx.Settings.Packages[0].ResolvedLocalPath
	runOnePartialJob(t, appCtx, backend, job)

	stored, err := backend.GetJob(job.Id)
	if err != nil || stored == nil {
		t.Fatalf("GetJob: %v, %+v", err, stored)
	}
	outs := outputValues(stored.Outputs)
	if outs["forJob"] != "job-9" {
		t.Errorf("Outputs[forJob] = %+v, want job-9", outs["forJob"])
	}
	if outs["relayed"] != "keep-me" {
		t.Errorf("Outputs[relayed] = %+v, want keep-me", outs["relayed"])
	}
	if outs["fromStep"] != wantPath {
		t.Errorf("Outputs[fromStep] = %+v, want %q - the run's own step outputs must still resolve", outs["fromStep"], wantPath)
	}
}

// parentVarsAppCtx builds an AppContext around one job definition whose
// mappings are given as the yaml fragment appended under it.
func parentVarsAppCtx(t *testing.T, mappings string) (*app.AppContext, *jobs.MemoryBackend) {
	t.Helper()

	config := `
settings:
  targetDir: ` + t.TempDir() + `
packages:
  - id: foo
    type: FS
    url: ` + t.TempDir() + `

workflows:
  - id: echoes-the-job
    parameters:
      - name: job
        type: string
        required: true
      - name: wanted
        type: string
    steps:
      - id: only
        action: pkg:pull
        pkg: foo

jobs:
  - kind: step-a
    workflow: echoes-the-job` + mappings

	configPath := filepath.Join(t.TempDir(), ".xtrasync.yml")
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatalf("WriteFile config: %v", err)
	}
	settings, err := app.LoadSettings(configPath)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}

	backend := jobs.NewMemoryBackend()
	return &app.AppContext{
		Logger:   zerolog.Nop(),
		Settings: settings,
		Drivers:  drivers.NewFactory(),
		Jobs:     backend,
		Locks:    lock.NoopLocker{},
	}, backend
}

// runOnePartialJob pushes job with a single step-a PartialJob and runs it
// through the processor, failing the test if it does not succeed.
func runOnePartialJob(t *testing.T, appCtx *app.AppContext, backend *jobs.MemoryBackend, job *model.Job) {
	t.Helper()

	if err := backend.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	partialJob := jobs.NewPartialJob("partial-1", "step-a", 1000, job.Id)
	partialJob.Progress.Total = 1
	if err := backend.InitJob(job.Id, 1, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	if err := backend.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	processor, err := WorkflowJobProcessor(appCtx, "step-a")
	if err != nil {
		t.Fatalf("WorkflowJobProcessor: %v", err)
	}
	taken, err := backend.Take("step-a", "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}
	if result := processor.Process(taken, job, backend); !result.IsSuccess() {
		t.Fatalf("Process: %+v", result)
	}
}

// A cleanup PartialJob runs whether the Job succeeded or failed, so
// ${parent.status} and ${parent.errors} are how its mapping can tell which.
func TestWorkflowJobProcessor_ExplicitParamsSeeTheJobStatusAndErrors(t *testing.T) {
	appCtx, backend := parentVarsAppCtx(t, `
    parameters:
      job: ${parent.id}
      wanted: ${parent.status}
    outputs:
      status: ${parameters.wanted}
      errors: ${parent.errors}
      errorsText: ${parent.errorsString}
`)

	// The realistic shape for these two: a fan-out where an earlier part
	// already failed while this one still runs (s. lib/jobs.pushFollowUps
	// on why a fan-out carries on).
	job := jobs.NewJob("job-11", "pipeline", 1000, "", nil)
	job.StartedAt = 1
	job.Errors = []string{"an earlier part blew up"}
	runOnePartialJob(t, appCtx, backend, job)

	stored, err := backend.GetJob(job.Id)
	if err != nil || stored == nil {
		t.Fatalf("GetJob: %v, %+v", err, stored)
	}
	outs := outputValues(stored.Outputs)

	if outs["errorsText"] != "an earlier part blew up" {
		t.Errorf("Outputs[errorsText] = %+v, want the joined errors", outs["errorsText"])
	}

	// Started and carrying an error, but not finished - so RUNNING.
	if outs["status"] != string(model.StatusRUNNING) {
		t.Errorf("Outputs[status] = %+v, want %v", outs["status"], model.StatusRUNNING)
	}
	// A whole placeholder keeps the list rather than stringifying it.
	list, ok := outs["errors"].([]any)
	if !ok || len(list) != 1 || list[0] != "an earlier part blew up" {
		t.Errorf("Outputs[errors] = %#v, want the error list as-is", outs["errors"])
	}
}

func TestParentVars_ReportsStatusAndErrors(t *testing.T) {
	job := jobs.NewJob("job-12", "pipeline", 1000, "", nil)

	fresh := parentVars(job)
	if fresh["status"] != string(model.StatusACCEPTED) {
		t.Errorf("status = %v, want ACCEPTED for a job that never started", fresh["status"])
	}
	if errs, ok := fresh["errors"].([]string); !ok || len(errs) != 0 {
		t.Errorf("errors = %#v, want an empty list", fresh["errors"])
	}
	if fresh["errorsString"] != "" {
		t.Errorf("errorsString = %q, want empty when there are no errors", fresh["errorsString"])
	}

	job.StartedAt = 1
	job.FinishedAt = 2
	job.Errors = []string{"first", "second"}
	failed := parentVars(job)
	if failed["status"] != string(model.StatusFAILED) {
		t.Errorf("status = %v, want FAILED", failed["status"])
	}
	if errs, _ := failed["errors"].([]string); len(errs) != 2 {
		t.Errorf("errors = %#v, want both", failed["errors"])
	}
	if failed["errorsString"] != "first;;;second" {
		t.Errorf("errorsString = %q, want the errors joined with the separator", failed["errorsString"])
	}
}

// A single error can itself contain commas and newlines - a failing
// cmd:exec carries its output tail - so the joined form has to stay
// splittable on the separator alone.
func TestParentVars_ErrorsStringSurvivesMultilineErrors(t *testing.T) {
	job := jobs.NewJob("job-13", "pipeline", 1000, "", nil)
	job.Errors = []string{"first failed\nlast output:\na, b, c", "second failed"}

	joined, _ := parentVars(job)["errorsString"].(string)
	parts := strings.Split(joined, errorsSeparator)
	if len(parts) != 2 {
		t.Fatalf("split %q into %d parts, want 2", joined, len(parts))
	}
	if !strings.Contains(parts[0], "a, b, c") || parts[1] != "second failed" {
		t.Errorf("parts = %#v, want each error back whole", parts)
	}
}
