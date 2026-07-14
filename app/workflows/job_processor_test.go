package workflows

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/ldproxy/xtralink/app"
	appjobs "github.com/ldproxy/xtralink/app/jobs"
	"github.com/ldproxy/xtralink/lib/drivers"
	"github.com/ldproxy/xtralink/lib/jobs"
	"github.com/ldproxy/xtralink/lib/lock"
	"github.com/ldproxy/xtralink/lib/workflows"
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
targetDir: ` + targetDir + `
packages:
  - id: foo
    type: FS
    url: ` + fooRemote + `

workflows:
  - id: nba-transform
    steps:
      - id: found
        action: pkg:find_any
        pkg: foo
        path: "*.zip"
  - id: nba-transaction
    params:
      - name: foo
        type: string
        required: true
    steps: []

jobDefinitions:
  - id: nba-pipeline
    parallel: false
    steps:
      - id: nba-transformation
        workflow: nba-transform
        outputs:
          foo: ${outputs.found.path}
      - id: nba-transaction-step
        workflow: nba-transaction
        parameters:
          foo: ${parent.outputs.foo}
        outputs:
          bar: ${params.foo}
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

	job, err := appjobs.Push(appCtx, "nba-pipeline", "", 1000, "")
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if job.Parallel {
		t.Error("expected Job.Parallel to be false (jobDefinitions[0].parallel: false)")
	}

	step1, err := NewWorkflowJobProcessor(appCtx, "nba-transformation")
	if err != nil {
		t.Fatalf("NewWorkflowJobProcessor(nba-transformation): %v", err)
	}
	step2, err := NewWorkflowJobProcessor(appCtx, "nba-transaction-step")
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
	var final *jobs.Job
	for time.Now().Before(deadline) {
		current, err := backend.GetJob(job.ID)
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
	if final.Status() != jobs.StatusSuccessful {
		t.Fatalf("Status() = %s, want successful (errors=%v)", final.Status(), final.Errors)
	}
	if final.Current != 2 || final.Total != 2 {
		t.Errorf("Current/Total = %d/%d, want 2/2 (one per step)", final.Current, final.Total)
	}

	fooOut, ok := final.Outputs["foo"]
	if !ok || fooOut.Value != "a.zip" {
		t.Errorf("Outputs[foo] = %+v, want value a.zip", fooOut)
	}
	barOut, ok := final.Outputs["bar"]
	if !ok || barOut.Value != "a.zip" {
		t.Errorf("Outputs[bar] = %+v, want value a.zip (relayed via ${parent.outputs.foo} -> params.foo)", barOut)
	}
}

// TestWorkflowJobProcessor_MissingRequiredParamIsError confirms explicit
// mode still enforces the referenced Workflow's required params - a
// parameters mapping that doesn't cover one is a runtime error, not a
// silent gap.
func TestWorkflowJobProcessor_MissingRequiredParamIsError(t *testing.T) {
	targetDir := t.TempDir()
	config := `
targetDir: ` + targetDir + `
packages:
  - id: foo
    type: FS
    url: ` + t.TempDir() + `

workflows:
  - id: needs-param
    params:
      - name: required-thing
        type: string
        required: true
    steps: []

jobDefinitions:
  - id: pipeline
    steps:
      - id: step-a
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
	partialJob := jobs.NewPartialJob("partial-1", "step-a", 1000, job.ID)
	if err := backend.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	processor, err := NewWorkflowJobProcessor(appCtx, "step-a")
	if err != nil {
		t.Fatalf("NewWorkflowJobProcessor: %v", err)
	}

	taken, err := backend.Take("step-a", "test")
	if err != nil || taken == nil {
		t.Fatalf("Take: %v, %+v", err, taken)
	}
	gotJob, err := backend.GetJob(job.ID)
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
		Workflows: []workflows.Workflow{{Id: "wf"}},
		JobDefinitions: []app.JobDefinition{{
			Id:    "pipeline",
			Steps: []app.JobStepDefinition{{Id: "step-a", Workflow: "wf"}},
		}},
	}}
	processor, err := NewWorkflowJobProcessor(appCtx, "step-a")
	if err != nil {
		t.Fatalf("NewWorkflowJobProcessor: %v", err)
	}

	partialJob := jobs.NewPartialJob("partial-1", "step-a", 1000, "missing-job-id")
	result := processor.Process(partialJob, nil, jobs.NewMemoryBackend())
	if !result.IsFailure() {
		t.Fatal("expected a failure result instead of a panic when job is nil")
	}
}
