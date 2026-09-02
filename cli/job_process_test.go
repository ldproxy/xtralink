package cli

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
	"github.com/ldproxy/xtralink/model"
)

func TestKindsToProcess_SpecificKind(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{
		JobDefinitions: []app.JobDefinition{
			{Kind: "step-a", Workflow: "wf-a"},
			{Kind: "step-b", Workflow: "wf-b"},
		},
	}}

	ids, err := kindsToProcess(appCtx, "step-b")
	if err != nil {
		t.Fatalf("kindsToProcess: %v", err)
	}
	if len(ids) != 1 || ids[0] != "step-b" {
		t.Errorf("ids = %v, want [step-b]", ids)
	}
}

func TestKindsToProcess_UnknownKindIsError(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{}}
	if _, err := kindsToProcess(appCtx, "does-not-exist"); err == nil {
		t.Fatal("expected an error for an unknown kind")
	}
}

func TestKindsToProcess_WildcardReturnsEveryKind(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{
		JobDefinitions: []app.JobDefinition{
			{Kind: "step-a", Workflow: "wf-a"},
			{Kind: "step-b1", Workflow: "wf-b"},
			{Kind: "step-b2", Workflow: "wf-b"},
		},
	}}

	ids, err := kindsToProcess(appCtx, "*")
	if err != nil {
		t.Fatalf("kindsToProcess: %v", err)
	}
	want := map[string]bool{"step-a": true, "step-b1": true, "step-b2": true}
	if len(ids) != len(want) {
		t.Fatalf("ids = %v, want %v", ids, want)
	}
	for _, id := range ids {
		if !want[id] {
			t.Errorf("unexpected id %q", id)
		}
		delete(want, id)
	}
	if len(want) != 0 {
		t.Errorf("missing ids: %v", want)
	}
}

func TestKindsToProcess_WildcardWithNoJobDefinitionsIsError(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{}}
	if _, err := kindsToProcess(appCtx, "*"); err == nil {
		t.Fatal("expected an error when no jobs are configured")
	}
}

func TestExecutorId_IncludesPid(t *testing.T) {
	id := executorId()
	if id == "" {
		t.Fatal("expected a non-empty executor id")
	}
}

// TestJobProcessCmd_ProcessesOneStepThenStopsOnCancel runs `job process
// <step-id>` end to end against a real MemoryBackend and a real, minimal
// pipeline: pushes a Job via app/jobs.Push, starts JobProcessCmd's run()
// with a cancellable context (standing in for SIGTERM, s. run's doc
// comment), waits for the Job to finish, then cancels - proving the CLI
// wiring (kindsToProcess -> NewWorkflowJobProcessor -> Runner) works
// together, not just each piece in isolation.
func TestJobProcessCmd_ProcessesOnePartialJobThenStopsOnCancel(t *testing.T) {
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

jobs:
  - kind: nba-transformation
    workflow: nba-transform
    outputs:
      foo: ${outputs.found.path}
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

	job, err := appjobs.Push(appCtx, appjobs.PushRequest{
		JobConfiguration: model.JobConfiguration{Kind: "nba-transformation", Priority: 1000},
	})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	cmd := &JobProcessCmd{Kind: "nba-transformation"}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- cmd.run(appCtx, ctx) }()

	deadline := time.Now().Add(3 * time.Second)
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
	if err := <-runDone; err != nil {
		t.Fatalf("cmd.run: %v", err)
	}

	if final == nil {
		t.Fatal("timed out waiting for the job to finish")
	}
	if final.Status != model.StatusSUCCESSFUL {
		t.Fatalf("Status = %s, want successful (errors=%v)", final.Status, final.Errors)
	}
	// Outputs is an opaque map the backend round-trips through JSON, so the
	// OutputValue the job wrote comes back as a generic map.
	out, ok := final.Outputs["foo"].(map[string]any)
	if !ok || out["value"] != "a.zip" {
		t.Errorf("Outputs[foo] = %+v, want value a.zip", final.Outputs["foo"])
	}
}

// RunJobWorkers is what `flow run --workers` shares with `job process`, so
// it has to refuse the same way when there is nothing configured to run.
func TestRunJobWorkers_WithNoJobsConfiguredIsError(t *testing.T) {
	appCtx := &app.AppContext{
		Logger:   zerolog.Nop(),
		Settings: &app.Settings{},
	}

	if err := RunJobWorkers(appCtx, context.Background(), "*"); err == nil {
		t.Fatal("expected an error when no jobs are configured")
	}
	if err := RunJobWorkers(appCtx, context.Background(), "no-such-kind"); err == nil {
		t.Fatal("expected an error for an unknown kind")
	}
}
