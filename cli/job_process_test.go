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
)

func TestStepIdsToProcess_SpecificId(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{
		JobDefinitions: []app.JobDefinition{{
			Id: "pipeline",
			Steps: []app.JobStepDefinition{
				{Id: "step-a", Workflow: "wf-a"},
				{Id: "step-b", Workflow: "wf-b"},
			},
		}},
	}}

	ids, err := stepIdsToProcess(appCtx, "step-b")
	if err != nil {
		t.Fatalf("stepIdsToProcess: %v", err)
	}
	if len(ids) != 1 || ids[0] != "step-b" {
		t.Errorf("ids = %v, want [step-b]", ids)
	}
}

func TestStepIdsToProcess_UnknownIdIsError(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{}}
	if _, err := stepIdsToProcess(appCtx, "does-not-exist"); err == nil {
		t.Fatal("expected an error for an unknown step id")
	}
}

func TestStepIdsToProcess_WildcardReturnsEveryStep(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{
		JobDefinitions: []app.JobDefinition{
			{Id: "pipeline-a", Steps: []app.JobStepDefinition{{Id: "step-a", Workflow: "wf-a"}}},
			{Id: "pipeline-b", Steps: []app.JobStepDefinition{{Id: "step-b1", Workflow: "wf-b"}, {Id: "step-b2", Workflow: "wf-b"}}},
		},
	}}

	ids, err := stepIdsToProcess(appCtx, "*")
	if err != nil {
		t.Fatalf("stepIdsToProcess: %v", err)
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

func TestStepIdsToProcess_WildcardWithNoJobDefinitionsIsError(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{}}
	if _, err := stepIdsToProcess(appCtx, "*"); err == nil {
		t.Fatal("expected an error when no jobDefinitions are configured")
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
// wiring (stepIdsToProcess -> NewWorkflowJobProcessor -> Runner) works
// together, not just each piece in isolation.
func TestJobProcessCmd_ProcessesOneStepThenStopsOnCancel(t *testing.T) {
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

jobDefinitions:
  - id: nba-pipeline
    steps:
      - id: nba-transformation
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

	job, err := appjobs.Push(appCtx, "nba-pipeline", "", 1000, "")
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	cmd := &JobProcessCmd{Id: "nba-transformation"}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- cmd.run(appCtx, ctx) }()

	deadline := time.Now().Add(3 * time.Second)
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
	if err := <-runDone; err != nil {
		t.Fatalf("cmd.run: %v", err)
	}

	if final == nil {
		t.Fatal("timed out waiting for the job to finish")
	}
	if final.Status() != jobs.StatusSuccessful {
		t.Fatalf("Status() = %s, want successful (errors=%v)", final.Status(), final.Errors)
	}
	if out, ok := final.Outputs["foo"]; !ok || out.Value != "a.zip" {
		t.Errorf("Outputs[foo] = %+v, want value a.zip", out)
	}
}
