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
	if job.Type != "demo" || job.Label != "my-label" || job.Priority != 500 {
		t.Errorf("unexpected Job fields: %+v", job)
	}
	if string(job.Inputs) != `{"foo":"bar"}` {
		t.Errorf("Inputs = %s, want {\"foo\":\"bar\"}", job.Inputs)
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
		t.Errorf("expected nil Inputs for empty inputsRaw, got %s", job.Inputs)
	}
}

func TestPush_WrapsBackendError(t *testing.T) {
	backend := &fakeBackend{pushJobErr: errors.New("boom")}
	appCtx := &app.AppContext{Jobs: backend}

	if _, err := Push(appCtx, "demo", "", 1000, ""); err == nil {
		t.Fatal("expected an error to be returned")
	}
}

func TestPush_BuildsMultiStepPipelineFromJobDefinition(t *testing.T) {
	backend := jobs.NewMemoryBackend()
	appCtx := &app.AppContext{
		Jobs: backend,
		Settings: &app.Settings{
			JobDefinitions: []app.JobDefinition{
				{
					Id: "nba-pipeline",
					Steps: []app.JobStepDefinition{
						{Id: "nba-transformation", Workflow: "nba-transform"},
						{Id: "nba-transaction-step", Workflow: "nba-transaction"},
					},
				},
			},
		},
	}

	job, err := Push(appCtx, "nba-pipeline", "my label", 500, "")
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if !job.Parallel {
		t.Error("expected Parallel to default to true")
	}

	step0, err := backend.Take("nba-transformation", "test")
	if err != nil || step0 == nil {
		t.Fatalf("Take(nba-transformation): %v, %+v", err, step0)
	}
	if step0.PartOf != job.ID || step0.Sequence != 0 {
		t.Errorf("step0: PartOf=%q Sequence=%d, want %q/0", step0.PartOf, step0.Sequence, job.ID)
	}

	// Parallel defaults to true here (no `parallel: false` set), so step1
	// must be immediately takeable too, not gated behind step0.
	step1, err := backend.Take("nba-transaction-step", "test")
	if err != nil || step1 == nil {
		t.Fatalf("Take(nba-transaction-step): %v, %+v", err, step1)
	}
	if step1.Sequence != 1 {
		t.Errorf("step1.Sequence = %d, want 1", step1.Sequence)
	}
}

func TestPush_SequentialPipelineGatesLaterSteps(t *testing.T) {
	backend := jobs.NewMemoryBackend()
	parallel := false
	appCtx := &app.AppContext{
		Jobs: backend,
		Settings: &app.Settings{
			JobDefinitions: []app.JobDefinition{
				{
					Id:       "nba-pipeline",
					Parallel: &parallel,
					Steps: []app.JobStepDefinition{
						{Id: "step-a", Workflow: "wf-a"},
						{Id: "step-b", Workflow: "wf-b"},
					},
				},
			},
		},
	}

	job, err := Push(appCtx, "nba-pipeline", "", 1000, "")
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if job.Parallel {
		t.Error("expected Parallel to be false")
	}

	if taken, err := backend.Take("step-b", "test"); err != nil || taken != nil {
		t.Fatalf("Take(step-b) before step-a is done = %+v, %v, want nil, nil", taken, err)
	}

	taken, err := backend.Take("step-a", "test")
	if err != nil || taken == nil {
		t.Fatalf("Take(step-a): %v, %+v", err, taken)
	}
}

func TestPush_UnknownTypeStaysBareJob(t *testing.T) {
	backend := jobs.NewMemoryBackend()
	appCtx := &app.AppContext{Jobs: backend, Settings: &app.Settings{}}

	job, err := Push(appCtx, "ad-hoc-type", "", 1000, "")
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if job.ID == "" {
		t.Error("expected a valid job")
	}
	if taken, err := backend.Take("ad-hoc-type", "test"); err != nil || taken != nil {
		t.Fatalf("expected no PartialJob queued for a bare job type, got %+v, %v", taken, err)
	}
}
