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
	if !job.Parallel {
		t.Error("expected Parallel to default to true")
	}

	step, err := backend.Take("nba-transformation", "test")
	if err != nil || step == nil {
		t.Fatalf("Take(nba-transformation): %v, %+v", err, step)
	}
	if step.PartOf != job.ID || step.Sequence != 0 {
		t.Errorf("step: PartOf=%q Sequence=%d, want %q/0", step.PartOf, step.Sequence, job.ID)
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
