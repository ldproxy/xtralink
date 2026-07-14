package jobs

import (
	"errors"
	"testing"

	"github.com/ldproxy/xtralink/app"
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
