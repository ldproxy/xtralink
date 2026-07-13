package jobs

import (
	"errors"
	"testing"

	"github.com/ldproxy/xtralink/app"
)

func TestPush_RejectsInvalidJSON(t *testing.T) {
	backend := &fakeBackend{}
	appCtx := &app.AppContext{Jobs: backend}

	if _, err := Push(appCtx, "demo", "label", "entity", 1000, "{not json"); err == nil {
		t.Fatal("expected an error for invalid JSON inputs")
	}
	if backend.pushedJobSet != nil {
		t.Error("expected PushJobSet not to be called for invalid inputs")
	}
}

func TestPush_BuildsAndPushesJobSet(t *testing.T) {
	backend := &fakeBackend{}
	appCtx := &app.AppContext{Jobs: backend}

	js, err := Push(appCtx, "demo", "my-label", "my-entity", 500, `{"foo":"bar"}`)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if js.Type != "demo" || js.Label != "my-label" || js.Entity != "my-entity" || js.Priority != 500 {
		t.Errorf("unexpected JobSet fields: %+v", js)
	}
	if string(js.Inputs) != `{"foo":"bar"}` {
		t.Errorf("Inputs = %s, want {\"foo\":\"bar\"}", js.Inputs)
	}
	if backend.pushedJobSet != js {
		t.Error("expected PushJobSet to have been called with the returned JobSet")
	}
}

func TestPush_EmptyInputsStaysNil(t *testing.T) {
	backend := &fakeBackend{}
	appCtx := &app.AppContext{Jobs: backend}

	js, err := Push(appCtx, "demo", "", "", 1000, "")
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if js.Inputs != nil {
		t.Errorf("expected nil Inputs for empty inputsRaw, got %s", js.Inputs)
	}
}

func TestPush_WrapsBackendError(t *testing.T) {
	backend := &fakeBackend{pushJobSetErr: errors.New("boom")}
	appCtx := &app.AppContext{Jobs: backend}

	if _, err := Push(appCtx, "demo", "", "", 1000, ""); err == nil {
		t.Fatal("expected an error to be returned")
	}
}
