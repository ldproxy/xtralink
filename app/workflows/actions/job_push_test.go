package actions

import (
	"encoding/json"
	"testing"

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
