package actions

import (
	"path/filepath"
	"testing"

	"github.com/ldproxy/xtralink/lib/workflows"
)

func TestPullAction_PullsAndExposesLocalPath(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	writeFile(t, filepath.Join(foo.URL, "a.zip"), "a")
	appCtx, _ := newTestAppContext(t, targetDir, foo)

	action := &PullAction{AppCtx: appCtx}
	result, err := action.Run(&workflows.StepContext{Params: map[string]any{"pkg": "foo"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(result.Outputs) != 1 {
		t.Fatalf("expected exactly 1 output set, got %+v", result.Outputs)
	}
	if got := result.Outputs[0]["path"]; got != foo.ResolvedLocalPath {
		t.Errorf("path = %v, want %v", got, foo.ResolvedLocalPath)
	}

	assertFileContent(t, filepath.Join(foo.ResolvedLocalPath, "a.zip"), "a")
}

func TestPullAction_MissingPkgParamIsError(t *testing.T) {
	targetDir := t.TempDir()
	appCtx, _ := newTestAppContext(t, targetDir)

	action := &PullAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{}}); err == nil {
		t.Fatal("expected an error for a missing pkg parameter")
	}
}

func TestPullAction_UnknownPackageIsError(t *testing.T) {
	targetDir := t.TempDir()
	appCtx, _ := newTestAppContext(t, targetDir)

	action := &PullAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{"pkg": "does-not-exist"}}); err == nil {
		t.Fatal("expected an error for an unknown package")
	}
}
