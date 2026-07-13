package actions

import (
	"path/filepath"
	"testing"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/workflows"
)

func TestPushAction_SyncsLocalChangesToRemote(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	writeFile(t, filepath.Join(foo.ResolvedLocalPath, "a.zip"), "a")
	appCtx, _ := newTestAppContext(t, targetDir, foo)

	action := &PushAction{AppCtx: appCtx}
	result, err := action.Run(&workflows.StepContext{Params: map[string]any{"pkg": "foo"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(result.Outputs) != 1 {
		t.Fatalf("expected exactly 1 output set, got %+v", result.Outputs)
	}

	assertFileContent(t, filepath.Join(foo.URL, "a.zip"), "a")
}

func TestPushAction_MissingPkgParamIsError(t *testing.T) {
	targetDir := t.TempDir()
	appCtx, _ := newTestAppContext(t, targetDir)

	action := &PushAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{}}); err == nil {
		t.Fatal("expected an error for a missing pkg parameter")
	}
}

func TestPushAction_UnknownPackageIsError(t *testing.T) {
	targetDir := t.TempDir()
	appCtx, _ := newTestAppContext(t, targetDir)

	action := &PushAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{"pkg": "does-not-exist"}}); err == nil {
		t.Fatal("expected an error for an unknown package")
	}
}

func TestPushAction_RejectsUnsupportedPackageType(t *testing.T) {
	targetDir := t.TempDir()
	gitPkg := app.Package{Id: "gitpkg", Type: "GIT", URL: "https://example.com/repo.git", ResolvedLocalPath: filepath.Join(targetDir, "gitpkg")}
	appCtx, _ := newTestAppContext(t, targetDir, gitPkg)

	action := &PushAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{"pkg": "gitpkg"}}); err == nil {
		t.Fatal("expected an error for a GIT package")
	}
}
