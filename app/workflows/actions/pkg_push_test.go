package actions

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/workflows"
)

func TestPushAction_SyncsLocalChangesToRemote(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	seedMirror(t, foo, map[string]string{"a.zip": "a"})
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
	seedMirror(t, gitPkg, nil)
	appCtx, _ := newTestAppContext(t, targetDir, gitPkg)

	action := &PushAction{AppCtx: appCtx}
	_, err := action.Run(&workflows.StepContext{Params: map[string]any{"pkg": "gitpkg"}})
	if err == nil {
		t.Fatal("expected an error for a GIT package")
	}
	if !strings.Contains(err.Error(), "FS/S3") {
		t.Errorf("error %q should be about the unsupported package type", err.Error())
	}
}

// A package without a local copy points at a missing step in the workflow;
// the push refuses it and leaves the remote alone.
func TestPushAction_UnpulledPackageIsError(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	writeFile(t, filepath.Join(foo.URL, "keep-me.zip"), "precious")
	appCtx, _ := newTestAppContext(t, targetDir, foo)

	action := &PushAction{AppCtx: appCtx}
	_, err := action.Run(&workflows.StepContext{Params: map[string]any{"pkg": "foo"}})
	if err == nil {
		t.Fatal("expected an error for a package with no local mirror")
	}
	if !strings.Contains(err.Error(), "pkg:pull") {
		t.Errorf("error %q should point at the missing pkg:pull step", err.Error())
	}

	assertFileContent(t, filepath.Join(foo.URL, "keep-me.zip"), "precious")
}
