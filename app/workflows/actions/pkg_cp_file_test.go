package actions

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/workflows"
)

func TestCpFileAction_CopiesFileAndKeepsTheSource(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	bar := fsPackage(t, "bar", targetDir)
	seedMirror(t, foo, map[string]string{"sub/a.zip": "a"})
	seedMirror(t, bar, nil)
	appCtx, _ := newTestAppContext(t, targetDir, foo, bar)

	action := &CpFileAction{AppCtx: appCtx}
	result, err := action.Run(&workflows.StepContext{Params: map[string]any{"from": "foo", "to": "bar", "path": "sub/a.zip"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(result.Outputs) != 1 {
		t.Fatalf("expected exactly 1 output set, got %+v", result.Outputs)
	}

	assertFileContent(t, filepath.Join(foo.ResolvedLocalPath, "sub", "a.zip"), "a")
	assertFileContent(t, filepath.Join(bar.ResolvedLocalPath, "sub", "a.zip"), "a")
}

func TestCpFileAction_TargetPathCanDifferAndOverwrites(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	bar := fsPackage(t, "bar", targetDir)
	seedMirror(t, foo, map[string]string{"incoming/a.zip": "new"})
	seedMirror(t, bar, map[string]string{"archive/2026/renamed.zip": "old"})
	appCtx, _ := newTestAppContext(t, targetDir, foo, bar)

	action := &CpFileAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"from": "foo", "to": "bar",
		"path":       "incoming/a.zip",
		"targetPath": "archive/2026/renamed.zip",
	}}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertFileContent(t, filepath.Join(foo.ResolvedLocalPath, "incoming", "a.zip"), "new")
	assertFileContent(t, filepath.Join(bar.ResolvedLocalPath, "archive", "2026", "renamed.zip"), "new")
}

func TestCpFileAction_CopyOntoItselfKeepsTheContent(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	seedMirror(t, foo, map[string]string{"a.zip": "a"})
	appCtx, _ := newTestAppContext(t, targetDir, foo)

	action := &CpFileAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{"from": "foo", "to": "foo", "path": "a.zip"}}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertFileContent(t, filepath.Join(foo.ResolvedLocalPath, "a.zip"), "a")
}

// The copy must stay local: a workflow decides when its remotes change by
// where it puts its pkg:push steps.
func TestCpFileAction_ReachesTheTargetRemoteOncePushed(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	bar := fsPackage(t, "bar", targetDir)
	seedMirror(t, foo, map[string]string{"a.zip": "a"})
	seedMirror(t, bar, nil)
	writeFile(t, filepath.Join(foo.URL, "a.zip"), "a")
	appCtx, _ := newTestAppContext(t, targetDir, foo, bar)

	cp := &CpFileAction{AppCtx: appCtx}
	if _, err := cp.Run(&workflows.StepContext{Params: map[string]any{"from": "foo", "to": "bar", "path": "a.zip"}}); err != nil {
		t.Fatalf("cp.Run: %v", err)
	}
	assertFileMissing(t, filepath.Join(bar.URL, "a.zip"))

	push := &PushAction{AppCtx: appCtx}
	if _, err := push.Run(&workflows.StepContext{Params: map[string]any{"pkg": "bar"}}); err != nil {
		t.Fatalf("push.Run: %v", err)
	}

	assertFileContent(t, filepath.Join(foo.URL, "a.zip"), "a")
	assertFileContent(t, filepath.Join(bar.URL, "a.zip"), "a")
}

// Only the target is written to, so a source that can never be synced back
// is fine - and the same package as a target is not.
func TestCpFileAction_OnlyTheTargetMustSupportSyncBack(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	gitPkg := app.Package{Id: "gitpkg", Type: "GIT", URL: "https://example.com/repo.git", ResolvedLocalPath: filepath.Join(targetDir, "gitpkg")}
	seedMirror(t, foo, nil)
	seedMirror(t, gitPkg, map[string]string{"a.zip": "a"})
	appCtx, _ := newTestAppContext(t, targetDir, foo, gitPkg)
	action := &CpFileAction{AppCtx: appCtx}

	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{"from": "gitpkg", "to": "foo", "path": "a.zip"}}); err != nil {
		t.Fatalf("copy from a GIT package: %v", err)
	}
	assertFileContent(t, filepath.Join(foo.ResolvedLocalPath, "a.zip"), "a")

	_, err := action.Run(&workflows.StepContext{Params: map[string]any{"from": "foo", "to": "gitpkg", "path": "a.zip"}})
	if err == nil {
		t.Fatal("expected an error for a GIT target package")
	}
	if !strings.Contains(err.Error(), "FS/S3") {
		t.Errorf("error %q should be about the unsupported package type", err.Error())
	}
}

func TestCpFileAction_UnpulledSourceIsError(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	bar := fsPackage(t, "bar", targetDir)
	seedMirror(t, bar, nil) // only the target was pulled
	appCtx, _ := newTestAppContext(t, targetDir, foo, bar)

	action := &CpFileAction{AppCtx: appCtx}
	_, err := action.Run(&workflows.StepContext{Params: map[string]any{"from": "foo", "to": "bar", "path": "a.zip"}})
	if err == nil {
		t.Fatal("expected an error for an unpulled from package")
	}
	if !strings.Contains(err.Error(), "from") || !strings.Contains(err.Error(), "pkg:pull") {
		t.Errorf("error %q should name the unpulled from package", err.Error())
	}
}

func TestCpFileAction_PathsEscapingAMirrorAreRejected(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	bar := fsPackage(t, "bar", targetDir)
	seedMirror(t, foo, map[string]string{"a.zip": "a"})
	seedMirror(t, bar, nil)
	appCtx, _ := newTestAppContext(t, targetDir, foo, bar)
	action := &CpFileAction{AppCtx: appCtx}

	_, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"from": "foo", "to": "bar", "path": "a.zip", "targetPath": "../../escaped.zip",
	}})
	if err == nil || !strings.Contains(err.Error(), "outside the local mirror") {
		t.Fatalf("expected a path-escape error, got %v", err)
	}
	assertFileMissing(t, filepath.Join(targetDir, "escaped.zip"))
}
