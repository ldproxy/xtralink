package actions

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/workflows"
)

func TestMvFileAction_MovesFileBetweenLocalMirrors(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	bar := fsPackage(t, "bar", targetDir)
	seedMirror(t, foo, map[string]string{"a.zip": "a"})
	seedMirror(t, bar, nil)
	appCtx, _ := newTestAppContext(t, targetDir, foo, bar)

	action := &MvFileAction{AppCtx: appCtx}
	result, err := action.Run(&workflows.StepContext{Params: map[string]any{"from": "foo", "to": "bar", "path": "a.zip"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(result.Outputs) != 1 {
		t.Fatalf("expected exactly 1 output set, got %+v", result.Outputs)
	}

	assertFileMissing(t, filepath.Join(foo.ResolvedLocalPath, "a.zip"))
	assertFileContent(t, filepath.Join(bar.ResolvedLocalPath, "a.zip"), "a")
}

func TestMvFileAction_MovesNestedPath(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	bar := fsPackage(t, "bar", targetDir)
	seedMirror(t, foo, map[string]string{"sub/a.zip": "a"})
	seedMirror(t, bar, nil)
	appCtx, _ := newTestAppContext(t, targetDir, foo, bar)

	action := &MvFileAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{"from": "foo", "to": "bar", "path": "sub/a.zip"}}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertFileMissing(t, filepath.Join(foo.ResolvedLocalPath, "sub", "a.zip"))
	assertFileContent(t, filepath.Join(bar.ResolvedLocalPath, "sub", "a.zip"), "a")
}

// The move must stay local: a workflow decides when its remotes change by
// where it puts its pkg:push steps.
func TestMvFileAction_LeavesBothRemotesUntouched(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	bar := fsPackage(t, "bar", targetDir)
	seedMirror(t, foo, map[string]string{"a.zip": "a"})
	seedMirror(t, bar, nil)
	writeFile(t, filepath.Join(foo.URL, "a.zip"), "a")
	appCtx, _ := newTestAppContext(t, targetDir, foo, bar)

	action := &MvFileAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{"from": "foo", "to": "bar", "path": "a.zip"}}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertFileContent(t, filepath.Join(foo.URL, "a.zip"), "a") // still on the source remote
	assertFileMissing(t, filepath.Join(bar.URL, "a.zip"))      // not yet on the target remote
}

// The pairing a real workflow uses: move locally, then push both packages.
func TestMvFileAction_ReachesTheRemotesOncePushed(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	bar := fsPackage(t, "bar", targetDir)
	seedMirror(t, foo, map[string]string{"a.zip": "a"})
	seedMirror(t, bar, nil)
	writeFile(t, filepath.Join(foo.URL, "a.zip"), "a")
	appCtx, _ := newTestAppContext(t, targetDir, foo, bar)

	mv := &MvFileAction{AppCtx: appCtx}
	if _, err := mv.Run(&workflows.StepContext{Params: map[string]any{"from": "foo", "to": "bar", "path": "a.zip"}}); err != nil {
		t.Fatalf("mv.Run: %v", err)
	}

	push := &PushAction{AppCtx: appCtx}
	for _, pkgId := range []string{"foo", "bar"} {
		if _, err := push.Run(&workflows.StepContext{Params: map[string]any{"pkg": pkgId}}); err != nil {
			t.Fatalf("push.Run(%s): %v", pkgId, err)
		}
	}

	assertFileMissing(t, filepath.Join(foo.URL, "a.zip"))
	assertFileContent(t, filepath.Join(bar.URL, "a.zip"), "a")
}

func TestMvFileAction_IsIdempotentAfterMove(t *testing.T) {
	// A second find on "foo" for the same glob must no longer see the moved
	// file - the whole point of mv_file actually deleting the source.
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	bar := fsPackage(t, "bar", targetDir)
	seedMirror(t, foo, map[string]string{"a.zip": "a"})
	seedMirror(t, bar, nil)
	appCtx, _ := newTestAppContext(t, targetDir, foo, bar)

	mv := &MvFileAction{AppCtx: appCtx}
	if _, err := mv.Run(&workflows.StepContext{Params: map[string]any{"from": "foo", "to": "bar", "path": "a.zip"}}); err != nil {
		t.Fatalf("mv.Run: %v", err)
	}

	find := &FindAnyAction{AppCtx: appCtx}
	result, err := find.Run(&workflows.StepContext{Params: map[string]any{"pkg": "foo", "path": "*.zip"}})
	if err != nil {
		t.Fatalf("find.Run: %v", err)
	}
	if len(result.Outputs) != 0 {
		t.Errorf("expected the moved file to no longer be found in foo, got %+v", result.Outputs)
	}
}

func TestMvFileAction_UnpulledPackageIsError(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	bar := fsPackage(t, "bar", targetDir)
	appCtx, _ := newTestAppContext(t, targetDir, foo, bar)
	action := &MvFileAction{AppCtx: appCtx}
	params := map[string]any{"from": "foo", "to": "bar", "path": "a.zip"}

	seedMirror(t, bar, nil) // only the target was pulled
	_, err := action.Run(&workflows.StepContext{Params: params})
	if err == nil {
		t.Fatal("expected an error for an unpulled from package")
	}
	if !strings.Contains(err.Error(), "from") || !strings.Contains(err.Error(), "pkg:pull") {
		t.Errorf("error %q should name the unpulled from package", err.Error())
	}

	seedMirror(t, foo, map[string]string{"a.zip": "a"})
	if _, err := action.Run(&workflows.StepContext{Params: params}); err != nil {
		t.Fatalf("both mirrors present, Run should succeed: %v", err)
	}
}

func TestMvFileAction_UnpulledTargetIsError(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	bar := fsPackage(t, "bar", targetDir)
	seedMirror(t, foo, map[string]string{"a.zip": "a"}) // only the source was pulled
	appCtx, _ := newTestAppContext(t, targetDir, foo, bar)

	action := &MvFileAction{AppCtx: appCtx}
	_, err := action.Run(&workflows.StepContext{Params: map[string]any{"from": "foo", "to": "bar", "path": "a.zip"}})
	if err == nil {
		t.Fatal("expected an error for an unpulled target package")
	}
	if !strings.Contains(err.Error(), "to") || !strings.Contains(err.Error(), "pkg:pull") {
		t.Errorf("error %q should name the unpulled to package", err.Error())
	}
}

func TestMvFileAction_RejectsUnsupportedPackageTypes(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	gitPkg := app.Package{Id: "gitpkg", Type: "GIT", URL: "https://example.com/repo.git", ResolvedLocalPath: filepath.Join(targetDir, "gitpkg")}
	seedMirror(t, foo, map[string]string{"a.zip": "a"})
	seedMirror(t, gitPkg, nil)
	appCtx, _ := newTestAppContext(t, targetDir, foo, gitPkg)

	action := &MvFileAction{AppCtx: appCtx}
	_, err := action.Run(&workflows.StepContext{Params: map[string]any{"from": "foo", "to": "gitpkg", "path": "a.zip"}})
	if err == nil {
		t.Fatal("expected an error for a GIT target package")
	}
	if !strings.Contains(err.Error(), "FS/S3") {
		t.Errorf("error %q should be about the unsupported package type", err.Error())
	}
}

func TestSupportsSyncBack(t *testing.T) {
	cases := map[string]bool{"FS": true, "S3": true, "fs": true, "GIT": false, "OCI": false, "": false}
	for typ, want := range cases {
		if got := SupportsSyncBack(typ); got != want {
			t.Errorf("SupportsSyncBack(%q) = %v, want %v", typ, got, want)
		}
	}
}
