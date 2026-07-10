package actions

import (
	"path/filepath"
	"testing"

	"github.com/ldproxy/xtrasync/app"
	"github.com/ldproxy/xtrasync/lib/workflows"
)

func TestMvFileAction_MovesFileBetweenFSPackages(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	bar := fsPackage(t, "bar", targetDir)
	writeFile(t, filepath.Join(foo.URL, "a.zip"), "a")
	appCtx, _ := newTestAppContext(t, targetDir, foo, bar)

	action := &MvFileAction{AppCtx: appCtx}
	result, err := action.Run(&workflows.StepContext{Params: map[string]any{"from": "foo", "to": "bar", "path": "a.zip"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(result.Outputs) != 1 {
		t.Fatalf("expected exactly 1 output set, got %+v", result.Outputs)
	}

	assertFileMissing(t, filepath.Join(foo.URL, "a.zip"))
	assertFileContent(t, filepath.Join(bar.URL, "a.zip"), "a")
}

func TestMvFileAction_MovesNestedPath(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	bar := fsPackage(t, "bar", targetDir)
	writeFile(t, filepath.Join(foo.URL, "sub", "a.zip"), "a")
	appCtx, _ := newTestAppContext(t, targetDir, foo, bar)

	action := &MvFileAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{"from": "foo", "to": "bar", "path": "sub/a.zip"}}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertFileMissing(t, filepath.Join(foo.URL, "sub", "a.zip"))
	assertFileContent(t, filepath.Join(bar.URL, "sub", "a.zip"), "a")
}

func TestMvFileAction_IsIdempotentAfterMove(t *testing.T) {
	// A second find on "foo" for the same glob must no longer see the moved
	// file - the whole point of mv_file actually deleting the source.
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	bar := fsPackage(t, "bar", targetDir)
	writeFile(t, filepath.Join(foo.URL, "a.zip"), "a")
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

func TestMvFileAction_RejectsUnsupportedPackageTypes(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	gitPkg := app.Package{Id: "gitpkg", Type: "GIT", URL: "https://example.com/repo.git", ResolvedLocalPath: filepath.Join(targetDir, "gitpkg")}
	appCtx, _ := newTestAppContext(t, targetDir, foo, gitPkg)

	action := &MvFileAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{"from": "foo", "to": "gitpkg", "path": "a.zip"}}); err == nil {
		t.Fatal("expected an error for a GIT target package")
	}
}

func TestSupportsMvFile(t *testing.T) {
	cases := map[string]bool{"FS": true, "S3": true, "fs": true, "GIT": false, "OCI": false, "": false}
	for typ, want := range cases {
		if got := SupportsMvFile(typ); got != want {
			t.Errorf("SupportsMvFile(%q) = %v, want %v", typ, got, want)
		}
	}
}
