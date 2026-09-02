package actions

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/workflows"
)

func TestWriteFileAction_WritesContentIntoTheMirror(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	seedMirror(t, foo, nil)
	appCtx, _ := newTestAppContext(t, targetDir, foo)

	action := &WriteFileAction{AppCtx: appCtx}
	result, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"pkg": "foo", "path": "manifest.txt", "content": "one\ntwo\n",
	}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := filepath.Join(foo.ResolvedLocalPath, "manifest.txt")
	if len(result.Outputs) != 1 || result.Outputs[0]["path"] != want {
		t.Errorf("outputs = %+v, want path %q", result.Outputs, want)
	}
	assertFileContent(t, want, "one\ntwo\n")
}

func TestWriteFileAction_CreatesMissingParentDirectories(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	seedMirror(t, foo, nil)
	appCtx, _ := newTestAppContext(t, targetDir, foo)

	action := &WriteFileAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"pkg": "foo", "path": "meta/2026/manifest.txt", "content": "x",
	}}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertFileContent(t, filepath.Join(foo.ResolvedLocalPath, "meta", "2026", "manifest.txt"), "x")
}

func TestWriteFileAction_OverwritesAnExistingFile(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	seedMirror(t, foo, map[string]string{"manifest.txt": "stale"})
	appCtx, _ := newTestAppContext(t, targetDir, foo)

	action := &WriteFileAction{AppCtx: appCtx}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"pkg": "foo", "path": "manifest.txt", "content": "fresh",
	}}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertFileContent(t, filepath.Join(foo.ResolvedLocalPath, "manifest.txt"), "fresh")
}

func TestWriteFileAction_ContentTypes(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	seedMirror(t, foo, nil)
	appCtx, _ := newTestAppContext(t, targetDir, foo)
	action := &WriteFileAction{AppCtx: appCtx}

	accepted := map[string]struct {
		content any
		want    string
	}{
		"text":         {"hello", "hello"},
		"empty string": {"", ""},
		"integer":      {42, "42"},
		"boolean":      {true, "true"},
	}
	for name, c := range accepted {
		t.Run(name, func(t *testing.T) {
			if _, err := action.Run(&workflows.StepContext{Params: map[string]any{
				"pkg": "foo", "path": "out.txt", "content": c.content,
			}}); err != nil {
				t.Fatalf("Run: %v", err)
			}
			assertFileContent(t, filepath.Join(foo.ResolvedLocalPath, "out.txt"), c.want)
		})
	}

	refused := map[string]any{
		"a map":  map[string]any{"a": 1},
		"a list": []any{1, 2},
	}
	for name, content := range refused {
		t.Run(name, func(t *testing.T) {
			_, err := action.Run(&workflows.StepContext{Params: map[string]any{
				"pkg": "foo", "path": "out.txt", "content": content,
			}})
			if err == nil {
				t.Fatal("expected structured content to be refused")
			}
			if !strings.Contains(err.Error(), "content") {
				t.Errorf("error %q should name the content parameter", err.Error())
			}
		})
	}
}

func TestWriteFileAction_MissingParamsAreErrors(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	seedMirror(t, foo, nil)
	appCtx, _ := newTestAppContext(t, targetDir, foo)
	action := &WriteFileAction{AppCtx: appCtx}

	cases := map[string]map[string]any{
		"no pkg":     {"path": "a.txt", "content": "x"},
		"no path":    {"pkg": "foo", "content": "x"},
		"no content": {"pkg": "foo", "path": "a.txt"},
	}
	for name, params := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := action.Run(&workflows.StepContext{Params: params}); err == nil {
				t.Fatal("expected an error for a missing parameter")
			}
		})
	}
}

func TestWriteFileAction_UnpulledPackageIsError(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	appCtx, _ := newTestAppContext(t, targetDir, foo)

	action := &WriteFileAction{AppCtx: appCtx}
	_, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"pkg": "foo", "path": "a.txt", "content": "x",
	}})
	if err == nil {
		t.Fatal("expected an error for a package with no local mirror")
	}
	if !strings.Contains(err.Error(), "pkg:pull") {
		t.Errorf("error %q should point at the missing pkg:pull step", err.Error())
	}
}

func TestWriteFileAction_RejectsUnsupportedPackageType(t *testing.T) {
	targetDir := t.TempDir()
	gitPkg := app.Package{Id: "gitpkg", Type: "GIT", URL: "https://example.com/repo.git", ResolvedLocalPath: filepath.Join(targetDir, "gitpkg")}
	seedMirror(t, gitPkg, nil)
	appCtx, _ := newTestAppContext(t, targetDir, gitPkg)

	action := &WriteFileAction{AppCtx: appCtx}
	_, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"pkg": "gitpkg", "path": "a.txt", "content": "x",
	}})
	if err == nil {
		t.Fatal("expected an error for a GIT package")
	}
	if !strings.Contains(err.Error(), "FS/S3") {
		t.Errorf("error %q should be about the unsupported package type", err.Error())
	}
}

func TestWriteFileAction_PathEscapingTheMirrorIsRejected(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	seedMirror(t, foo, nil)
	appCtx, _ := newTestAppContext(t, targetDir, foo)

	action := &WriteFileAction{AppCtx: appCtx}
	_, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"pkg": "foo", "path": "../escaped.txt", "content": "x",
	}})
	if err == nil {
		t.Fatal("expected an error for a path outside the mirror")
	}
	if !strings.Contains(err.Error(), "outside the local mirror") {
		t.Errorf("error %q should say the path leaves the mirror", err.Error())
	}
	assertFileMissing(t, filepath.Join(targetDir, "escaped.txt"))
}
