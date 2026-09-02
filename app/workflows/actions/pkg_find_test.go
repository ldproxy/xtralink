package actions

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/ldproxy/xtralink/lib/workflows"
)

func TestFindAnyAction_NoMatchHalts(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	seedMirror(t, foo, nil)
	appCtx, _ := newTestAppContext(t, targetDir, foo)

	action := &FindAnyAction{AppCtx: appCtx}
	result, err := action.Run(&workflows.StepContext{Params: map[string]any{"pkg": "foo", "path": "*.zip"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(result.Outputs) != 0 {
		t.Errorf("expected 0 output sets, got %+v", result.Outputs)
	}
}

func TestFindAnyAction_ReturnsFirstMatchOnly(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	seedMirror(t, foo, map[string]string{"b.zip": "b", "a.zip": "a"})
	appCtx, _ := newTestAppContext(t, targetDir, foo)

	action := &FindAnyAction{AppCtx: appCtx}
	result, err := action.Run(&workflows.StepContext{Params: map[string]any{"pkg": "foo", "path": "*.zip"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(result.Outputs) != 1 {
		t.Fatalf("expected exactly 1 output set, got %d: %+v", len(result.Outputs), result.Outputs)
	}
	if result.Outputs[0]["path"] != "a.zip" {
		t.Errorf("path = %v, want a.zip (alphabetically first)", result.Outputs[0]["path"])
	}
}

func TestFindEachAction_ReturnsOneOutputPerMatch(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	seedMirror(t, foo, map[string]string{"b.zip": "b", "a.zip": "a", "c.txt": "not a zip"})
	appCtx, _ := newTestAppContext(t, targetDir, foo)

	action := &FindEachAction{AppCtx: appCtx}
	result, err := action.Run(&workflows.StepContext{Params: map[string]any{"pkg": "foo", "path": "*.zip"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(result.Outputs) != 2 {
		t.Fatalf("expected 2 output sets, got %d: %+v", len(result.Outputs), result.Outputs)
	}
	if result.Outputs[0]["path"] != "a.zip" || result.Outputs[1]["path"] != "b.zip" {
		t.Errorf("outputs = %+v, want [a.zip, b.zip] in order", result.Outputs)
	}
}

func TestFindEachAction_NoMatchReturnsEmpty(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	seedMirror(t, foo, nil)
	appCtx, _ := newTestAppContext(t, targetDir, foo)

	action := &FindEachAction{AppCtx: appCtx}
	result, err := action.Run(&workflows.StepContext{Params: map[string]any{"pkg": "foo", "path": "*.zip"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(result.Outputs) != 0 {
		t.Errorf("expected 0 output sets, got %+v", result.Outputs)
	}
}

func TestFindAction_MissingParamsAreErrors(t *testing.T) {
	targetDir := t.TempDir()
	appCtx, _ := newTestAppContext(t, targetDir)
	action := &FindAnyAction{AppCtx: appCtx}

	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{"path": "*.zip"}}); err == nil {
		t.Error("expected an error for a missing pkg param")
	}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{"pkg": "foo"}}); err == nil {
		t.Error("expected an error for a missing path param")
	}
}

// An unpulled package must be an error, not zero matches: find_any would
// otherwise Halt() and end the whole workflow as a success having done
// nothing at all.
func TestFindAction_UnpulledPackageIsError(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	writeFile(t, filepath.Join(foo.URL, "a.zip"), "a") // present on the remote, never pulled
	appCtx, _ := newTestAppContext(t, targetDir, foo)

	for name, action := range map[string]workflows.Action{
		"pkg:find_any":  &FindAnyAction{AppCtx: appCtx},
		"pkg:find_each": &FindEachAction{AppCtx: appCtx},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := action.Run(&workflows.StepContext{Params: map[string]any{"pkg": "foo", "path": "*.zip"}})
			if err == nil {
				t.Fatal("expected an error for a package with no local mirror")
			}
			if !strings.Contains(err.Error(), "pkg:pull") {
				t.Errorf("error %q should point at the missing pkg:pull step", err.Error())
			}
		})
	}
}

func TestFindAction_DoesNotPull(t *testing.T) {
	targetDir := t.TempDir()
	foo := fsPackage(t, "foo", targetDir)
	seedMirror(t, foo, map[string]string{"stale.zip": "stale"})
	writeFile(t, filepath.Join(foo.URL, "fresh.zip"), "fresh") // only on the remote
	appCtx, _ := newTestAppContext(t, targetDir, foo)

	action := &FindEachAction{AppCtx: appCtx}
	result, err := action.Run(&workflows.StepContext{Params: map[string]any{"pkg": "foo", "path": "*.zip"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(result.Outputs) != 1 || result.Outputs[0]["path"] != "stale.zip" {
		t.Errorf("outputs = %+v, want only stale.zip - find must match the mirror as-is, not pull first", result.Outputs)
	}
}
