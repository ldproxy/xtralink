package workflows

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"

	"github.com/ldproxy/xtrasync/app"
	"github.com/ldproxy/xtrasync/lib/drivers"
	"github.com/ldproxy/xtrasync/lib/jobs"
	"github.com/ldproxy/xtrasync/lib/lock"
	"github.com/ldproxy/xtrasync/lib/workflows"
)

// fakeBackend mirrors the ones in app/jobs and app/workflows/actions test
// files - job:push only needs PushJobSet, nothing else is exercised here.
type fakeBackend struct {
	pushedJobSets []*jobs.JobSet
}

func (f *fakeBackend) IsEnabled() bool { return true }
func (f *fakeBackend) PushJobSet(js *jobs.JobSet) error {
	f.pushedJobSets = append(f.pushedJobSets, js)
	return nil
}
func (f *fakeBackend) PushJob(job *jobs.Job, untake bool) error         { return nil }
func (f *fakeBackend) Take(jobType, executor string) (*jobs.Job, error) { return nil, nil }
func (f *fakeBackend) Done(jobID string) error                          { return nil }
func (f *fakeBackend) Error(jobID, message string, retry bool) error    { return nil }
func (f *fakeBackend) GetSets() ([]*jobs.JobSet, error)                 { return nil, nil }
func (f *fakeBackend) GetSet(id string) (*jobs.JobSet, error)           { return nil, nil }
func (f *fakeBackend) GetOpen(jobType string) ([]*jobs.Job, error)      { return nil, nil }
func (f *fakeBackend) GetTaken() ([]*jobs.Job, error)                   { return nil, nil }
func (f *fakeBackend) GetFailed() ([]*jobs.Job, error)                  { return nil, nil }
func (f *fakeBackend) StartJobSet(jobSetID string) error                { return nil }
func (f *fakeBackend) SetProgressDetails(jobSetID string, details any) error {
	return nil
}
func (f *fakeBackend) SetOutput(jobSetID, key string, value jobs.OutputValue) error {
	return nil
}
func (f *fakeBackend) InitJobSet(jobSetID string, totalDelta int, updates []jobs.ProgressUpdate) error {
	return nil
}
func (f *fakeBackend) UpdateJobSet(jobSetID string, currentDelta int, updates []jobs.ProgressUpdate) error {
	return nil
}
func (f *fakeBackend) UpdateJob(jobID string, currentDelta int) error { return nil }

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func assertMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("expected %s to not exist, stat err = %v", path, err)
	}
}

func assertContent(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	if string(got) != want {
		t.Errorf("content of %s = %q, want %q", path, got, want)
	}
}

// TestRun_CheckLdmExample runs a full example workflow end to end - FS
// packages instead of real S3, a fake Job backend instead of Redis.
// find_each discovers two zips, mv_file moves each into "bar" (deleting it
// from "foo"), job:push pushes one JobSet per file.
func TestRun_CheckLdmExample(t *testing.T) {
	targetDir := t.TempDir()
	fooRemote := t.TempDir()
	barRemote := t.TempDir()

	writeFile(t, filepath.Join(fooRemote, "a.zip"), "a")
	writeFile(t, filepath.Join(fooRemote, "b.zip"), "b")
	writeFile(t, filepath.Join(fooRemote, "c.txt"), "not a zip")

	config := `
targetDir: ` + targetDir + `
packages:
  - id: foo
    type: FS
    url: ` + fooRemote + `
  - id: bar
    type: FS
    url: ` + barRemote + `

workflows:
  - id: check-ldm
    steps:
      - id: input
        action: pkg:find_each
        pkg: foo
        path: "*.zip"
      - action: pkg:mv_file
        from: foo
        to: bar
        path: ${outputs.input.path}
      - action: job:push
        type: nba-apply
        inputs:
          - name: package
            value: ${packages.bar.url}
          - name: file
            value: ${outputs.input.path}
`
	configPath := filepath.Join(t.TempDir(), ".xtrasync.yml")
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatalf("WriteFile config: %v", err)
	}

	settings, err := app.LoadSettings(configPath)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}

	backend := &fakeBackend{}
	appCtx := &app.AppContext{
		Logger:   zerolog.Nop(),
		Settings: settings,
		Drivers:  drivers.NewFactory(),
		Jobs:     backend,
		Locks:    lock.NoopLocker{},
	}

	if err := Run(appCtx, "check-ldm", nil); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertMissing(t, filepath.Join(fooRemote, "a.zip"))
	assertMissing(t, filepath.Join(fooRemote, "b.zip"))
	assertContent(t, filepath.Join(barRemote, "a.zip"), "a")
	assertContent(t, filepath.Join(barRemote, "b.zip"), "b")
	assertContent(t, filepath.Join(fooRemote, "c.txt"), "not a zip") // never matched, untouched

	if len(backend.pushedJobSets) != 2 {
		t.Fatalf("expected 2 pushed job sets (one per zip), got %d", len(backend.pushedJobSets))
	}

	var files []string
	for _, js := range backend.pushedJobSets {
		if js.Type != "nba-apply" {
			t.Errorf("Type = %q, want nba-apply", js.Type)
		}
		var inputs map[string]string
		if err := json.Unmarshal(js.Inputs, &inputs); err != nil {
			t.Fatalf("unmarshal inputs: %v", err)
		}
		if inputs["package"] != barRemote {
			t.Errorf("inputs.package = %q, want %q", inputs["package"], barRemote)
		}
		files = append(files, inputs["file"])
	}
	want := map[string]bool{"a.zip": true, "b.zip": true}
	for _, f := range files {
		if !want[f] {
			t.Errorf("unexpected file in pushed inputs: %s", f)
		}
		delete(want, f)
	}
	if len(want) != 0 {
		t.Errorf("missing files in pushed inputs: %v", want)
	}
}

// TestRun_ParamsOverrideAndDefault runs check-ldm again, but this time the
// package id is a workflow param (${params.pkg}) supplied via --input at
// invocation, and the glob pattern is a param with a default that's left
// unset. Verifies both the override path and the default-fallback path in
// one real Run() call, through the same fan-out/mv_file/job:push pipeline
// as TestRun_CheckLdmExample.
func TestRun_ParamsOverrideAndDefault(t *testing.T) {
	targetDir := t.TempDir()
	fooRemote := t.TempDir()
	barRemote := t.TempDir()

	writeFile(t, filepath.Join(fooRemote, "a.zip"), "a")

	config := `
targetDir: ` + targetDir + `
packages:
  - id: foo
    type: FS
    url: ` + fooRemote + `
  - id: bar
    type: FS
    url: ` + barRemote + `

workflows:
  - id: check-ldm
    params:
      - name: pkg
        type: string
        required: true
      - name: path
        type: string
        default: "*.zip"
    steps:
      - id: input
        action: pkg:find_each
        pkg: ${params.pkg}
        path: ${params.path}
      - action: pkg:mv_file
        from: ${params.pkg}
        to: bar
        path: ${outputs.input.path}
      - action: job:push
        type: nba-apply
        inputs:
          - name: file
            value: ${outputs.input.path}
`
	configPath := filepath.Join(t.TempDir(), ".xtrasync.yml")
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatalf("WriteFile config: %v", err)
	}

	settings, err := app.LoadSettings(configPath)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}

	backend := &fakeBackend{}
	appCtx := &app.AppContext{
		Logger:   zerolog.Nop(),
		Settings: settings,
		Drivers:  drivers.NewFactory(),
		Jobs:     backend,
		Locks:    lock.NoopLocker{},
	}

	// "pkg" is overridden via --input, "path" is left to its default.
	if err := Run(appCtx, "check-ldm", map[string]string{"pkg": "foo"}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertMissing(t, filepath.Join(fooRemote, "a.zip"))
	assertContent(t, filepath.Join(barRemote, "a.zip"), "a")

	if len(backend.pushedJobSets) != 1 {
		t.Fatalf("expected 1 pushed job set, got %d", len(backend.pushedJobSets))
	}
}

// TestRun_MissingRequiredParamAbortsBeforeAnythingRuns verifies a missing
// required param is rejected immediately - before the lock is claimed and
// before any step (including the implicit pull) runs.
func TestRun_MissingRequiredParamAbortsBeforeAnythingRuns(t *testing.T) {
	targetDir := t.TempDir()
	fooRemote := t.TempDir()
	writeFile(t, filepath.Join(fooRemote, "a.zip"), "a")

	config := `
targetDir: ` + targetDir + `
packages:
  - id: foo
    type: FS
    url: ` + fooRemote + `

workflows:
  - id: needs-pkg
    params:
      - name: pkg
        type: string
        required: true
    steps:
      - action: pkg:find_any
        pkg: ${params.pkg}
        path: "*.zip"
`
	configPath := filepath.Join(t.TempDir(), ".xtrasync.yml")
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatalf("WriteFile config: %v", err)
	}

	settings, err := app.LoadSettings(configPath)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}

	appCtx := &app.AppContext{
		Logger:   zerolog.Nop(),
		Settings: settings,
		Drivers:  drivers.NewFactory(),
		Jobs:     &fakeBackend{},
		Locks:    lock.NoopLocker{},
	}

	if err := Run(appCtx, "needs-pkg", nil); err == nil {
		t.Fatal("expected an error for a missing required param")
	}

	// Nothing should have been pulled/touched - the pull would have
	// mirrored fooRemote into targetDir/foo, which must not exist.
	if _, err := os.Stat(filepath.Join(targetDir, "foo")); !os.IsNotExist(err) {
		t.Errorf("expected no pull to have happened, but %s exists", filepath.Join(targetDir, "foo"))
	}
}

func TestParseOverrides_SplitsOnFirstEquals(t *testing.T) {
	got, err := ParseOverrides([]string{"pkg=foo", "path=foo/*.zip", "expr=a=b=c"})
	if err != nil {
		t.Fatalf("ParseOverrides: %v", err)
	}
	want := map[string]string{"pkg": "foo", "path": "foo/*.zip", "expr": "a=b=c"}
	if len(got) != len(want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("got[%q] = %q, want %q", k, got[k], v)
		}
	}
}

func TestParseOverrides_MissingEqualsIsError(t *testing.T) {
	if _, err := ParseOverrides([]string{"no-equals-sign"}); err == nil {
		t.Fatal("expected an error for an entry without '='")
	}
}

func TestParseOverrides_EmptyInputIsEmptyMap(t *testing.T) {
	got, err := ParseOverrides(nil)
	if err != nil {
		t.Fatalf("ParseOverrides: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %+v, want empty map", got)
	}
}

func TestRun_UnknownWorkflowIdIsError(t *testing.T) {
	settings := &app.Settings{TargetDir: t.TempDir()}
	appCtx := &app.AppContext{Logger: zerolog.Nop(), Settings: settings, Drivers: drivers.NewFactory(), Jobs: &fakeBackend{}, Locks: lock.NoopLocker{}}

	if err := Run(appCtx, "does-not-exist", nil); err == nil {
		t.Fatal("expected an error for an unknown workflow id")
	}
}

func TestValidate_RejectsUnknownAction(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{}}
	registry := NewRegistry(appCtx)
	wf := workflows.Workflow{Id: "wf", Steps: []workflows.Step{{Action: "does-not-exist"}}}

	if err := Validate(appCtx, wf, registry); err == nil {
		t.Fatal("expected an error for an unregistered action")
	}
}

func TestValidate_RejectsMvFileWithUnsupportedPackageType(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{Packages: []app.Package{
		{Id: "foo", Type: "FS"},
		{Id: "gitpkg", Type: "GIT"},
	}}}
	registry := NewRegistry(appCtx)
	wf := workflows.Workflow{Id: "wf", Steps: []workflows.Step{{
		Action: "pkg:mv_file",
		Params: map[string]any{"from": "foo", "to": "gitpkg", "path": "a.zip"},
	}}}

	if err := Validate(appCtx, wf, registry); err == nil {
		t.Fatal("expected an error for a GIT target package")
	}
}

func TestValidate_RejectsUnknownPackageReference(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{}}
	registry := NewRegistry(appCtx)
	wf := workflows.Workflow{Id: "wf", Steps: []workflows.Step{{
		Action: "pkg:find_any",
		Params: map[string]any{"pkg": "does-not-exist", "path": "*.zip"},
	}}}

	if err := Validate(appCtx, wf, registry); err == nil {
		t.Fatal("expected an error for a reference to an unknown package")
	}
}

func TestValidate_SkipsTemplatedPackageRefs(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{}}
	registry := NewRegistry(appCtx)
	wf := workflows.Workflow{Id: "wf", Steps: []workflows.Step{{
		Action: "pkg:find_any",
		Params: map[string]any{"pkg": "${outputs.x.y}", "path": "*.zip"},
	}}}

	if err := Validate(appCtx, wf, registry); err != nil {
		t.Errorf("expected a templated pkg ref to be skipped (resolved only at runtime), got: %v", err)
	}
}
