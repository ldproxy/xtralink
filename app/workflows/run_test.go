package workflows

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/drivers"
	"github.com/ldproxy/xtralink/lib/jobs"
	"github.com/ldproxy/xtralink/lib/lock"
	"github.com/ldproxy/xtralink/lib/workflows"
	"github.com/ldproxy/xtralink/model"
)

// fakeBackend mirrors the ones in app/jobs and app/workflows/actions test
// files - job:push only needs PushJob, nothing else is exercised here.
type fakeBackend struct {
	pushedJobs []*model.Job
}

func (f *fakeBackend) IsEnabled() bool { return true }
func (f *fakeBackend) PushJob(job *model.Job) error {
	return f.PushJobListen(job, jobs.NoopJobListener{})
}
func (f *fakeBackend) PushJobListen(job *model.Job, onProgress jobs.JobListener) error {
	f.pushedJobs = append(f.pushedJobs, job)
	return nil
}
func (f *fakeBackend) PushPartialJob(partialJob *model.PartialJob, untake bool) error { return nil }
func (f *fakeBackend) Take(partialJobType, executor string) (*model.PartialJob, error) {
	return nil, nil
}
func (f *fakeBackend) Done(partialJobID string) error                       { return nil }
func (f *fakeBackend) Error(partialJobID, message string, retry bool) error { return nil }
func (f *fakeBackend) DoneJob(jobID string) error                           { return nil }
func (f *fakeBackend) Cancel(jobID string) (bool, error)                    { return false, nil }
func (f *fakeBackend) GetJobs() ([]*model.Job, error)                       { return nil, nil }
func (f *fakeBackend) GetJob(id string) (*model.Job, error)                 { return nil, nil }
func (f *fakeBackend) GetPartialJob(id string) (*model.PartialJob, error)   { return nil, nil }
func (f *fakeBackend) GetOpen(partialJobType string) ([]*model.PartialJob, error) {
	return nil, nil
}
func (f *fakeBackend) GetTaken() ([]*model.PartialJob, error)  { return nil, nil }
func (f *fakeBackend) GetFailed() ([]*model.PartialJob, error) { return nil, nil }
func (f *fakeBackend) StartJob(jobID string) error             { return nil }
func (f *fakeBackend) SetProgressDetails(jobID string, details map[string]any) error {
	return nil
}
func (f *fakeBackend) SetOutput(jobID, key string, value model.OutputValue) error {
	return nil
}
func (f *fakeBackend) SetOutputs(jobID string, outputs map[string]any) error {
	return nil
}
func (f *fakeBackend) InitJob(jobID string, totalDelta int, updates []model.ProgressUpdate) error {
	return nil
}
func (f *fakeBackend) UpdateJob(jobID string, currentDelta int, updates []model.ProgressUpdate) error {
	return nil
}
func (f *fakeBackend) UpdatePartialJob(partialJobID string, currentDelta int) error { return nil }

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
settings:
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
      - action: pkg:pull
        pkg: foo
      - action: pkg:pull
        pkg: bar
      - id: input
        action: pkg:find_each
        pkg: foo
        path: "*.zip"
      - action: pkg:mv_file
        from: foo
        to: bar
        path: ${outputs.input.path}
      - action: pkg:push
        pkg: foo
      - action: pkg:push
        pkg: bar
      - action: job:push
        kind: nba-apply
        inputs:
          package: ${packages.bar.url}
          file: ${outputs.input.path}
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

	if len(backend.pushedJobs) != 2 {
		t.Fatalf("expected 2 pushed job sets (one per zip), got %d", len(backend.pushedJobs))
	}

	var files []string
	for _, js := range backend.pushedJobs {
		if js.Kind != "nba-apply" {
			t.Errorf("Kind = %q, want nba-apply", js.Kind)
		}
		inputs := js.Inputs
		if inputs["package"] != barRemote {
			t.Errorf("inputs.package = %q, want %q", inputs["package"], barRemote)
		}
		file, _ := inputs["file"].(string)
		files = append(files, file)
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
// package id is a workflow param (${parameters.pkg}) supplied via --input at
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
settings:
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
    parameters:
      - name: pkg
        type: string
        required: true
      - name: path
        type: string
        default: "*.zip"
    steps:
      - action: pkg:pull
        pkg: ${parameters.pkg}
      - action: pkg:pull
        pkg: bar
      - id: input
        action: pkg:find_each
        pkg: ${parameters.pkg}
        path: ${parameters.path}
      - action: pkg:mv_file
        from: ${parameters.pkg}
        to: bar
        path: ${outputs.input.path}
      - action: pkg:push
        pkg: ${parameters.pkg}
      - action: pkg:push
        pkg: bar
      - action: job:push
        kind: nba-apply
        inputs:
          file: ${outputs.input.path}
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

	if len(backend.pushedJobs) != 1 {
		t.Fatalf("expected 1 pushed job set, got %d", len(backend.pushedJobs))
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
settings:
  targetDir: ` + targetDir + `
packages:
  - id: foo
    type: FS
    url: ` + fooRemote + `

workflows:
  - id: needs-pkg
    parameters:
      - name: pkg
        type: string
        required: true
    steps:
      - action: pkg:find_any
        pkg: ${parameters.pkg}
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
	settings := &app.Settings{General: app.GeneralConfig{TargetDir: t.TempDir()}}
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

func TestValidate_CpFileOnlyRequiresSyncBackForTheTarget(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{Packages: []app.Package{
		{Id: "foo", Type: "FS"},
		{Id: "gitpkg", Type: "GIT"},
	}}}
	registry := NewRegistry(appCtx)
	validate := func(from, to string) error {
		return Validate(appCtx, workflows.Workflow{Id: "wf", Steps: []workflows.Step{{
			Action: "pkg:cp_file",
			Params: map[string]any{"from": from, "to": to, "path": "a.zip"},
		}}}, registry)
	}

	if err := validate("gitpkg", "foo"); err != nil {
		t.Errorf("expected a GIT source to be accepted, got: %v", err)
	}
	if err := validate("foo", "gitpkg"); err == nil {
		t.Error("expected an error for a GIT target package")
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

func TestValidate_AcceptsJobPushPartialsReferencingExistingSteps(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{JobDefinitions: []app.JobDefinition{
		{Kind: "nba-transformation", Workflow: "nba-transform"},
	}}}
	registry := NewRegistry(appCtx)
	wf := workflows.Workflow{Id: "wf", Steps: []workflows.Step{{
		Action: "job:push",
		Params: map[string]any{
			"kind":     "nba-apply",
			"partials": []any{map[string]any{"kind": "nba-transformation"}},
		},
	}}}

	if err := Validate(appCtx, wf, registry); err != nil {
		t.Errorf("expected partials referencing an existing kind to be valid, got: %v", err)
	}
}

// A partial kind with no job definition here is legitimate: another
// service may process it (s. actions.resolvePartials). Only a missing kind
// is a mistake validation can name.
func TestValidate_AcceptsJobPushPartialsWithNoJobDefinition(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{}}
	registry := NewRegistry(appCtx)
	stepWith := func(partials any) workflows.Workflow {
		return workflows.Workflow{Id: "wf", Steps: []workflows.Step{{
			Action: "job:push",
			Params: map[string]any{"kind": "nba-apply", "partials": partials},
		}}}
	}

	external := stepWith([]any{map[string]any{"kind": "processed-elsewhere"}})
	if err := Validate(appCtx, external, registry); err != nil {
		t.Errorf("expected a kind with no definition to be valid, got: %v", err)
	}

	nameless := stepWith([]any{map[string]any{"workflow": "oops"}})
	if err := Validate(appCtx, nameless, registry); err == nil {
		t.Fatal("expected an error for a partials entry with no kind")
	}
}

func TestValidate_AcceptsTemplatedJobPushPartialKind(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{}}
	registry := NewRegistry(appCtx)
	wf := workflows.Workflow{Id: "wf", Steps: []workflows.Step{{
		Action: "job:push",
		Params: map[string]any{
			"kind":     "nba-apply",
			"partials": []any{map[string]any{"kind": "${outputs.x.y}"}},
		},
	}}}

	if err := Validate(appCtx, wf, registry); err != nil {
		t.Errorf("expected a templated partials type to be skipped (resolved only at runtime), got: %v", err)
	}
}

func TestValidate_JobPushWithoutPartialsIsFine(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{}}
	registry := NewRegistry(appCtx)
	wf := workflows.Workflow{Id: "wf", Steps: []workflows.Step{{
		Action: "job:push",
		Params: map[string]any{"kind": "nba-apply"},
	}}}

	if err := Validate(appCtx, wf, registry); err != nil {
		t.Errorf("expected job:push without partials to remain valid, got: %v", err)
	}
}

func TestValidate_RejectsPushWithUnsupportedPackageType(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{Packages: []app.Package{
		{Id: "gitpkg", Type: "GIT"},
	}}}
	registry := NewRegistry(appCtx)
	wf := workflows.Workflow{Id: "wf", Steps: []workflows.Step{{
		Action: "pkg:push",
		Params: map[string]any{"pkg": "gitpkg"},
	}}}

	if err := Validate(appCtx, wf, registry); err == nil {
		t.Fatal("expected an error for a GIT package used with pkg:push")
	}
}

func TestValidate_AcceptsPullOfAnyPackageType(t *testing.T) {
	appCtx := &app.AppContext{Settings: &app.Settings{Packages: []app.Package{
		{Id: "gitpkg", Type: "GIT"},
	}}}
	registry := NewRegistry(appCtx)
	wf := workflows.Workflow{Id: "wf", Steps: []workflows.Step{{
		Action: "pkg:pull",
		Params: map[string]any{"pkg": "gitpkg"},
	}}}

	if err := Validate(appCtx, wf, registry); err != nil {
		t.Errorf("pkg:pull should accept any package type, got: %v", err)
	}
}

// TestRun_NbaTransformExample runs pkg:pull -> pkg:find_any -> cmd:exec ->
// pkg:push end to end: pull exposes the package's local path as an output
// (packageVars alone doesn't - that's the whole reason pkg:pull exists),
// find_any locates the zip, cmd:exec copies it to a second file entirely
// via a real OS command (no shell), and pkg:push mirrors the local mirror
// (now containing both files) back to the FS "remote".
func TestRun_NbaTransformExample(t *testing.T) {
	targetDir := t.TempDir()
	fooRemote := t.TempDir()
	writeFile(t, filepath.Join(fooRemote, "a.zip"), "a")

	config := `
settings:
  targetDir: ` + targetDir + `
packages:
  - id: foo
    type: FS
    url: ` + fooRemote + `

workflows:
  - id: nba-transform
    steps:
      - id: pulled
        action: pkg:pull
        pkg: foo
      - id: found
        action: pkg:find_any
        pkg: foo
        path: "*.zip"
      - action: cmd:exec
        cmd: cp ${outputs.pulled.path}/${outputs.found.path} ${outputs.pulled.path}/COPY_${outputs.found.path}
      - action: pkg:push
        pkg: foo
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

	if err := Run(appCtx, "nba-transform", nil); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertContent(t, filepath.Join(fooRemote, "a.zip"), "a")
	assertContent(t, filepath.Join(fooRemote, "COPY_a.zip"), "a")
}

// A job:push parameter of the wrong kind must fail validation, before any
// earlier step of the workflow has run.
func TestValidate_JobPushRejectsWrongParameterShapes(t *testing.T) {
	cases := map[string]string{
		"inputs as a name/value list": `
      - action: job:push
        kind: demo
        inputs:
          - name: file
            value: a.zip`,
		"context as a list": `
      - action: job:push
        kind: demo
        context:
          - trace`,
		"ttlSeconds as a string": `
      - action: job:push
        kind: demo
        ttlSeconds: soon`,
		"sequential as a string": `
      - action: job:push
        kind: demo
        sequential: maybe`,
		"setup as a definition id": `
      - action: job:push
        kind: demo
        setup: no-such-definition`,
	}

	for name, steps := range cases {
		t.Run(name, func(t *testing.T) {
			appCtx := jobPushValidationAppCtx(t, steps)
			if err := Run(appCtx, "dispatch", nil); err == nil {
				t.Fatal("expected the workflow to be rejected")
			}
		})
	}
}

func TestValidate_JobPushAcceptsTemplatedParameters(t *testing.T) {
	// A whole-value template's type is only known once earlier steps have
	// run, so validation must let it through rather than guess.
	appCtx := jobPushValidationAppCtx(t, `
      - action: job:push
        kind: demo
        inputs: ${parameters.blob}
        ttlSeconds: ${parameters.ttl}`)

	if err := Validate(appCtx, appCtx.Settings.Workflows[0], NewRegistry(appCtx)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func jobPushValidationAppCtx(t *testing.T, steps string) *app.AppContext {
	t.Helper()

	config := `
settings:
  targetDir: ` + t.TempDir() + `
packages:
  - id: foo
    type: FS
    url: ` + t.TempDir() + `

workflows:
  - id: dispatch
    steps:` + steps + `
`
	configPath := filepath.Join(t.TempDir(), ".xtrasync.yml")
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatalf("WriteFile config: %v", err)
	}
	settings, err := app.LoadSettings(configPath)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}

	return &app.AppContext{
		Logger:   zerolog.Nop(),
		Settings: settings,
		Drivers:  drivers.NewFactory(),
		Jobs:     &fakeBackend{},
		Locks:    lock.NoopLocker{},
	}
}

// uuid:gen has no parameters to validate and no references to check - what
// matters is that it is registered, so a workflow using it runs and its
// output reaches a later step.
func TestRun_UUIDGenOutputReachesALaterStep(t *testing.T) {
	targetDir := t.TempDir()
	config := `
settings:
  targetDir: ` + targetDir + `
packages:
  - id: foo
    type: FS
    url: ` + t.TempDir() + `

workflows:
  - id: names-a-file
    steps:
      - id: name
        action: uuid:gen
      - action: cmd:exec
        cmd: touch ` + targetDir + `/${outputs.name.uuid}
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

	if err := Run(appCtx, "names-a-file", nil); err != nil {
		t.Fatalf("Run: %v", err)
	}

	entries, err := os.ReadDir(targetDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly one created file, got %+v", entries)
	}
	if _, err := uuid.Parse(entries[0].Name()); err != nil {
		t.Errorf("created file %q is not named after a uuid: %v", entries[0].Name(), err)
	}
}

// pkg:write_file is validated like pkg:push: the package must exist and
// support sync-back, since a file written into a mirror that can never be
// pushed is a file nobody sees.
func TestValidate_WriteFileRequiresASyncBackPackage(t *testing.T) {
	config := `
settings:
  targetDir: ` + t.TempDir() + `
packages:
  - id: foo
    type: FS
    url: ` + t.TempDir() + `
  - id: gitpkg
    type: GIT
    url: https://example.com/repo.git

workflows:
  - id: writes-to-git
    steps:
      - action: pkg:write_file
        pkg: gitpkg
        path: a.txt
        content: x
  - id: writes-to-unknown
    steps:
      - action: pkg:write_file
        pkg: no-such-package
        path: a.txt
        content: x
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

	for _, id := range []string{"writes-to-git", "writes-to-unknown"} {
		t.Run(id, func(t *testing.T) {
			if err := Run(appCtx, id, nil); err == nil {
				t.Fatal("expected the workflow to be rejected before it ran")
			}
		})
	}
}

// Handler steps are validated the same way as ordinary ones, and by name,
// so a mistake in one is caught before the run rather than only when
// something fails and the handler is finally reached.
func TestValidate_HandlerStepsAreCheckedByName(t *testing.T) {
	cases := map[string]string{
		"unknown action in failure": `
    handlers:
      failure:
        - action: no:such_action`,
		"unknown package in always": `
    handlers:
      always:
        - action: pkg:pull
          pkg: no-such-package`,
		"git package in success": `
    handlers:
      success:
        - action: pkg:write_file
          pkg: gitpkg
          path: a.txt
          content: x`,
	}

	for name, handlers := range cases {
		t.Run(name, func(t *testing.T) {
			config := `
settings:
  targetDir: ` + t.TempDir() + `
packages:
  - id: foo
    type: FS
    url: ` + t.TempDir() + `
  - id: gitpkg
    type: GIT
    url: https://example.com/repo.git

workflows:
  - id: with-handlers
    steps:
      - action: pkg:pull
        pkg: foo` + handlers + `
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

			err = Run(appCtx, "with-handlers", nil)
			if err == nil {
				t.Fatal("expected the workflow to be rejected before it ran")
			}
			if !strings.Contains(err.Error(), "handlers.") {
				t.Errorf("error %q should name the handler the problem is in", err.Error())
			}
		})
	}
}

// A failure handler runs against the same packages the workflow does, so it
// can record what went wrong where the next run will find it.
func TestRun_FailureHandlerWritesWhatWentWrong(t *testing.T) {
	targetDir := t.TempDir()
	remote := t.TempDir()
	config := `
settings:
  targetDir: ` + targetDir + `
packages:
  - id: foo
    type: FS
    url: ` + remote + `

workflows:
  - id: doomed
    steps:
      - action: pkg:pull
        pkg: foo
      - id: breaks
        action: cmd:exec
        cmd: false
    handlers:
      failure:
        - action: pkg:write_file
          pkg: foo
          path: failed/${error.step}.txt
          content: ${error.message}
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

	runErr := Run(appCtx, "doomed", nil)
	if runErr == nil {
		t.Fatal("expected the run to fail")
	}

	written := filepath.Join(targetDir, "foo", "failed", "breaks.txt")
	content, err := os.ReadFile(written)
	if err != nil {
		t.Fatalf("expected the failure handler to have written %s: %v", written, err)
	}
	if !strings.Contains(string(content), "cmd:exec") {
		t.Errorf("handler wrote %q, want the failure it was told about", content)
	}
}
