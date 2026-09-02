package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, ".xtrasync.yml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

const minimalPackage = `
packages:
  - id: foo
    type: GIT
    url: https://example.com/foo.git
`

func TestLoadSettings_ParsesWorkflows(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
workflows:
  - id: check-ldm
    defaults:
      retry_policy:
        limit: 2
        interval_sec: 5
    steps:
      - id: input
        action: pkg:find_each
        pkg: foo
        path: "*.zip"
      - action: pkg:mv_file
        from: foo
        to: foo
        path: ${outputs.input.path}
`)

	settings, err := LoadSettings(path)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}

	if len(settings.Workflows) != 1 {
		t.Fatalf("len(Workflows) = %d, want 1", len(settings.Workflows))
	}
	wf := settings.Workflows[0]
	if wf.Id != "check-ldm" {
		t.Errorf("Id = %q", wf.Id)
	}
	if len(wf.Steps) != 2 {
		t.Fatalf("len(Steps) = %d, want 2", len(wf.Steps))
	}
	if wf.Steps[0].Params["pkg"] != "foo" {
		t.Errorf("step 0 Params = %+v", wf.Steps[0].Params)
	}
}

func TestLoadSettings_WorkflowsOptional(t *testing.T) {
	path := writeConfig(t, minimalPackage)

	settings, err := LoadSettings(path)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}
	if len(settings.Workflows) != 0 {
		t.Errorf("expected no workflows, got %+v", settings.Workflows)
	}
}

func TestLoadSettings_RejectsDuplicateWorkflowId(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
workflows:
  - id: dup
    steps:
      - action: job:push
  - id: dup
    steps:
      - action: job:push
`)

	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for duplicate workflow ids")
	}
}

func TestLoadSettings_RejectsDuplicateParamName(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
workflows:
  - id: wf
    params:
      - name: pkg
      - name: pkg
    steps:
      - action: job:push
`)

	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for duplicate param names")
	}
}

func TestLoadSettings_RejectsMissingParamName(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
workflows:
  - id: wf
    params:
      - type: string
    steps:
      - action: job:push
`)

	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for a param without a name")
	}
}

func TestLoadSettings_RejectsDuplicateStepId(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
workflows:
  - id: wf
    steps:
      - id: same
        action: job:push
      - id: same
        action: job:push
`)

	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for duplicate step ids")
	}
}

func TestLoadSettings_ImplicitStepIdCanCollideWithExplicit(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
workflows:
  - id: wf
    steps:
      - action: job:push
      - id: "0"
        action: job:push
`)

	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error: step 0's implicit id \"0\" collides with step 1's explicit id \"0\"")
	}
}

func TestLoadSettings_RejectsMissingWorkflowId(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
workflows:
  - steps:
      - action: job:push
`)

	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for a workflow without an id")
	}
}

func TestLoadSettings_RejectsMissingStepAction(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
workflows:
  - id: wf
    steps:
      - id: input
`)

	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for a step without an action")
	}
}

func TestSettings_HasWorkflowAndGetWorkflow(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
workflows:
  - id: check-ldm
    steps:
      - action: job:push
`)
	settings, err := LoadSettings(path)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}

	if !settings.HasWorkflow("check-ldm") {
		t.Error("expected HasWorkflow(check-ldm) to be true")
	}
	if settings.HasWorkflow("missing") {
		t.Error("expected HasWorkflow(missing) to be false")
	}

	wf, err := settings.GetWorkflow("check-ldm")
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if wf.Id != "check-ldm" {
		t.Errorf("GetWorkflow returned %+v", wf)
	}

	if _, err := settings.GetWorkflow("missing"); err == nil {
		t.Fatal("expected an error for an unknown workflow id")
	}
}

func TestLoadSettings_JobsDefaultsToLocalWithMaxConcurrentOne(t *testing.T) {
	path := writeConfig(t, minimalPackage)

	settings, err := LoadSettings(path)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}
	if settings.JobQueue.Queue != "local" {
		t.Errorf("JobQueue.Queue = %q, want local", settings.JobQueue.Queue)
	}
	if settings.JobQueue.MaxConcurrent != 1 {
		t.Errorf("JobQueue.MaxConcurrent = %d, want 1", settings.JobQueue.MaxConcurrent)
	}
}

func TestLoadSettings_ParsesRedisQueueConfig(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
settings:
  queue: redis
  maxConcurrent: 4
  redis:
    - localhost:6379
    - localhost:6380
`)

	settings, err := LoadSettings(path)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}
	if settings.JobQueue.Queue != "redis" {
		t.Errorf("JobQueue.Queue = %q, want redis", settings.JobQueue.Queue)
	}
	if settings.JobQueue.MaxConcurrent != 4 {
		t.Errorf("JobQueue.MaxConcurrent = %d, want 4", settings.JobQueue.MaxConcurrent)
	}
	if len(settings.JobQueue.Redis) != 2 || settings.JobQueue.Redis[0] != "localhost:6379" || settings.JobQueue.Redis[1] != "localhost:6380" {
		t.Errorf("JobQueue.Redis = %v", settings.JobQueue.Redis)
	}
}

func TestLoadSettings_RejectsInvalidQueueValue(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
settings:
  queue: memcached
`)
	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for an invalid settings.queue value")
	}
}

func TestLoadSettings_RejectsRedisQueueWithoutNodes(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
settings:
  queue: redis
`)
	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for settings.queue=redis without settings.redis")
	}
}

func TestLoadSettings_RejectsNegativeMaxConcurrent(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
settings:
  maxConcurrent: -1
`)
	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for a negative settings.maxConcurrent")
	}
}

func TestLoadSettings_RejectsEmptyRedisNode(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
settings:
  queue: redis
  redis:
    - ""
`)
	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for an empty settings.redis entry")
	}
}

func TestLoadSettings_UnknownKeyIsError(t *testing.T) {
	cases := map[string]string{
		"top level": `
targetDir: /tmp/t
jobDefinitions:
  - id: transform-step
    workflow: transform
packages:
  - id: foo
    type: FS
    url: /tmp/r
`,
		"inside a package": `
targetDir: /tmp/t
packages:
  - id: foo
    type: FS
    url: /tmp/r
    localpath: typo
`,
		"inside a workflow": `
targetDir: /tmp/t
packages:
  - id: foo
    type: FS
    url: /tmp/r
workflows:
  - id: wf
    describe: a misspelled description
    steps: []
`,
	}

	for name, config := range cases {
		t.Run(name, func(t *testing.T) {
			path := writeConfig(t, config)
			_, err := LoadSettings(path)
			if err == nil {
				t.Fatal("expected an unrecognized key to be rejected")
			}
			if !strings.Contains(err.Error(), "could not parse yaml") {
				t.Errorf("error %q should report a parse failure", err.Error())
			}
		})
	}
}

// A Step's action parameters are an inline map, so strict decoding must
// still let arbitrary keys through there - that is the whole mechanism by
// which an Action interprets its own parameters.
func TestLoadSettings_StepActionParametersStayFree(t *testing.T) {
	path := writeConfig(t, `
targetDir: /tmp/t
packages:
  - id: foo
    type: FS
    url: /tmp/r
workflows:
  - id: wf
    steps:
      - id: pulled
        action: pkg:pull
        pkg: foo
      - action: job:push
        kind: nba-apply
        anything: at all
        nested:
          deeper: value
`)

	settings, err := LoadSettings(path)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}

	steps := settings.Workflows[0].Steps
	if steps[0].Params["pkg"] != "foo" {
		t.Errorf("step 0 params = %+v", steps[0].Params)
	}
	if steps[1].Params["anything"] != "at all" {
		t.Errorf("step 1 params = %+v", steps[1].Params)
	}
	if nested, ok := steps[1].Params["nested"].(map[string]any); !ok || nested["deeper"] != "value" {
		t.Errorf("step 1 nested params = %+v", steps[1].Params["nested"])
	}
}

func TestLoadSettings_EmptyFileStillReportsTheMissingSetting(t *testing.T) {
	path := writeConfig(t, "")

	_, err := LoadSettings(path)
	if err == nil {
		t.Fatal("expected an error for an empty config")
	}
	if !strings.Contains(err.Error(), "at least one package is required") {
		t.Errorf("error %q should name the missing setting, not the empty document", err.Error())
	}
}
