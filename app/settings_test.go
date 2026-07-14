package app

import (
	"os"
	"path/filepath"
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
	if settings.Jobs.Queue != "local" {
		t.Errorf("Jobs.Queue = %q, want local", settings.Jobs.Queue)
	}
	if settings.Jobs.MaxConcurrent != 1 {
		t.Errorf("Jobs.MaxConcurrent = %d, want 1", settings.Jobs.MaxConcurrent)
	}
}

func TestLoadSettings_ParsesRedisQueueConfig(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
jobs:
  queue: redis
  maxConcurrent: 4

redis:
  nodes:
    - localhost:6379
    - localhost:6380
`)

	settings, err := LoadSettings(path)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}
	if settings.Jobs.Queue != "redis" {
		t.Errorf("Jobs.Queue = %q, want redis", settings.Jobs.Queue)
	}
	if settings.Jobs.MaxConcurrent != 4 {
		t.Errorf("Jobs.MaxConcurrent = %d, want 4", settings.Jobs.MaxConcurrent)
	}
	if len(settings.Redis.Nodes) != 2 || settings.Redis.Nodes[0] != "localhost:6379" || settings.Redis.Nodes[1] != "localhost:6380" {
		t.Errorf("Redis.Nodes = %v", settings.Redis.Nodes)
	}
}

func TestLoadSettings_RejectsInvalidQueueValue(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
jobs:
  queue: memcached
`)
	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for an invalid jobs.queue value")
	}
}

func TestLoadSettings_RejectsRedisQueueWithoutNodes(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
jobs:
  queue: redis
`)
	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for jobs.queue=redis without redis.nodes")
	}
}

func TestLoadSettings_RejectsNegativeMaxConcurrent(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
jobs:
  maxConcurrent: -1
`)
	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for a negative jobs.maxConcurrent")
	}
}

func TestLoadSettings_RejectsEmptyRedisNode(t *testing.T) {
	path := writeConfig(t, minimalPackage+`
jobs:
  queue: redis
redis:
  nodes:
    - ""
`)
	if _, err := LoadSettings(path); err == nil {
		t.Fatal("expected an error for an empty redis.nodes entry")
	}
}
