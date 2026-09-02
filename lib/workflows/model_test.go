package workflows

import (
	"encoding/json"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestWorkflow_YAMLParsing(t *testing.T) {
	raw := `
id: check-ldm
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
    retry_policy:
      limit: 5
      interval_sec: 10
      backoff: true
      max_interval_sec: 30
    from: foo
    to: bar
    path: ${outputs.input.path}
  - action: job:push
    type: nba-apply
    inputs:
      - name: package
        value: ${packages.bar.url}
`
	var wf Workflow
	if err := yaml.Unmarshal([]byte(raw), &wf); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if wf.Id != "check-ldm" {
		t.Errorf("Id = %q", wf.Id)
	}
	if wf.Defaults == nil || wf.Defaults.RetryPolicy == nil {
		t.Fatal("expected Defaults.RetryPolicy to be set")
	}
	if wf.Defaults.RetryPolicy.Limit != 2 || wf.Defaults.RetryPolicy.IntervalSec != 5 {
		t.Errorf("Defaults.RetryPolicy = %+v", wf.Defaults.RetryPolicy)
	}

	if len(wf.Steps) != 3 {
		t.Fatalf("len(Steps) = %d, want 3", len(wf.Steps))
	}

	step0 := wf.Steps[0]
	if step0.Id != "input" || step0.Action != "pkg:find_each" {
		t.Errorf("step0 = %+v", step0)
	}
	if step0.RetryPolicy != nil {
		t.Errorf("step0 should have no own retry_policy, got %+v", step0.RetryPolicy)
	}
	if step0.Params["pkg"] != "foo" || step0.Params["path"] != "*.zip" {
		t.Errorf("step0.Params = %+v, want pkg/path to land there via the inline map", step0.Params)
	}
	// "id", "action" and "retry_policy" are named fields, not action params -
	// they must NOT also leak into the inline Params map.
	for _, key := range []string{"id", "action", "retry_policy"} {
		if _, ok := step0.Params[key]; ok {
			t.Errorf("Params leaked the named field %q", key)
		}
	}

	step1 := wf.Steps[1]
	if step1.EffectiveId(1) != "1" {
		t.Errorf("EffectiveId = %q, want \"1\" (no explicit id, index 1)", step1.EffectiveId(1))
	}
	if step1.RetryPolicy == nil {
		t.Fatal("expected step1 to have its own retry_policy")
	}
	if step1.RetryPolicy.Limit != 5 || step1.RetryPolicy.IntervalSec != 10 || step1.RetryPolicy.MaxIntervalSec != 30 {
		t.Errorf("step1.RetryPolicy = %+v", step1.RetryPolicy)
	}
	if step1.RetryPolicy.Backoff != 2.0 {
		t.Errorf("backoff: true should parse as 2.0, got %v", step1.RetryPolicy.Backoff)
	}
	if step1.Params["from"] != "foo" || step1.Params["to"] != "bar" {
		t.Errorf("step1.Params = %+v", step1.Params)
	}

	step2 := wf.Steps[2]
	inputs, ok := step2.Params["inputs"].([]any)
	if !ok || len(inputs) != 1 {
		t.Fatalf("step2.Params[\"inputs\"] = %#v, want a one-element slice", step2.Params["inputs"])
	}
}

func TestBackoff_UnmarshalYAML(t *testing.T) {
	cases := []struct {
		yaml string
		want Backoff
	}{
		{"backoff: true", 2.0},
		{"backoff: false", 0},
		{"backoff: 3.5", 3.5},
		{"backoff: 2", 2.0},
	}
	for _, c := range cases {
		var wrapper struct {
			Backoff Backoff `yaml:"backoff"`
		}
		if err := yaml.Unmarshal([]byte(c.yaml), &wrapper); err != nil {
			t.Fatalf("Unmarshal(%q): %v", c.yaml, err)
		}
		if wrapper.Backoff != c.want {
			t.Errorf("Unmarshal(%q) = %v, want %v", c.yaml, wrapper.Backoff, c.want)
		}
	}
}

func TestStep_EffectiveId(t *testing.T) {
	if got := (Step{Id: "input"}).EffectiveId(0); got != "input" {
		t.Errorf("EffectiveId = %q, want input", got)
	}
	if got := (Step{}).EffectiveId(2); got != "2" {
		t.Errorf("EffectiveId = %q, want \"2\"", got)
	}
}

func TestWorkflow_DescriptionIsOptional(t *testing.T) {
	withDescription := `
id: check-ldm
description: moves finished packages over to the archive
steps:
  - action: pkg:pull
    pkg: foo
`
	var wf Workflow
	if err := yaml.Unmarshal([]byte(withDescription), &wf); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if wf.Description != "moves finished packages over to the archive" {
		t.Errorf("Description = %q", wf.Description)
	}

	var without Workflow
	if err := yaml.Unmarshal([]byte("id: bare\nsteps: []\n"), &without); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if without.Description != "" {
		t.Errorf("Description = %q, want empty", without.Description)
	}
}

// `flow get` prints a Workflow back out, so its JSON has to read like the
// configuration file it came from - action parameters inline on the step,
// not buried in a nested object.
func TestWorkflow_JSONRoundTripsTheConfiguredShape(t *testing.T) {
	raw := `
id: check-ldm
description: what this does
params:
  - name: pkg
    required: true
defaults:
  retry_policy:
    limit: 2
    interval_sec: 5
steps:
  - id: pulled
    action: pkg:pull
    pkg: ${params.pkg}
  - action: cmd:exec
    retry_policy:
      limit: 1
      interval_sec: 0
    cmd: echo hello
`
	var wf Workflow
	if err := yaml.Unmarshal([]byte(raw), &wf); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	encoded, err := json.Marshal(wf)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got["id"] != "check-ldm" || got["description"] != "what this does" {
		t.Errorf("id/description = %v/%v", got["id"], got["description"])
	}

	steps, _ := got["steps"].([]any)
	if len(steps) != 2 {
		t.Fatalf("steps = %+v, want 2", steps)
	}

	first, _ := steps[0].(map[string]any)
	if first["id"] != "pulled" || first["action"] != "pkg:pull" {
		t.Errorf("first step = %+v", first)
	}
	if first["pkg"] != "${params.pkg}" {
		t.Errorf("first step: pkg = %v, want the action parameter inline", first["pkg"])
	}
	if _, nested := first["Params"]; nested {
		t.Errorf("first step exposes a nested Params object: %+v", first)
	}

	second, _ := steps[1].(map[string]any)
	if second["cmd"] != "echo hello" {
		t.Errorf("second step: cmd = %v", second["cmd"])
	}
	if _, ok := second["id"]; ok {
		t.Errorf("second step declares no id, so none should be printed: %+v", second)
	}
	if policy, ok := second["retry_policy"].(map[string]any); !ok || policy["limit"] != float64(1) {
		t.Errorf("second step: retry_policy = %v", second["retry_policy"])
	}
}
