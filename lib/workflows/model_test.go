package workflows

import (
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
