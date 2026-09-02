package workflows

import (
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestResolveParameters_OverrideWinsOverDefault(t *testing.T) {
	wf := Workflow{Parameters: []Parameter{{Name: "pkg", Type: "string", Default: "bar"}}}

	got, err := ResolveParameters(wf, map[string]string{"pkg": "foo"})
	if err != nil {
		t.Fatalf("ResolveParameters: %v", err)
	}
	if got["pkg"] != "foo" {
		t.Errorf("pkg = %v, want foo", got["pkg"])
	}
}

func TestResolveParameters_FallsBackToDefault(t *testing.T) {
	wf := Workflow{Parameters: []Parameter{{Name: "path", Type: "string", Default: "*.zip"}}}

	got, err := ResolveParameters(wf, map[string]string{})
	if err != nil {
		t.Fatalf("ResolveParameters: %v", err)
	}
	if got["path"] != "*.zip" {
		t.Errorf("path = %v, want *.zip", got["path"])
	}
}

func TestResolveParameters_MissingRequiredIsError(t *testing.T) {
	wf := Workflow{Parameters: []Parameter{{Name: "pkg", Required: true}}}

	if _, err := ResolveParameters(wf, map[string]string{}); err == nil {
		t.Fatal("expected an error for a missing required param")
	}
}

func TestResolveParameters_OptionalWithoutDefaultIsSimplyAbsent(t *testing.T) {
	wf := Workflow{Parameters: []Parameter{{Name: "optional"}}}

	got, err := ResolveParameters(wf, map[string]string{})
	if err != nil {
		t.Fatalf("ResolveParameters: %v", err)
	}
	if _, ok := got["optional"]; ok {
		t.Errorf("expected \"optional\" to be absent, got %v", got["optional"])
	}
}

func TestResolveParameters_CoercesIntAndBool(t *testing.T) {
	wf := Workflow{Parameters: []Parameter{
		{Name: "count", Type: "int"},
		{Name: "flag", Type: "bool"},
	}}

	got, err := ResolveParameters(wf, map[string]string{"count": "42", "flag": "true"})
	if err != nil {
		t.Fatalf("ResolveParameters: %v", err)
	}
	if got["count"] != 42 {
		t.Errorf("count = %v (%T), want 42 (int)", got["count"], got["count"])
	}
	if got["flag"] != true {
		t.Errorf("flag = %v (%T), want true (bool)", got["flag"], got["flag"])
	}
}

func TestResolveParameters_InvalidIntOverrideIsError(t *testing.T) {
	wf := Workflow{Parameters: []Parameter{{Name: "count", Type: "int"}}}

	if _, err := ResolveParameters(wf, map[string]string{"count": "not-a-number"}); err == nil {
		t.Fatal("expected an error for a non-numeric int override")
	}
}

func TestResolveParameters_UnsupportedTypeIsError(t *testing.T) {
	wf := Workflow{Parameters: []Parameter{{Name: "x", Type: "float"}}}

	if _, err := ResolveParameters(wf, map[string]string{"x": "1.5"}); err == nil {
		t.Fatal("expected an error for an unsupported declared type")
	}
}

func TestResolveParameters_NoOverrideAndDefaultLeavesDeclaredTypeAlone(t *testing.T) {
	// Defaults come straight from YAML (already a native Go value) and are
	// used as-is, no coercion applied.
	wf := Workflow{Parameters: []Parameter{{Name: "count", Type: "int", Default: 5}}}

	got, err := ResolveParameters(wf, map[string]string{})
	if err != nil {
		t.Fatalf("ResolveParameters: %v", err)
	}
	if got["count"] != 5 {
		t.Errorf("count = %v (%T), want 5 (int)", got["count"], got["count"])
	}
}

func TestWorkflow_ParamsYAMLParsing(t *testing.T) {
	raw := `
id: check-ldm
parameters:
  - name: pkg
    type: string
    required: true
  - name: path
    type: string
    default: "*.zip"
steps:
  - action: pkg:find_any
`
	var wf Workflow
	if err := yaml.Unmarshal([]byte(raw), &wf); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	want := []Parameter{
		{Name: "pkg", Type: "string", Required: true},
		{Name: "path", Type: "string", Default: "*.zip"},
	}
	if !reflect.DeepEqual(wf.Parameters, want) {
		t.Errorf("Params = %+v, want %+v", wf.Parameters, want)
	}
}

func TestWorkflow_ParametersUseTheParametersKey(t *testing.T) {
	raw := `
id: wf
parameters:
  - name: pkg
    required: true
steps:
  - action: cmd:exec
    cmd: echo ${parameters.pkg}
`
	var wf Workflow
	if err := yaml.Unmarshal([]byte(raw), &wf); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(wf.Parameters) != 1 || wf.Parameters[0].Name != "pkg" {
		t.Fatalf("Parameters = %+v", wf.Parameters)
	}

	// The substitution namespace matches the declaration key.
	resolved, err := ResolveParameters(wf, map[string]string{"pkg": "foo"})
	if err != nil {
		t.Fatalf("ResolveParameters: %v", err)
	}
	value, err := ResolveValue(wf.Steps[0].Params["cmd"], map[string]any{"parameters": resolved})
	if err != nil {
		t.Fatalf("ResolveValue: %v", err)
	}
	if value != "echo foo" {
		t.Errorf("resolved cmd = %v, want \"echo foo\"", value)
	}
}
