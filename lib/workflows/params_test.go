package workflows

import (
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestResolveParams_OverrideWinsOverDefault(t *testing.T) {
	wf := Workflow{Params: []Param{{Name: "pkg", Type: "string", Default: "bar"}}}

	got, err := ResolveParams(wf, map[string]string{"pkg": "foo"})
	if err != nil {
		t.Fatalf("ResolveParams: %v", err)
	}
	if got["pkg"] != "foo" {
		t.Errorf("pkg = %v, want foo", got["pkg"])
	}
}

func TestResolveParams_FallsBackToDefault(t *testing.T) {
	wf := Workflow{Params: []Param{{Name: "path", Type: "string", Default: "*.zip"}}}

	got, err := ResolveParams(wf, map[string]string{})
	if err != nil {
		t.Fatalf("ResolveParams: %v", err)
	}
	if got["path"] != "*.zip" {
		t.Errorf("path = %v, want *.zip", got["path"])
	}
}

func TestResolveParams_MissingRequiredIsError(t *testing.T) {
	wf := Workflow{Params: []Param{{Name: "pkg", Required: true}}}

	if _, err := ResolveParams(wf, map[string]string{}); err == nil {
		t.Fatal("expected an error for a missing required param")
	}
}

func TestResolveParams_OptionalWithoutDefaultIsSimplyAbsent(t *testing.T) {
	wf := Workflow{Params: []Param{{Name: "optional"}}}

	got, err := ResolveParams(wf, map[string]string{})
	if err != nil {
		t.Fatalf("ResolveParams: %v", err)
	}
	if _, ok := got["optional"]; ok {
		t.Errorf("expected \"optional\" to be absent, got %v", got["optional"])
	}
}

func TestResolveParams_CoercesIntAndBool(t *testing.T) {
	wf := Workflow{Params: []Param{
		{Name: "count", Type: "int"},
		{Name: "flag", Type: "bool"},
	}}

	got, err := ResolveParams(wf, map[string]string{"count": "42", "flag": "true"})
	if err != nil {
		t.Fatalf("ResolveParams: %v", err)
	}
	if got["count"] != 42 {
		t.Errorf("count = %v (%T), want 42 (int)", got["count"], got["count"])
	}
	if got["flag"] != true {
		t.Errorf("flag = %v (%T), want true (bool)", got["flag"], got["flag"])
	}
}

func TestResolveParams_InvalidIntOverrideIsError(t *testing.T) {
	wf := Workflow{Params: []Param{{Name: "count", Type: "int"}}}

	if _, err := ResolveParams(wf, map[string]string{"count": "not-a-number"}); err == nil {
		t.Fatal("expected an error for a non-numeric int override")
	}
}

func TestResolveParams_UnsupportedTypeIsError(t *testing.T) {
	wf := Workflow{Params: []Param{{Name: "x", Type: "float"}}}

	if _, err := ResolveParams(wf, map[string]string{"x": "1.5"}); err == nil {
		t.Fatal("expected an error for an unsupported declared type")
	}
}

func TestResolveParams_NoOverrideAndDefaultLeavesDeclaredTypeAlone(t *testing.T) {
	// Defaults come straight from YAML (already a native Go value) and are
	// used as-is, no coercion applied.
	wf := Workflow{Params: []Param{{Name: "count", Type: "int", Default: 5}}}

	got, err := ResolveParams(wf, map[string]string{})
	if err != nil {
		t.Fatalf("ResolveParams: %v", err)
	}
	if got["count"] != 5 {
		t.Errorf("count = %v (%T), want 5 (int)", got["count"], got["count"])
	}
}

func TestWorkflow_ParamsYAMLParsing(t *testing.T) {
	raw := `
id: check-ldm
params:
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

	want := []Param{
		{Name: "pkg", Type: "string", Required: true},
		{Name: "path", Type: "string", Default: "*.zip"},
	}
	if !reflect.DeepEqual(wf.Params, want) {
		t.Errorf("Params = %+v, want %+v", wf.Params, want)
	}
}
