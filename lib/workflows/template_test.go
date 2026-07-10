package workflows

import (
	"reflect"
	"testing"
)

func TestResolveValue_WholePlaceholderReturnsUnderlyingType(t *testing.T) {
	vars := map[string]any{
		"outputs": map[string]any{
			"input": map[string]any{"path": "a.zip", "count": 3},
		},
	}

	got, err := ResolveValue("${outputs.input.path}", vars)
	if err != nil {
		t.Fatalf("ResolveValue: %v", err)
	}
	if got != "a.zip" {
		t.Errorf("got %v, want a.zip", got)
	}

	got, err = ResolveValue("${outputs.input.count}", vars)
	if err != nil {
		t.Fatalf("ResolveValue: %v", err)
	}
	if got != 3 {
		t.Errorf("got %v (%T), want 3 (int)", got, got)
	}
}

func TestResolveValue_EmbeddedPlaceholderStringifies(t *testing.T) {
	vars := map[string]any{"packages": map[string]any{"bar": map[string]any{"url": "s3://bucket"}}}

	got, err := ResolveValue("prefix-${packages.bar.url}-suffix", vars)
	if err != nil {
		t.Fatalf("ResolveValue: %v", err)
	}
	if got != "prefix-s3://bucket-suffix" {
		t.Errorf("got %v", got)
	}
}

func TestResolveValue_UnknownPathIsError(t *testing.T) {
	vars := map[string]any{"outputs": map[string]any{}}

	if _, err := ResolveValue("${outputs.missing.path}", vars); err == nil {
		t.Fatal("expected an error for an unresolvable path, got nil")
	}
}

func TestResolveValue_RecursesIntoMapsAndSlices(t *testing.T) {
	vars := map[string]any{"outputs": map[string]any{"input": map[string]any{"path": "a.zip"}}}

	params := map[string]any{
		"type": "nba-apply",
		"inputs": []any{
			map[string]any{"name": "file", "value": "${outputs.input.path}"},
		},
	}

	got, err := ResolveValue(params, vars)
	if err != nil {
		t.Fatalf("ResolveValue: %v", err)
	}
	want := map[string]any{
		"type": "nba-apply",
		"inputs": []any{
			map[string]any{"name": "file", "value": "a.zip"},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}
}

func TestResolveValue_NonStringScalarsPassThrough(t *testing.T) {
	vars := map[string]any{}
	for _, v := range []any{42, true, 3.14, nil} {
		got, err := ResolveValue(v, vars)
		if err != nil {
			t.Fatalf("ResolveValue(%v): %v", v, err)
		}
		if got != v {
			t.Errorf("ResolveValue(%v) = %v, want unchanged", v, got)
		}
	}
}
