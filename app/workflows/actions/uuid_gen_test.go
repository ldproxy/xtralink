package actions

import (
	"bytes"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ldproxy/xtralink/lib/workflows"
)

func TestUUIDGenAction_ProducesOneParseableUUID(t *testing.T) {
	action := &UUIDGenAction{}

	result, err := action.Run(&workflows.StepContext{Params: map[string]any{}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(result.Outputs) != 1 {
		t.Fatalf("expected exactly 1 output set, got %+v", result.Outputs)
	}

	generated, ok := result.Outputs[0]["uuid"].(string)
	if !ok {
		t.Fatalf("output = %+v, want a uuid string", result.Outputs[0])
	}
	if _, err := uuid.Parse(generated); err != nil {
		t.Errorf("uuid %q does not parse: %v", generated, err)
	}
}

func TestUUIDGenAction_GeneratesAFreshUUIDEachRun(t *testing.T) {
	action := &UUIDGenAction{}
	seen := map[string]bool{}

	for i := 0; i < 100; i++ {
		result, err := action.Run(&workflows.StepContext{Params: map[string]any{}})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		generated, _ := result.Outputs[0]["uuid"].(string)
		if seen[generated] {
			t.Fatalf("uuid %q was generated twice", generated)
		}
		seen[generated] = true
	}
}

func TestUUIDGenAction_IgnoresAnyParameters(t *testing.T) {
	action := &UUIDGenAction{}

	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{"unexpected": "value"}}); err != nil {
		t.Errorf("Run with a stray parameter: %v", err)
	}
}

func TestUUIDGenAction_LogsTheUUIDAtDebug(t *testing.T) {
	var buf bytes.Buffer
	action := &UUIDGenAction{}

	result, err := action.Run(&workflows.StepContext{
		Params: map[string]any{},
		Logger: captureLogger(&buf),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	generated, _ := result.Outputs[0]["uuid"].(string)
	if !strings.Contains(buf.String(), generated) {
		t.Errorf("log %s should carry the generated uuid %q", buf.String(), generated)
	}
}
