package workflows

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// logEntry is one captured log line, reduced to the fields these tests care
// about.
type logEntry struct {
	Level   string `json:"level"`
	Step    string `json:"step"`
	Action  string `json:"action"`
	Message string `json:"message"`
}

// runCapturingLogs runs wf at the given log level and returns every log line
// the engine produced.
func runCapturingLogs(t *testing.T, wf Workflow, registry *Registry, level zerolog.Level) ([]logEntry, error) {
	t.Helper()

	var buf bytes.Buffer
	err := Run(wf, registry, map[string]any{}, zerolog.New(&buf).Level(level))

	var entries []logEntry
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var entry logEntry
		if unmarshalErr := json.Unmarshal([]byte(line), &entry); unmarshalErr != nil {
			t.Fatalf("log line %q is not JSON: %v", line, unmarshalErr)
		}
		entries = append(entries, entry)
	}
	return entries, err
}

func messagesAtLevel(entries []logEntry, level string) []string {
	var messages []string
	for _, entry := range entries {
		if entry.Level == level {
			messages = append(messages, entry.Message)
		}
	}
	return messages
}

func TestRun_LogsEachStepStartingAndFinishingAtDebug(t *testing.T) {
	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "noop", run: func(ctx *StepContext) (StepResult, error) {
		return one(map[string]any{}), nil
	}})

	wf := Workflow{Steps: []Step{
		{Id: "first", Action: "noop"},
		{Id: "second", Action: "noop"},
	}}

	entries, err := runCapturingLogs(t, wf, registry, zerolog.DebugLevel)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	var got []string
	for _, entry := range entries {
		got = append(got, fmt.Sprintf("%s %s/%s %s", entry.Level, entry.Step, entry.Action, entry.Message))
	}
	want := []string{
		"debug first/noop step starting",
		"debug first/noop step finished",
		"debug second/noop step starting",
		"debug second/noop step finished",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("logged:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestRun_LogsNothingAtInfo(t *testing.T) {
	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "noop", run: func(ctx *StepContext) (StepResult, error) {
		return one(map[string]any{}), nil
	}})

	entries, err := runCapturingLogs(t, Workflow{Steps: []Step{{Id: "only", Action: "noop"}}}, registry, zerolog.InfoLevel)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected a silent run at info level, got %+v", entries)
	}
}

func TestRun_ActionLogsThroughAStepTaggedLogger(t *testing.T) {
	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "chatty", run: func(ctx *StepContext) (StepResult, error) {
		ctx.Logger.Debug().Msg("something an action wants to report")
		return one(map[string]any{}), nil
	}})

	entries, err := runCapturingLogs(t, Workflow{Steps: []Step{{Id: "talk", Action: "chatty"}}}, registry, zerolog.DebugLevel)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	for _, entry := range entries {
		if entry.Message != "something an action wants to report" {
			continue
		}
		if entry.Step != "talk" || entry.Action != "chatty" {
			t.Errorf("action log line carries step=%q action=%q, want talk/chatty", entry.Step, entry.Action)
		}
		return
	}
	t.Errorf("the action's own log line is missing from %+v", entries)
}

func TestRun_LogsResolvedParamsOnlyAtTrace(t *testing.T) {
	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "noop", run: func(ctx *StepContext) (StepResult, error) {
		return one(map[string]any{}), nil
	}})

	wf := Workflow{Steps: []Step{{Id: "only", Action: "noop", Params: map[string]any{"key": "value"}}}}

	atDebug, err := runCapturingLogs(t, wf, registry, zerolog.DebugLevel)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if messages := messagesAtLevel(atDebug, "trace"); len(messages) != 0 {
		t.Errorf("expected no trace lines at debug level, got %v", messages)
	}

	atTrace, err := runCapturingLogs(t, wf, registry, zerolog.TraceLevel)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if messages := messagesAtLevel(atTrace, "trace"); len(messages) != 1 || messages[0] != "step params resolved" {
		t.Errorf("trace lines = %v, want exactly one \"step params resolved\"", messages)
	}
}

func TestRun_LogsEachForkBranchAtDebug(t *testing.T) {
	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "find_each", run: func(ctx *StepContext) (StepResult, error) {
		return one(map[string]any{"path": "a"}, map[string]any{"path": "b"}), nil
	}})
	registry.Register(&funcAction{actionType: "noop", run: func(ctx *StepContext) (StepResult, error) {
		return one(map[string]any{}), nil
	}})

	wf := Workflow{Steps: []Step{
		{Id: "each", Action: "find_each"},
		{Id: "use", Action: "noop"},
	}}

	entries, err := runCapturingLogs(t, wf, registry, zerolog.DebugLevel)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	var branches int
	for _, entry := range entries {
		if entry.Message == "branch starting" {
			branches++
		}
	}
	if branches != 2 {
		t.Errorf("logged %d branch lines, want 2", branches)
	}
}

func TestRun_LogsNoBranchLineForANonForkingStep(t *testing.T) {
	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "noop", run: func(ctx *StepContext) (StepResult, error) {
		return one(map[string]any{}), nil
	}})

	entries, err := runCapturingLogs(t, Workflow{Steps: []Step{{Id: "only", Action: "noop"}}}, registry, zerolog.DebugLevel)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	for _, entry := range entries {
		if entry.Message == "branch starting" {
			t.Errorf("a single-output step should not log a branch line, got %+v", entries)
		}
	}
}

func TestRun_WarnsOnEachRetriedAttempt(t *testing.T) {
	var attempts int

	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "flaky", run: func(ctx *StepContext) (StepResult, error) {
		attempts++
		if attempts < 3 {
			return StepResult{}, fmt.Errorf("transient failure %d", attempts)
		}
		return one(map[string]any{}), nil
	}})

	wf := Workflow{Steps: []Step{{
		Id:          "flaky",
		Action:      "flaky",
		RetryPolicy: &RetryPolicy{Limit: 5, IntervalSec: 0},
	}}}

	entries, err := runCapturingLogs(t, wf, registry, zerolog.InfoLevel)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	warnings := messagesAtLevel(entries, "warn")
	if len(warnings) != 2 {
		t.Fatalf("logged %d warnings, want 2 (one per retried attempt): %+v", len(warnings), entries)
	}
	for _, message := range warnings {
		if message != "step attempt failed, retrying" {
			t.Errorf("warning message = %q", message)
		}
	}
}

func TestRun_ExhaustedRetryErrorNamesTheAttemptCount(t *testing.T) {
	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "always-fails", run: func(ctx *StepContext) (StepResult, error) {
		return StepResult{}, fmt.Errorf("nope")
	}})

	wf := Workflow{Steps: []Step{{
		Id:          "doomed",
		Action:      "always-fails",
		RetryPolicy: &RetryPolicy{Limit: 2, IntervalSec: 0},
	}}}

	err := Run(wf, registry, map[string]any{}, zerolog.Nop())
	if err == nil {
		t.Fatal("expected an error after exhausting retries")
	}
	if !strings.Contains(err.Error(), "after 3 attempts") {
		t.Errorf("error %q should name the attempt count", err.Error())
	}
	if !strings.Contains(err.Error(), "nope") {
		t.Errorf("error %q should keep the underlying failure", err.Error())
	}
}

func TestRun_UnresolvableParamErrorSaysItWasParamResolution(t *testing.T) {
	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "noop", run: func(ctx *StepContext) (StepResult, error) {
		return one(map[string]any{}), nil
	}})

	wf := Workflow{Steps: []Step{{
		Id:     "only",
		Action: "noop",
		Params: map[string]any{"path": "${outputs.nonexistent.path}"},
	}}}

	err := Run(wf, registry, map[string]any{}, zerolog.Nop())
	if err == nil {
		t.Fatal("expected an error for an unresolvable template")
	}
	if !strings.Contains(err.Error(), "resolving params") {
		t.Errorf("error %q should say param resolution was the failing phase", err.Error())
	}
}
