package workflows

import (
	"fmt"
	"testing"
	"time"
)

// funcAction lets tests supply Run() as a plain closure instead of a new
// named type per test, mirroring lib/jobs' funcProcessor test helper.
type funcAction struct {
	actionType string
	run        func(ctx *StepContext) (StepResult, error)
}

func (a *funcAction) Type() string { return a.actionType }
func (a *funcAction) Run(ctx *StepContext) (StepResult, error) {
	return a.run(ctx)
}

func one(outputs ...map[string]any) StepResult {
	return StepResult{Outputs: outputs}
}

func TestRun_LinearSequencePassesOutputsForward(t *testing.T) {
	var seenByStep2 any

	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "find", run: func(ctx *StepContext) (StepResult, error) {
		return one(map[string]any{"path": "a.zip"}), nil
	}})
	registry.Register(&funcAction{actionType: "use", run: func(ctx *StepContext) (StepResult, error) {
		seenByStep2 = ctx.Params["path"]
		return one(map[string]any{}), nil
	}})

	wf := Workflow{
		Id: "wf",
		Steps: []Step{
			{Id: "input", Action: "find"},
			{Action: "use", Params: map[string]any{"path": "${outputs.input.path}"}},
		},
	}

	if err := Run(wf, registry, map[string]any{}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if seenByStep2 != "a.zip" {
		t.Errorf("step 2 saw path=%v, want a.zip", seenByStep2)
	}
}

func TestRun_ZeroOutputsHaltsWithoutError(t *testing.T) {
	var laterStepRan bool

	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "find", run: func(ctx *StepContext) (StepResult, error) {
		return StepResult{Outputs: nil}, nil // 0 output sets
	}})
	registry.Register(&funcAction{actionType: "use", run: func(ctx *StepContext) (StepResult, error) {
		laterStepRan = true
		return one(map[string]any{}), nil
	}})

	wf := Workflow{Steps: []Step{{Id: "input", Action: "find"}, {Action: "use"}}}

	if err := Run(wf, registry, map[string]any{}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if laterStepRan {
		t.Error("expected the step after a 0-output-set step to not run")
	}
}

func TestRun_NOutputsForksRemainingStepsIndependently(t *testing.T) {
	// Fork branches run sequentially, not concurrently (s. engine.go), so
	// no locking is needed around this slice.
	var seenPaths []string

	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "find_each", run: func(ctx *StepContext) (StepResult, error) {
		return one(
			map[string]any{"path": "a.zip"},
			map[string]any{"path": "b.zip"},
			map[string]any{"path": "c.zip"},
		), nil
	}})
	registry.Register(&funcAction{actionType: "use", run: func(ctx *StepContext) (StepResult, error) {
		seenPaths = append(seenPaths, ctx.Params["path"].(string))
		return one(map[string]any{}), nil
	}})

	wf := Workflow{
		Steps: []Step{
			{Id: "input", Action: "find_each"},
			{Action: "use", Params: map[string]any{"path": "${outputs.input.path}"}},
		},
	}

	if err := Run(wf, registry, map[string]any{}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(seenPaths) != 3 {
		t.Fatalf("expected 3 fork branches to run, got %d: %v", len(seenPaths), seenPaths)
	}
	want := map[string]bool{"a.zip": true, "b.zip": true, "c.zip": true}
	for _, p := range seenPaths {
		if !want[p] {
			t.Errorf("unexpected path seen: %s", p)
		}
		delete(want, p)
	}
	if len(want) != 0 {
		t.Errorf("missing paths: %v", want)
	}
}

func TestRun_NestedForksProduceCartesianProduct(t *testing.T) {
	var combos []string

	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "find_each", run: func(ctx *StepContext) (StepResult, error) {
		return one(map[string]any{"v": "1"}, map[string]any{"v": "2"}), nil
	}})
	registry.Register(&funcAction{actionType: "use", run: func(ctx *StepContext) (StepResult, error) {
		combos = append(combos, fmt.Sprintf("%v-%v", ctx.Params["a"], ctx.Params["b"]))
		return one(map[string]any{}), nil
	}})

	wf := Workflow{
		Steps: []Step{
			{Id: "s1", Action: "find_each"},
			{Id: "s2", Action: "find_each"},
			{Action: "use", Params: map[string]any{"a": "${outputs.s1.v}", "b": "${outputs.s2.v}"}},
		},
	}

	if err := Run(wf, registry, map[string]any{}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(combos) != 4 {
		t.Fatalf("expected 2x2=4 combinations, got %d: %v", len(combos), combos)
	}
}

func TestRun_FailingStepAbortsBranch(t *testing.T) {
	var secondStepRan bool

	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "fail", run: func(ctx *StepContext) (StepResult, error) {
		return StepResult{}, fmt.Errorf("boom")
	}})
	registry.Register(&funcAction{actionType: "use", run: func(ctx *StepContext) (StepResult, error) {
		secondStepRan = true
		return one(map[string]any{}), nil
	}})

	wf := Workflow{Steps: []Step{{Action: "fail"}, {Action: "use"}}}

	err := Run(wf, registry, map[string]any{})
	if err == nil {
		t.Fatal("expected an error")
	}
	if secondStepRan {
		t.Error("expected the step after a failing step to not run")
	}
}

func TestRun_NoRetryPolicyAnywhereMeansSingleAttempt(t *testing.T) {
	var attempts int

	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "fail", run: func(ctx *StepContext) (StepResult, error) {
		attempts++
		return StepResult{}, fmt.Errorf("boom")
	}})

	// Neither the Step nor the Workflow define a retry_policy.
	wf := Workflow{Steps: []Step{{Action: "fail"}}}

	if err := Run(wf, registry, map[string]any{}); err == nil {
		t.Fatal("expected an error")
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 - no retry_policy anywhere means a single attempt, no retry", attempts)
	}
}

func TestRun_UnknownActionIsError(t *testing.T) {
	wf := Workflow{Steps: []Step{{Action: "does-not-exist"}}}
	if err := Run(wf, NewRegistry(), map[string]any{}); err == nil {
		t.Fatal("expected an error for an unregistered action type")
	}
}

func TestRun_RetryPolicyRetriesUpToLimitThenSucceeds(t *testing.T) {
	var attempts int32

	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "flaky", run: func(ctx *StepContext) (StepResult, error) {
		attempts++
		if attempts < 3 {
			return StepResult{}, fmt.Errorf("transient failure %d", attempts)
		}
		return one(map[string]any{}), nil
	}})

	wf := Workflow{Steps: []Step{{
		Action:      "flaky",
		RetryPolicy: &RetryPolicy{Limit: 5, IntervalSec: 0},
	}}}

	if err := Run(wf, registry, map[string]any{}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3 (2 failures + 1 success)", attempts)
	}
}

func TestRun_RetryPolicyGivesUpAfterLimit(t *testing.T) {
	var attempts int32

	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "always-fails", run: func(ctx *StepContext) (StepResult, error) {
		attempts++
		return StepResult{}, fmt.Errorf("nope")
	}})

	wf := Workflow{Steps: []Step{{
		Action:      "always-fails",
		RetryPolicy: &RetryPolicy{Limit: 2, IntervalSec: 0},
	}}}

	if err := Run(wf, registry, map[string]any{}); err == nil {
		t.Fatal("expected an error after exhausting retries")
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3 (1 initial + 2 retries)", attempts)
	}
}

func TestRun_StepRetryPolicyReplacesWorkflowDefaultsCompletely(t *testing.T) {
	var attempts int32

	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "always-fails", run: func(ctx *StepContext) (StepResult, error) {
		attempts++
		return StepResult{}, fmt.Errorf("nope")
	}})

	wf := Workflow{
		Defaults: &Defaults{RetryPolicy: &RetryPolicy{Limit: 10, IntervalSec: 0}},
		Steps: []Step{{
			Action:      "always-fails",
			RetryPolicy: &RetryPolicy{Limit: 1, IntervalSec: 0}, // overrides Defaults entirely
		}},
	}

	if err := Run(wf, registry, map[string]any{}); err == nil {
		t.Fatal("expected an error")
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2 (1 initial + 1 retry from the step's own policy, not 10 from defaults)", attempts)
	}
}

func TestRun_StepWithoutRetryPolicyInheritsWorkflowDefaults(t *testing.T) {
	var attempts int32

	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "flaky", run: func(ctx *StepContext) (StepResult, error) {
		attempts++
		if attempts < 3 {
			return StepResult{}, fmt.Errorf("transient")
		}
		return one(map[string]any{}), nil
	}})

	wf := Workflow{
		Defaults: &Defaults{RetryPolicy: &RetryPolicy{Limit: 5, IntervalSec: 0}},
		Steps:    []Step{{Action: "flaky"}}, // no own retry_policy
	}

	if err := Run(wf, registry, map[string]any{}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
}

func TestRetryDelay_FixedIntervalWithoutBackoff(t *testing.T) {
	policy := RetryPolicy{IntervalSec: 2}
	for attempt := 1; attempt <= 3; attempt++ {
		if got := retryDelay(policy, attempt); got != 2*time.Second {
			t.Errorf("retryDelay(attempt=%d) = %v, want 2s", attempt, got)
		}
	}
}

func TestRetryDelay_ExponentialBackoffCappedByMaxInterval(t *testing.T) {
	policy := RetryPolicy{IntervalSec: 2, Backoff: 2.0, MaxIntervalSec: 30}

	cases := map[int]time.Duration{
		1: 2 * time.Second,  // 2 * 2^0
		2: 4 * time.Second,  // 2 * 2^1
		3: 8 * time.Second,  // 2 * 2^2
		4: 16 * time.Second, // 2 * 2^3
		5: 30 * time.Second, // 2 * 2^4 = 32, capped to 30
	}
	for attempt, want := range cases {
		if got := retryDelay(policy, attempt); got != want {
			t.Errorf("retryDelay(attempt=%d) = %v, want %v", attempt, got, want)
		}
	}
}

func TestRun_PackagesVarsAreVisibleToFirstStep(t *testing.T) {
	var seenURL any

	registry := NewRegistry()
	registry.Register(&funcAction{actionType: "use", run: func(ctx *StepContext) (StepResult, error) {
		seenURL = ctx.Params["url"]
		return one(map[string]any{}), nil
	}})

	wf := Workflow{Steps: []Step{{Action: "use", Params: map[string]any{"url": "${packages.bar.url}"}}}}
	vars := map[string]any{"packages": map[string]any{"bar": map[string]any{"url": "s3://bucket"}}}

	if err := Run(wf, registry, vars); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if seenURL != "s3://bucket" {
		t.Errorf("got %v, want s3://bucket", seenURL)
	}
}
