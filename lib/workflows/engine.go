package workflows

import (
	"fmt"
	"math"
	"time"
)

// Run executes every Step of workflow in order, starting from vars (the
// caller-provided root namespace for template resolution - typically at
// least {"packages": {...}}, s. template.go). It does not know about
// locking a concurrent run of the same workflow out - that is an
// orchestration concern above this package (s. app/workflows/run.go).
func Run(workflow Workflow, registry *Registry, vars map[string]any) error {
	_, err := RunWithResults(workflow, registry, vars)
	return err
}

// RunWithResults behaves exactly like Run, but additionally returns the
// vars tree at every leaf continuation reached - normally exactly one,
// unless some Step forked (s. StepResult's 0/1/N output-set model), in
// which case there's one per branch. A caller that wraps a Workflow (e.g. a
// Job whose steps each run one) needs this to resolve an output-mapping
// template expression against the workflow's own final outputs once it
// finishes - Run's plain error-only result can't answer that.
func RunWithResults(workflow Workflow, registry *Registry, vars map[string]any) ([]map[string]any, error) {
	seeded := cloneTopLevel(vars)
	if _, ok := seeded["outputs"]; !ok {
		seeded["outputs"] = map[string]any{}
	}
	return runFrom(workflow.Steps, 0, workflow.Defaults, registry, seeded)
}

// runFrom recursively executes steps[index:] against vars, returning the
// vars tree at every leaf reached. Every Step whose Action returns N output
// sets forks: the remaining steps run once per output set, independently,
// each with outputs.<stepId> set to that one output set (s. StepResult doc
// comment) and contributing its own leaves to the result. Nested forks are
// allowed and fall out of this naturally - no special-casing needed.
func runFrom(steps []Step, index int, defaults *Defaults, registry *Registry, vars map[string]any) ([]map[string]any, error) {
	if index >= len(steps) {
		return []map[string]any{vars}, nil
	}

	step := steps[index]
	stepId := step.EffectiveId(index)

	result, err := runStepWithRetry(step, defaults, registry, vars)
	if err != nil {
		return nil, fmt.Errorf("step %d (id=%s, action=%s): %w", index, stepId, step.Action, err)
	}

	var leaves []map[string]any
	for _, outputSet := range result.Outputs {
		branchVars := withOutput(vars, stepId, outputSet)
		branchLeaves, err := runFrom(steps, index+1, defaults, registry, branchVars)
		if err != nil {
			if len(result.Outputs) > 1 {
				return nil, fmt.Errorf("branch %s=%v: %w", stepId, outputSet, err)
			}
			return nil, err
		}
		leaves = append(leaves, branchLeaves...)
	}

	return leaves, nil
}

// runStepWithRetry resolves the Step's Action and parameters, then runs it,
// retrying on error per the effective RetryPolicy (the Step's own, or the
// Workflow's Defaults if the Step has none - s. effectiveRetryPolicy).
func runStepWithRetry(step Step, defaults *Defaults, registry *Registry, vars map[string]any) (StepResult, error) {
	action, err := registry.Lookup(step.Action)
	if err != nil {
		return StepResult{}, err
	}

	resolved, err := ResolveValue(step.Params, vars)
	if err != nil {
		return StepResult{}, err
	}
	params, _ := resolved.(map[string]any)

	policy := effectiveRetryPolicy(step, defaults)
	attempts := 1
	if policy != nil && policy.Limit > 0 {
		attempts = policy.Limit + 1
	}

	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			time.Sleep(retryDelay(*policy, attempt))
		}
		result, err := action.Run(&StepContext{Params: params})
		if err == nil {
			return result, nil
		}
		lastErr = err
	}
	return StepResult{}, lastErr
}

// effectiveRetryPolicy: a Step's own retry_policy, if set, replaces the
// Workflow's defaults.retry_policy entirely - no field-level merge, matching
// Dagu's retry_policy/defaults.retry_policy rule.
func effectiveRetryPolicy(step Step, defaults *Defaults) *RetryPolicy {
	if step.RetryPolicy != nil {
		return step.RetryPolicy
	}
	if defaults != nil {
		return defaults.RetryPolicy
	}
	return nil
}

// retryDelay computes the wait before the given retry attempt (1 = first
// retry): interval * backoff^(attempt-1), capped by MaxIntervalSec. Backoff
// <= 0 means a fixed interval.
func retryDelay(policy RetryPolicy, attempt int) time.Duration {
	interval := policy.IntervalSec
	if policy.Backoff > 0 {
		interval = policy.IntervalSec * math.Pow(float64(policy.Backoff), float64(attempt-1))
	}
	if policy.MaxIntervalSec > 0 && interval > policy.MaxIntervalSec {
		interval = policy.MaxIntervalSec
	}
	return time.Duration(interval * float64(time.Second))
}

func cloneTopLevel(vars map[string]any) map[string]any {
	out := make(map[string]any, len(vars)+1)
	for k, v := range vars {
		out[k] = v
	}
	return out
}

// withOutput returns a copy of vars with outputs.<stepId> set to output -
// a copy, not a mutation in place, so sibling fork branches never see each
// other's outputs.
func withOutput(vars map[string]any, stepId string, output map[string]any) map[string]any {
	newVars := cloneTopLevel(vars)

	outputs, _ := newVars["outputs"].(map[string]any)
	newOutputs := make(map[string]any, len(outputs)+1)
	for k, v := range outputs {
		newOutputs[k] = v
	}
	newOutputs[stepId] = output
	newVars["outputs"] = newOutputs

	return newVars
}
