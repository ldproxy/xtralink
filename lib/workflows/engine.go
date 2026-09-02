package workflows

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/rs/zerolog"
)

// Run executes every Step of workflow in order, starting from vars (the
// caller-provided root namespace for template resolution - typically at
// least {"packages": {...}}, s. template.go). It does not know about
// locking a concurrent run of the same workflow out - that is an
// orchestration concern above this package (s. app/workflows/run.go).
//
// logger is the run-scoped logger the engine writes step progress to and
// derives every Action's own logger from; zerolog.Nop() disables all of it.
func Run(workflow Workflow, registry *Registry, vars map[string]any, logger zerolog.Logger) error {
	_, err := RunWithResults(workflow, registry, vars, logger)
	return err
}

// RunWithResults behaves exactly like Run, but additionally returns the
// vars tree at every leaf continuation reached - normally exactly one,
// unless some Step forked (s. StepResult's 0/1/N output-set model), in
// which case there's one per branch. A caller that wraps a Workflow (e.g. a
// Job whose steps each run one) needs this to resolve an output-mapping
// template expression against the workflow's own final outputs once it
// finishes - Run's plain error-only result can't answer that.
func RunWithResults(workflow Workflow, registry *Registry, vars map[string]any, logger zerolog.Logger) ([]map[string]any, error) {
	seeded := cloneTopLevel(vars)
	if _, ok := seeded["outputs"]; !ok {
		seeded["outputs"] = map[string]any{}
	}
	r := &runner{defaults: workflow.Defaults, registry: registry, logger: logger}

	leaves, runErr := r.from(workflow.Steps, 0, seeded)

	// The run's own error outranks anything the handlers report: it is what
	// actually went wrong, and a handler failing on top of it must not
	// replace it (s. runHandlers). errors.Join keeps both, the run's first.
	handlerErr := r.runHandlers(workflow.Handlers, vars, runErr)
	if runErr != nil {
		return nil, errors.Join(runErr, handlerErr)
	}
	if handlerErr != nil {
		return leaves, handlerErr
	}
	return leaves, nil
}

// runHandlers runs the Workflow's handler steps for the outcome runErr
// describes: Failure or Success, then Always. It returns what the handlers
// themselves failed with, if anything - never runErr, which the caller
// already has.
func (r *runner) runHandlers(handlers *Handlers, vars map[string]any, runErr error) error {
	if handlers == nil {
		return nil
	}

	outcome := "success"
	steps := handlers.Success
	if runErr != nil {
		outcome = "failure"
		steps = handlers.Failure
	}

	handlerVars := cloneTopLevel(vars)
	handlerVars["error"] = ErrorVars(runErr)

	var errs []error
	if err := r.runHandler(outcome, steps, handlerVars); err != nil {
		errs = append(errs, err)
	}
	if err := r.runHandler("always", handlers.Always, handlerVars); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (r *runner) runHandler(outcome string, steps []Step, vars map[string]any) error {
	if len(steps) == 0 {
		return nil
	}

	handler := &runner{
		defaults: r.defaults,
		registry: r.registry,
		logger:   r.logger.With().Str("handler", outcome).Logger(),
	}
	handler.logger.Debug().Int("steps", len(steps)).Msg("handler starting")

	if _, err := handler.from(steps, 0, vars); err != nil {
		handler.logger.Error().Err(err).Msg("handler failed")
		return fmt.Errorf("handlers.%s: %w", outcome, err)
	}

	handler.logger.Debug().Msg("handler finished")
	return nil
}

// ErrorVars builds the ${error} namespace handler steps resolve against:
// the message, and the id and action of the step that failed. Every field
// is empty when nothing failed, so an Always handler can name ${error...}
// without having to know which way the run went - an empty message is how
// it reads "no failure".
func ErrorVars(runErr error) map[string]any {
	vars := map[string]any{"message": "", "step": "", "action": ""}
	if runErr == nil {
		return vars
	}

	vars["message"] = runErr.Error()

	var stepErr *StepError
	if errors.As(runErr, &stepErr) {
		vars["step"] = stepErr.StepId
		vars["action"] = stepErr.Action
	}
	return vars
}

// runner holds what stays the same for the whole run, so the recursive
// descent below passes only what actually varies per call.
type runner struct {
	defaults *Defaults
	registry *Registry
	logger   zerolog.Logger
}

// from recursively executes steps[index:] against vars, returning the
// vars tree at every leaf reached. Every Step whose Action returns N output
// sets forks: the remaining steps run once per output set, independently,
// each with outputs.<stepId> set to that one output set (s. StepResult doc
// comment) and contributing its own leaves to the result. Nested forks are
// allowed and fall out of this naturally - no special-casing needed.
func (r *runner) from(steps []Step, index int, vars map[string]any) ([]map[string]any, error) {
	if index >= len(steps) {
		return []map[string]any{vars}, nil
	}

	step := steps[index]
	stepId := step.EffectiveId(index)
	stepLogger := r.logger.With().Str("step", stepId).Str("action", step.Action).Logger()

	stepLogger.Debug().Int("index", index).Msg("step starting")
	started := time.Now()

	result, err := r.step(step, stepLogger, vars)
	if err != nil {
		return nil, &StepError{Index: index, StepId: stepId, Action: step.Action, Err: err}
	}
	stepLogger.Debug().Dur("duration", time.Since(started)).Int("outputs", len(result.Outputs)).Msg("step finished")

	var leaves []map[string]any
	for branch, outputSet := range result.Outputs {
		if len(result.Outputs) > 1 {
			stepLogger.Debug().Int("branch", branch+1).Int("of", len(result.Outputs)).
				Interface("output", outputSet).Msg("branch starting")
		}

		branchVars := withOutput(vars, stepId, outputSet)
		branchLeaves, err := r.from(steps, index+1, branchVars)
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

// step resolves the Step's Action and parameters, then runs it, retrying on
// error per the effective RetryPolicy (the Step's own, or the Workflow's
// Defaults if the Step has none - s. effectiveRetryPolicy).
func (r *runner) step(step Step, stepLogger zerolog.Logger, vars map[string]any) (StepResult, error) {
	action, err := r.registry.Lookup(step.Action)
	if err != nil {
		return StepResult{}, err
	}

	resolved, err := ResolveValue(step.Params, vars)
	if err != nil {
		return StepResult{}, fmt.Errorf("resolving params: %w", err)
	}
	params, _ := resolved.(map[string]any)
	stepLogger.Trace().Interface("params", params).Msg("step params resolved")

	policy := effectiveRetryPolicy(step, r.defaults)
	attempts := 1
	if policy != nil && policy.Limit > 0 {
		attempts = policy.Limit + 1
	}

	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			delay := retryDelay(*policy, attempt)
			stepLogger.Warn().Err(lastErr).Int("attempt", attempt).Int("of", attempts).
				Dur("retry_in", delay).Msg("step attempt failed, retrying")
			time.Sleep(delay)
		}
		result, err := action.Run(&StepContext{Params: params, Logger: stepLogger})
		if err == nil {
			return result, nil
		}
		lastErr = err
	}
	if attempts > 1 {
		return StepResult{}, fmt.Errorf("after %d attempts: %w", attempts, lastErr)
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

// StepError identifies the Step a run failed on, so a failure handler can
// name it as ${error.step}/${error.action} (s. ErrorVars) rather than
// having to parse it back out of a message. Its own message is what the
// engine has always reported, so nothing reading the error as text sees a
// change.
type StepError struct {
	Index  int
	StepId string
	Action string
	Err    error
}

func (e *StepError) Error() string {
	return fmt.Sprintf("step %d (id=%s, action=%s): %v", e.Index, e.StepId, e.Action, e.Err)
}

func (e *StepError) Unwrap() error { return e.Err }
