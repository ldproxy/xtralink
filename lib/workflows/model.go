// Package workflows implements the generic Workflow/Step execution engine:
// a workflow is a named sequence of Steps, each running a registered
// Action. The engine knows nothing about packages or jobs - concrete
// Actions live in app/workflows/actions.
package workflows

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// Workflow is a named sequence of Steps, as configured under the
// .xtrasync.yml "workflows:" key.
type Workflow struct {
	Id       string    `yaml:"id"`
	Params   []Param   `yaml:"params,omitempty"`
	Defaults *Defaults `yaml:"defaults,omitempty"`
	Steps    []Step    `yaml:"steps"`
}

// Param declares one runtime input a Workflow expects, read from Steps via
// ${params.<name>} (s. ResolveParams in params.go). Deliberately just these
// four fields - no enum/minimum-style value validation.
type Param struct {
	Name string `yaml:"name"`
	// Type is "string" (the default if omitted) or "int"/"bool" for basic
	// coercion of CLI-provided overrides (s. ResolveParams).
	Type     string `yaml:"type,omitempty"`
	Default  any    `yaml:"default,omitempty"`
	Required bool   `yaml:"required,omitempty"`
}

// Defaults holds workflow-wide fallbacks applied to every Step that doesn't
// declare its own value (currently only RetryPolicy).
type Defaults struct {
	RetryPolicy *RetryPolicy `yaml:"retry_policy,omitempty"`
}

// Step is a single entry in Workflow.Steps: an Action type plus its
// parameters. Action-specific parameters are deliberately not modeled as
// fixed struct fields (Params would have to grow for every new Action type)
// - they land in Params instead, and each Action interprets its own keys.
type Step struct {
	Id          string         `yaml:"id,omitempty"`
	Action      string         `yaml:"action"`
	RetryPolicy *RetryPolicy   `yaml:"retry_policy,omitempty"`
	Params      map[string]any `yaml:",inline"`
}

// EffectiveId returns Step.Id, or the given zero-based index as a string if
// Id was left empty.
func (s Step) EffectiveId(index int) string {
	if s.Id != "" {
		return s.Id
	}
	return fmt.Sprintf("%d", index)
}

// RetryPolicy configures how many times, and with what delay, a failing Step
// is retried before its branch is given up on. Modeled after Dagu's
// step-level retry_policy (https://docs.dagu.sh/writing-workflows/durable-execution#step-retry-policy),
// minus the exit_code filter: Actions are native Go functions that return
// error, not shell commands with an OS exit code to filter on.
type RetryPolicy struct {
	// Limit is the maximum number of retries after the first failure (not
	// the total number of attempts).
	Limit int `yaml:"limit"`
	// IntervalSec is the base delay between attempts, in seconds.
	IntervalSec float64 `yaml:"interval_sec"`
	// Backoff multiplies IntervalSec by Backoff^attempt for each subsequent
	// retry; 0 (the default) means a fixed interval.
	Backoff Backoff `yaml:"backoff,omitempty"`
	// MaxIntervalSec caps the computed delay, if > 0.
	MaxIntervalSec float64 `yaml:"max_interval_sec,omitempty"`
}

// Backoff is a float64 that also accepts YAML bool literals (true == 2.0,
// false == 0), matching Dagu's retry_policy.backoff convenience syntax.
type Backoff float64

func (b *Backoff) UnmarshalYAML(node *yaml.Node) error {
	var raw any
	if err := node.Decode(&raw); err != nil {
		return err
	}
	switch v := raw.(type) {
	case bool:
		if v {
			*b = 2.0
		} else {
			*b = 0
		}
	case int:
		*b = Backoff(v)
	case float64:
		*b = Backoff(v)
	default:
		return fmt.Errorf("backoff: expected a bool or a number, got %T", v)
	}
	return nil
}

// StepResult is what an Action.Run call returns: 0, 1 or N output sets.
// The engine runs every Step following this one once per output set (Fork) -
// 0 means the branch ends here successfully (no error, nothing further
// runs), 1 is the ordinary linear case, N fans out into N independent
// continuations.
type StepResult struct {
	Outputs []map[string]any
}

// Success is the common case for an Action that succeeded but has nothing
// meaningful to report (e.g. job:push, pkg:mv_file): a single, empty output
// set, so the branch continues exactly once. Analogous to Success() in
// lib/jobs/processor.go.
func Success() StepResult {
	return StepResult{Outputs: []map[string]any{{}}}
}

// Halt is a successful result that ends this branch here - the remaining
// Steps do not run, and this is not an error (e.g. pkg:find_any/find_each
// matching nothing).
func Halt() StepResult {
	return StepResult{}
}

// One returns a StepResult carrying a single output set.
func One(output map[string]any) StepResult {
	return StepResult{Outputs: []map[string]any{output}}
}

// Many returns a StepResult carrying N output sets - the remaining Steps
// fork, running once per entry (s. Halt/Success doc comments above for the
// full 0/1/N rationale).
func Many(outputs []map[string]any) StepResult {
	return StepResult{Outputs: outputs}
}
