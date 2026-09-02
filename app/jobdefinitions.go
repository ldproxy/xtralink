package app

import "fmt"

// JobDefinition is a Processor definition for one PartialJob kind, wrapping
// a single Workflow run - a flat, reusable registry entry, not a
// pre-declared multi-part pipeline. Configured under the .xtrasync.yml
// "jobs:" key.
//
// A Job's actual pipeline shape (which PartialJobs it has, in what order
// or parallelism) is decided by the Job itself when it is pushed
// (s. app/jobs.PushRequest.Sequential and model.PartialJob.Sequence),
// either as a single-PartialJob Job (`job push <kind>`, the Kind used
// directly for both the Job and its one PartialJob) or as an ad-hoc
// multi-part Job composed from several JobDefinitions (the job:push
// workflow action's `partials:` list).
type JobDefinition struct {
	// Kind is the PartialJob kind this definition handles - what `job push`,
	// `job process` and `partials[].kind` reference, and what the Runner
	// matches a queued PartialJob against (s. lib/jobs.Runner.processor).
	// Unique across every entry under jobs:, a flat namespace like
	// PartialJob.Kind has always been.
	//
	// It is a kind rather than an id on purpose: an id would suggest the
	// uuid `job get`/`job status` take, and this is a dispatch key.
	Kind string `yaml:"kind"`
	// Workflow is the id of a Workflow declared under workflows:.
	Workflow string `yaml:"workflow"`
	// Parameters, if present, is an explicit input mapping (template
	// expressions): every one of the Workflow's required params must
	// appear here. If absent, the Job's Inputs are mapped automatically by
	// field name onto the Workflow's declared params instead.
	Parameters map[string]any `yaml:"parameters,omitempty"`
	// Outputs is an explicit output mapping: keys become entries in the
	// shared Job's Outputs, values are template expressions resolved
	// against the finished Workflow run's own vars tree.
	Outputs map[string]any `yaml:"outputs,omitempty"`
}

// GetJobDefinition finds the JobDefinition for kind - the PartialJob.Kind a
// WorkflowJobProcessor is asked to process, or the Kind of a
// single-PartialJob Job pushed directly via `job push <kind>`.
func (s *Settings) GetJobDefinition(kind string) (*JobDefinition, error) {
	for i := range s.JobDefinitions {
		if s.JobDefinitions[i].Kind == kind {
			return &s.JobDefinitions[i], nil
		}
	}
	return nil, fmt.Errorf("job definition for kind %q not found", kind)
}

// validateJobDefinitions checks only what Settings itself can verify
// (kind uniqueness, a referenced workflow actually exists) - same split as
// validateWorkflows: whether the workflow's params are satisfiable by a
// definition's parameters mapping needs the Action registry and is deferred
// to app/workflows, just like Validate() already does for plain workflows.
func validateJobDefinitions(settings *Settings) error {
	seenKinds := map[string]bool{}

	for i, def := range settings.JobDefinitions {
		if def.Kind == "" {
			return fmt.Errorf("jobs[%d].kind is required", i)
		}
		if seenKinds[def.Kind] {
			return fmt.Errorf("jobs[%d]: duplicate kind %q", i, def.Kind)
		}
		seenKinds[def.Kind] = true

		if def.Workflow == "" {
			return fmt.Errorf("jobs[%d] (%s): workflow is required", i, def.Kind)
		}
		if !settings.HasWorkflow(def.Workflow) {
			return fmt.Errorf("jobs[%d] (%s): references unknown workflow %q", i, def.Kind, def.Workflow)
		}
	}

	return nil
}
