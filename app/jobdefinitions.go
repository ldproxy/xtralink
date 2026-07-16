package app

import "fmt"

// JobDefinition is a Processor definition for one PartialJob type, wrapping
// a single Workflow run - a flat, reusable registry entry, not a
// pre-declared multi-step pipeline. A Job's actual pipeline shape (which
// PartialJobs it has, in what order/parallelism) is defined by the Job
// itself when it's pushed (s. jobs.Job.Parallel/jobs.PartialJob.Sequence),
// either as a single-PartialJob Job (`job push <id>`, Id used directly as
// both Job.Type and PartialJob.Type) or as an ad-hoc multi-step Job
// composed from several JobDefinitions (the job:push workflow action's
// `partials:` list).
type JobDefinition struct {
	// Id is this definition's PartialJob type - what `job push`/
	// `job process`/`partials[].type` reference, unique across all
	// JobDefinitions (a flat namespace, like PartialJob.Type always has
	// been).
	Id string `yaml:"id"`
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

// GetJobDefinition finds the JobDefinition whose Id matches id - the
// PartialJob.Type a WorkflowJobProcessor is asked to process, or the
// Job.Type of a single-PartialJob Job pushed directly via `job push <id>`.
func (s *Settings) GetJobDefinition(id string) (*JobDefinition, error) {
	for i := range s.JobDefinitions {
		if s.JobDefinitions[i].Id == id {
			return &s.JobDefinitions[i], nil
		}
	}
	return nil, fmt.Errorf("job definition with id %q not found", id)
}

// validateJobDefinitions checks only what Settings itself can verify
// (id uniqueness, a referenced workflow actually exists) - same split as
// validateWorkflows: whether the workflow's params are satisfiable by a
// step's parameters mapping needs the Action registry and is deferred to
// app/workflows, just like Validate() already does for plain workflows.
func validateJobDefinitions(settings *Settings) error {
	seenIds := map[string]bool{}

	for i, def := range settings.JobDefinitions {
		if def.Id == "" {
			return fmt.Errorf("jobDefinitions[%d].id is required", i)
		}
		if seenIds[def.Id] {
			return fmt.Errorf("jobDefinitions[%d]: duplicate id %q", i, def.Id)
		}
		seenIds[def.Id] = true

		if def.Workflow == "" {
			return fmt.Errorf("jobDefinitions[%d] (%s): workflow is required", i, def.Id)
		}
		if !settings.HasWorkflow(def.Workflow) {
			return fmt.Errorf("jobDefinitions[%d] (%s): references unknown workflow %q", i, def.Id, def.Workflow)
		}
	}

	return nil
}
