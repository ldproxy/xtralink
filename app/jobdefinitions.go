package app

import "fmt"

// JobDefinition is a named pipeline of ordered PartialJob steps, each
// wrapping a single Workflow run - pushing a Job of this type creates one
// PartialJob per step, all belonging to that same Job (s. Job-Queue
// concept, jobs-as-workflow-wrapper).
type JobDefinition struct {
	// Id is the Job type, as referenced by `job push`/`job process`.
	Id string `yaml:"id"`
	// Parallel mirrors jobs.Job.Parallel - a pointer because YAML's zero
	// value for bool (false) can't be told apart from "not set" (which
	// should default to true) otherwise.
	Parallel *bool               `yaml:"parallel,omitempty"`
	Steps    []JobStepDefinition `yaml:"steps"`
}

// IsParallel returns the effective Parallel value - true (the default)
// unless explicitly set to false.
func (d JobDefinition) IsParallel() bool {
	return d.Parallel == nil || *d.Parallel
}

// JobStepDefinition is one PartialJob step of a JobDefinition, wrapping a
// single Workflow run.
type JobStepDefinition struct {
	// Id is this step's PartialJob type - what `job process <id>`
	// references, unique across all JobDefinitions, not just within one
	// (a flat namespace, like PartialJob.Type always has been).
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

func (s *Settings) GetJobDefinition(id string) (*JobDefinition, error) {
	for i := range s.JobDefinitions {
		if s.JobDefinitions[i].Id == id {
			return &s.JobDefinitions[i], nil
		}
	}
	return nil, fmt.Errorf("job definition with id %q not found", id)
}

// GetJobStep finds the step (and its owning JobDefinition) whose Id matches
// stepId - the PartialJob.Type a WorkflowJobProcessor is asked to process.
func (s *Settings) GetJobStep(stepId string) (*JobDefinition, *JobStepDefinition, error) {
	for i := range s.JobDefinitions {
		def := &s.JobDefinitions[i]
		for j := range def.Steps {
			if def.Steps[j].Id == stepId {
				return def, &def.Steps[j], nil
			}
		}
	}
	return nil, nil, fmt.Errorf("job step with id %q not found", stepId)
}

// validateJobDefinitions checks only what Settings itself can verify
// (id uniqueness, a referenced workflow actually exists) - same split as
// validateWorkflows: whether the workflow's params are satisfiable by a
// step's parameters mapping needs the Action registry and is deferred to
// app/workflows, just like Validate() already does for plain workflows.
func validateJobDefinitions(settings *Settings) error {
	seenDefIds := map[string]bool{}
	seenStepIds := map[string]bool{}

	for di, def := range settings.JobDefinitions {
		if def.Id == "" {
			return fmt.Errorf("jobDefinitions[%d].id is required", di)
		}
		if seenDefIds[def.Id] {
			return fmt.Errorf("jobDefinitions[%d]: duplicate id %q", di, def.Id)
		}
		seenDefIds[def.Id] = true

		if len(def.Steps) == 0 {
			return fmt.Errorf("jobDefinitions[%d] (%s): at least one step is required", di, def.Id)
		}

		for si, step := range def.Steps {
			if step.Id == "" {
				return fmt.Errorf("jobDefinitions[%d] (%s).steps[%d].id is required", di, def.Id, si)
			}
			if seenStepIds[step.Id] {
				return fmt.Errorf("jobDefinitions[%d] (%s).steps[%d]: duplicate step id %q across jobDefinitions", di, def.Id, si, step.Id)
			}
			seenStepIds[step.Id] = true

			if step.Workflow == "" {
				return fmt.Errorf("jobDefinitions[%d] (%s).steps[%d] (%s): workflow is required", di, def.Id, si, step.Id)
			}
			if !settings.HasWorkflow(step.Workflow) {
				return fmt.Errorf("jobDefinitions[%d] (%s).steps[%d] (%s): references unknown workflow %q", di, def.Id, si, step.Id, step.Workflow)
			}
		}
	}

	return nil
}
