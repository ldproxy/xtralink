package workflows

import (
	"fmt"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/jobs"
	"github.com/ldproxy/xtralink/lib/workflows"
	"github.com/ldproxy/xtralink/model"
)

// WorkflowJobProcessor makes a Job a thin wrapper around a single Workflow
// run: one instance handles exactly one JobDefinition (its JobType() is
// that definition's id), resolving its input-parameter mapping, running the
// referenced Workflow, and writing its output mapping into the shared
// Job's Outputs. A multi-step Job (several JobDefinitions composed ad-hoc,
// s. the job:push workflow action's `partials:`) registers one
// WorkflowJobProcessor per JobDefinition - not one processor handling all
// of them - so `job process <id>` can scale a single PartialJob type's
// workers independently of the rest.
func WorkflowJobProcessor(appCtx *app.AppContext, stepId string) (*jobs.JobProcessor, error) {
	def, err := appCtx.Settings.GetJobDefinition(stepId)
	if err != nil {
		return nil, err
	}
	return &jobs.JobProcessor{Kind: stepId, Priority: 1000, Process: func(partialJob *model.PartialJob, job *model.Job, backend jobs.Backend) model.JobResult {
		if job == nil {
			// Can legitimately happen if the Job was deleted/expired while an
			// orphaned PartialJob for it still lingered in the queue (s. the
			// same guard in tileSeedingSetupProcessor) - fail this PartialJob
			// instead of panicking on a nil dereference below.
			return model.Error(fmt.Sprintf("partial job %s has no job (partOf=%q)", partialJob.Id, partialJob.PartOf))
		}

		wf, err := appCtx.Settings.GetWorkflow(def.Workflow)
		if err != nil {
			return model.Error(err.Error())
		}

		params, err := resolveParams(appCtx, def, wf, job)
		if err != nil {
			return model.Error(fmt.Sprintf("resolving parameters: %v", err))
		}

		registry := NewRegistry(appCtx)
		if err := Validate(appCtx, *wf, registry); err != nil {
			return model.Error(fmt.Sprintf("workflow %q is invalid: %v", wf.Id, err))
		}

		vars := map[string]any{
			"packages": packageVars(appCtx.Settings.Packages),
			"params":   params,
		}
		leaves, err := workflows.RunWithResults(*wf, registry, vars)
		if err != nil {
			return model.Error(fmt.Sprintf("workflow %q failed: %v", wf.Id, err))
		}
		// A job-wrapped Workflow is expected to complete linearly - a step that
		// forks (pkg:find_each and the like) would produce more than one
		// result here, and there's no defined way to pick "the" one for the
		// outputs mapping below, so treat it as a configuration error rather
		// than silently guessing.
		if len(leaves) != 1 {
			return model.Error(fmt.Sprintf(
				"workflow %q produced %d parallel results, expected exactly 1 - job-wrapped workflows must not fork", wf.Id, len(leaves)))
		}

		if err := writeOutputs(backend, job.Id, leaves[0], def); err != nil {
			return model.Error(fmt.Sprintf("writing outputs: %v", err))
		}

		// This step has no progressDetails fan-out (no intermediate progress,
		// s. concept) - it's atomic, either fully done or not, so it reports a
		// single +1 once. UpdatePartialJob carries that through to the Job's
		// own current as well, so there is nothing to report separately.
		if err := backend.UpdatePartialJob(partialJob.Id, 1); err != nil {
			return model.Error(fmt.Sprintf("updating partial job progress: %v", err))
		}

		return model.Success()
	}}, nil
}

// resolveParams implements the two input-mapping modes from the concept:
// implicit (Def.Parameters absent - Job.Inputs mapped onto the Workflow's
// declared params by field name) or explicit (Def.Parameters present -
// every param comes from there, templated, nothing auto-filled from
// Inputs).
func resolveParams(appCtx *app.AppContext, def *app.JobDefinition, wf *workflows.Workflow, job *model.Job) (map[string]any, error) {
	if len(def.Parameters) == 0 {
		return resolveImplicitParams(wf, job)
	}
	return resolveExplicitParams(appCtx, def, wf, job)
}

func resolveImplicitParams(wf *workflows.Workflow, job *model.Job) (map[string]any, error) {
	return applyParamDefaults(wf, job.Inputs)
}

// resolveExplicitParams resolves Def.Parameters as workflow-style
// ${...} template expressions against packages/parent - parent.outputs is
// the shared Job's own Outputs (s. PartialJob.PartOf), i.e. whatever an
// earlier step of the same Job already wrote; there is no separate
// "parent job" to look up, since all steps of a Job are PartialJobs of the
// very same Job.
func resolveExplicitParams(appCtx *app.AppContext, def *app.JobDefinition, wf *workflows.Workflow, job *model.Job) (map[string]any, error) {
	resolveVars := map[string]any{
		"packages": packageVars(appCtx.Settings.Packages),
		"parent":   map[string]any{"outputs": outputValues(job.Outputs)},
	}
	resolved, err := workflows.ResolveValue(map[string]any(def.Parameters), resolveVars)
	if err != nil {
		return nil, err
	}
	provided, _ := resolved.(map[string]any)
	return applyParamDefaults(wf, provided)
}

// applyParamDefaults fills in each declared param from provided if present,
// else its Default, erroring if a Required param ends up with neither -
// the same default/required rule as lib/workflows.ResolveParams, but
// without the string-to-int/bool coercion that only makes sense for
// CLI-provided string overrides: provided here is already properly typed
// (straight from JSON Inputs, or from template resolution).
func applyParamDefaults(wf *workflows.Workflow, provided map[string]any) (map[string]any, error) {
	result := make(map[string]any, len(wf.Params))
	for _, param := range wf.Params {
		if v, ok := provided[param.Name]; ok {
			result[param.Name] = v
			continue
		}
		if param.Default != nil {
			result[param.Name] = param.Default
			continue
		}
		if param.Required {
			return nil, fmt.Errorf("missing required parameter %q", param.Name)
		}
	}
	return result, nil
}

func writeOutputs(backend jobs.Backend, jobID string, leafVars map[string]any, def *app.JobDefinition) error {
	if len(def.Outputs) == 0 {
		return nil
	}
	resolved, err := workflows.ResolveValue(map[string]any(def.Outputs), leafVars)
	if err != nil {
		return err
	}
	outputs, _ := resolved.(map[string]any)
	for key, value := range outputs {
		if err := backend.SetOutput(jobID, key, model.OutputValue{Value: value}); err != nil {
			return err
		}
	}
	return nil
}

// outputValues strips OutputValue down to its .Value for ${parent...}
// template resolution - the wrapper type is an implementation detail of
// how Job.Outputs is stored, not something a template author should need
// to know about.
//
// Job.Outputs is an opaque map the backend round-trips through JSON, so an
// entry written as a model.OutputValue arrives as a generic map with a
// "value" key rather than as the struct - anything that doesn't carry the
// wrapper is passed through as-is.
func outputValues(outputs map[string]any) map[string]any {
	result := make(map[string]any, len(outputs))
	for k, v := range outputs {
		switch value := v.(type) {
		case model.OutputValue:
			result[k] = value.Value
		case map[string]any:
			if inner, ok := value["value"]; ok {
				result[k] = inner
				continue
			}
			result[k] = v
		default:
			result[k] = v
		}
	}
	return result
}
