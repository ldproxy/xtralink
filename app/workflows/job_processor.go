package workflows

import (
	"encoding/json"
	"fmt"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/jobs"
	"github.com/ldproxy/xtralink/lib/workflows"
)

// WorkflowJobProcessor makes a Job a thin wrapper around a single Workflow
// run: one instance handles exactly one JobStepDefinition (its JobType()
// is that step's id, s. app.JobStepDefinition), resolving the step's
// input-parameter mapping, running the referenced Workflow, and writing the
// step's output mapping into the shared Job's Outputs. A pipeline with
// several steps registers one WorkflowJobProcessor per step - not one
// processor handling all of them - so `job process <step-id>` can scale a
// single step's workers independently of the rest of its pipeline.
type WorkflowJobProcessor struct {
	AppCtx *app.AppContext
	StepId string
	Step   *app.JobStepDefinition
}

// NewWorkflowJobProcessor looks up stepId once at construction time (rather
// than on every Process call) - Settings never change during a run, so
// there is nothing to gain from re-resolving it each time.
func NewWorkflowJobProcessor(appCtx *app.AppContext, stepId string) (*WorkflowJobProcessor, error) {
	_, step, err := appCtx.Settings.GetJobStep(stepId)
	if err != nil {
		return nil, err
	}
	return &WorkflowJobProcessor{AppCtx: appCtx, StepId: stepId, Step: step}, nil
}

func (p *WorkflowJobProcessor) JobType() string { return p.StepId }
func (p *WorkflowJobProcessor) Priority() int   { return 1000 }

func (p *WorkflowJobProcessor) Process(partialJob *jobs.PartialJob, job *jobs.Job, backend jobs.Backend) jobs.JobResult {
	wf, err := p.AppCtx.Settings.GetWorkflow(p.Step.Workflow)
	if err != nil {
		return jobs.Error(err.Error())
	}

	params, err := p.resolveParams(wf, job)
	if err != nil {
		return jobs.Error(fmt.Sprintf("resolving parameters: %v", err))
	}

	registry := NewRegistry(p.AppCtx)
	if err := Validate(p.AppCtx, *wf, registry); err != nil {
		return jobs.Error(fmt.Sprintf("workflow %q is invalid: %v", wf.Id, err))
	}

	vars := map[string]any{
		"packages": packageVars(p.AppCtx.Settings.Packages),
		"params":   params,
	}
	leaves, err := workflows.RunWithResults(*wf, registry, vars)
	if err != nil {
		return jobs.Error(fmt.Sprintf("workflow %q failed: %v", wf.Id, err))
	}
	// A job-wrapped Workflow is expected to complete linearly - a step that
	// forks (pkg:find_each and the like) would produce more than one
	// result here, and there's no defined way to pick "the" one for the
	// outputs mapping below, so treat it as a configuration error rather
	// than silently guessing.
	if len(leaves) != 1 {
		return jobs.Error(fmt.Sprintf(
			"workflow %q produced %d parallel results, expected exactly 1 - job-wrapped workflows must not fork", wf.Id, len(leaves)))
	}

	if err := p.writeOutputs(backend, job.ID, leaves[0]); err != nil {
		return jobs.Error(fmt.Sprintf("writing outputs: %v", err))
	}

	// This step has no progressDetails/UpdateTargets fan-out (no
	// intermediate progress, s. concept) - it's atomic, either fully done
	// or not, so it reports its own +1 on both levels directly (mirrors
	// how lib/jobs' counterProcessor test double reports progress for a
	// job type with no fan-out descriptor either).
	if err := backend.UpdatePartialJob(partialJob.ID, 1); err != nil {
		return jobs.Error(fmt.Sprintf("updating partial job progress: %v", err))
	}
	if err := backend.UpdateJob(job.ID, 1, nil); err != nil {
		return jobs.Error(fmt.Sprintf("updating job progress: %v", err))
	}

	return jobs.Success()
}

// resolveParams implements the two input-mapping modes from the concept:
// implicit (Step.Parameters absent - Job.Inputs mapped onto the Workflow's
// declared params by field name) or explicit (Step.Parameters present -
// every param comes from there, templated, nothing auto-filled from
// Inputs).
func (p *WorkflowJobProcessor) resolveParams(wf *workflows.Workflow, job *jobs.Job) (map[string]any, error) {
	if len(p.Step.Parameters) == 0 {
		return p.resolveImplicitParams(wf, job)
	}
	return p.resolveExplicitParams(wf, job)
}

func (p *WorkflowJobProcessor) resolveImplicitParams(wf *workflows.Workflow, job *jobs.Job) (map[string]any, error) {
	var provided map[string]any
	if len(job.Inputs) > 0 {
		if err := json.Unmarshal(job.Inputs, &provided); err != nil {
			return nil, fmt.Errorf("invalid inputs: %w", err)
		}
	}
	return applyParamDefaults(wf, provided)
}

// resolveExplicitParams resolves Step.Parameters as workflow-style
// ${...} template expressions against packages/parent - parent.outputs is
// the shared Job's own Outputs (s. PartialJob.PartOf), i.e. whatever an
// earlier step of the same pipeline already wrote; there is no separate
// "parent job" to look up, since all steps of a pipeline are PartialJobs
// of the very same Job.
func (p *WorkflowJobProcessor) resolveExplicitParams(wf *workflows.Workflow, job *jobs.Job) (map[string]any, error) {
	resolveVars := map[string]any{
		"packages": packageVars(p.AppCtx.Settings.Packages),
		"parent":   map[string]any{"outputs": outputValues(job.Outputs)},
	}
	resolved, err := workflows.ResolveValue(map[string]any(p.Step.Parameters), resolveVars)
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

func (p *WorkflowJobProcessor) writeOutputs(backend jobs.Backend, jobID string, leafVars map[string]any) error {
	if len(p.Step.Outputs) == 0 {
		return nil
	}
	resolved, err := workflows.ResolveValue(map[string]any(p.Step.Outputs), leafVars)
	if err != nil {
		return err
	}
	outputs, _ := resolved.(map[string]any)
	for key, value := range outputs {
		if err := backend.SetOutput(jobID, key, jobs.OutputValue{Value: value}); err != nil {
			return err
		}
	}
	return nil
}

// outputValues strips OutputValue down to its .Value for ${parent...}
// template resolution - the wrapper type is an implementation detail of
// how Job.Outputs is stored, not something a template author should need
// to know about.
func outputValues(outputs map[string]jobs.OutputValue) map[string]any {
	result := make(map[string]any, len(outputs))
	for k, v := range outputs {
		result[k] = v.Value
	}
	return result
}
