package actions

import (
	"encoding/json"
	"fmt"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/app/jobs"
	"github.com/ldproxy/xtralink/lib/workflows"
)

// JobPushAction implements "job:push": builds a Job from the Step's inputs
// list and pushes it via the existing app/jobs.Push - fire-and-forget, it
// never waits for the Job to finish. If `partials:` is given, the pushed
// Job gets one PartialJob per listed type instead (s. resolvePartialSteps)
// - an ad-hoc, multi-step pipeline under the Step's own `type`, without
// that type needing its own JobDefinition entry.
type JobPushAction struct {
	AppCtx *app.AppContext
}

func (a *JobPushAction) Type() string { return "job:push" }

func (a *JobPushAction) Run(ctx *workflows.StepContext) (workflows.StepResult, error) {
	jobType, ok := ctx.Params["type"].(string)
	if !ok || jobType == "" {
		return workflows.StepResult{}, fmt.Errorf(`job:push: "type" parameter is required`)
	}
	label, _ := ctx.Params["label"].(string)
	priority := 1000
	switch v := ctx.Params["priority"].(type) {
	case int:
		priority = v
	case float64:
		priority = int(v)
	}

	inputsJSON, err := buildInputsJSON(ctx.Params["inputs"])
	if err != nil {
		return workflows.StepResult{}, fmt.Errorf("job:push: %w", err)
	}

	if raw, ok := ctx.Params["partials"]; ok {
		steps, err := resolvePartialSteps(a.AppCtx, raw)
		if err != nil {
			return workflows.StepResult{}, fmt.Errorf("job:push: %w", err)
		}
		parallel := true
		if v, ok := ctx.Params["parallel"].(bool); ok {
			parallel = v
		}
		if _, err := jobs.PushPipeline(a.AppCtx, jobType, label, priority, inputsJSON, steps, parallel); err != nil {
			return workflows.StepResult{}, fmt.Errorf("job:push: %w", err)
		}
		return workflows.Success(), nil
	}

	if _, err := jobs.Push(a.AppCtx, jobType, label, priority, inputsJSON); err != nil {
		return workflows.StepResult{}, fmt.Errorf("job:push: %w", err)
	}

	return workflows.Success(), nil
}

// resolvePartialSteps turns `partials: [{type: ...}, ...]` into the
// JobDefinitions those types already reference - job:push does not declare
// new ones itself, it only reuses existing entries (their Workflow
// binding, Parameters/Outputs mapping), the same way job process <id>
// already resolves them.
func resolvePartialSteps(appCtx *app.AppContext, raw any) ([]app.JobDefinition, error) {
	entries, _ := raw.([]any)
	if len(entries) == 0 {
		return nil, fmt.Errorf("partials: at least one entry is required")
	}

	defs := make([]app.JobDefinition, 0, len(entries))
	for i, item := range entries {
		entry, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("partials[%d]: invalid entry", i)
		}
		typ, _ := entry["type"].(string)
		if typ == "" {
			return nil, fmt.Errorf("partials[%d]: \"type\" is required", i)
		}
		def, err := appCtx.Settings.GetJobDefinition(typ)
		if err != nil {
			return nil, fmt.Errorf("partials[%d]: %w", i, err)
		}
		defs = append(defs, *def)
	}
	return defs, nil
}

// buildInputsJSON turns the Step's `inputs: [{name, value}, ...]` list into
// the flat JSON object app/jobs.Push expects as its inputsRaw string.
func buildInputsJSON(raw any) (string, error) {
	entries, _ := raw.([]any)
	inputs := make(map[string]any, len(entries))

	for _, item := range entries {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name, _ := entry["name"].(string)
		if name == "" {
			continue
		}
		inputs[name] = entry["value"]
	}

	if len(inputs) == 0 {
		return "", nil
	}

	encoded, err := json.Marshal(inputs)
	if err != nil {
		return "", fmt.Errorf("could not encode inputs: %w", err)
	}
	return string(encoded), nil
}
