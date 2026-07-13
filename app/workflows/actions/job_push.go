package actions

import (
	"encoding/json"
	"fmt"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/app/jobs"
	"github.com/ldproxy/xtralink/lib/workflows"
)

// JobPushAction implements "job:push": builds a JobSet from the Step's
// inputs list and pushes it via the existing app/jobs.Push - fire-and-
// forget, it never waits for the Job to finish.
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
	entity, _ := ctx.Params["entity"].(string)
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

	if _, err := jobs.Push(a.AppCtx, jobType, label, entity, priority, inputsJSON); err != nil {
		return workflows.StepResult{}, fmt.Errorf("job:push: %w", err)
	}

	return workflows.Success(), nil
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
