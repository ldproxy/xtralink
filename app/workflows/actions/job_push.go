package actions

import (
	"fmt"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/app/jobs"
	"github.com/ldproxy/xtralink/lib/workflows"
	"github.com/ldproxy/xtralink/model"
)

// JobPushAction implements "job:push": builds a Job from the workflow
// step's parameters and pushes it via app/jobs.Push - fire-and-forget, it
// never waits for the Job to finish.
//
// Each parameter maps onto one field of model.JobConfiguration, so the
// step reads the same way the pushed Job looks: kind, label, description,
// priority, inputs, context, ttlSeconds, setup, cleanup, followUps.
//
// `partials:` names the JobDefinitions the Job's PartialJobs run, which is
// what binds each of them to a workflow; job:push never declares new ones
// itself. The pushed Job then gets one PartialJob per listed kind - an
// ad-hoc pipeline under the Job's own kind, without that kind needing its
// own JobDefinition entry. `setup:`/`cleanup:` are plain booleans instead,
// since their kinds follow from the Job's own by convention
// (s. lib/jobs.SetupKind).
type JobPushAction struct {
	AppCtx *app.AppContext
}

func (a *JobPushAction) Type() string { return "job:push" }

func (a *JobPushAction) Run(ctx *workflows.StepContext) (workflows.StepResult, error) {
	req, err := a.buildRequest(ctx.Params)
	if err != nil {
		return workflows.StepResult{}, fmt.Errorf("job:push: %w", err)
	}

	job, err := jobs.Push(a.AppCtx, *req)
	if err != nil {
		return workflows.StepResult{}, fmt.Errorf("job:push: %w", err)
	}

	// external names the partial kinds this configuration has no job
	// definition for: another service is expected to process them, and if
	// none does the Job waits until its TTL. It is also where a mistyped
	// kind shows up.
	ctx.Logger.Debug().Str("job", job.Id).Str("kind", job.Kind).
		Strs("partials", req.Partials).Strs("external", a.externalKinds(req.Partials)).
		Bool("sequential", req.Sequential).Msg("pushed job")

	return workflows.Success(), nil
}

// externalKinds returns the partial kinds with no job definition here, so
// the pushed-job log line says which parts depend on another service.
func (a *JobPushAction) externalKinds(kinds []string) []string {
	var external []string
	for _, kind := range kinds {
		if _, err := a.AppCtx.Settings.GetJobDefinition(kind); err != nil {
			external = append(external, kind)
		}
	}
	return external
}

func (a *JobPushAction) buildRequest(params map[string]any) (*jobs.PushRequest, error) {
	cfg, err := jobConfiguration(params)
	if err != nil {
		return nil, err
	}

	partials, err := resolvePartials(params)
	if err != nil {
		return nil, err
	}
	sequential, err := boolParam(params, "sequential")
	if err != nil {
		return nil, err
	}

	return &jobs.PushRequest{
		JobConfiguration: *cfg,
		Partials:         partials,
		Sequential:       sequential,
	}, nil
}

// jobConfiguration reads the parameters that are the Job itself, in the
// shape the generated model already defines - so there is one description
// of a Job to push, not a second one owned by this action.
func jobConfiguration(params map[string]any) (*model.JobConfiguration, error) {
	kind, _ := params["kind"].(string)
	if kind == "" {
		return nil, fmt.Errorf(`"kind" parameter is required`)
	}

	label, _ := params["label"].(string)
	description, _ := params["description"].(string)

	inputs, err := mapParam(params, "inputs")
	if err != nil {
		return nil, err
	}
	context, err := mapParam(params, "context")
	if err != nil {
		return nil, err
	}
	ttlSeconds, err := optionalIntParam(params, "ttlSeconds")
	if err != nil {
		return nil, err
	}
	setup, err := boolParam(params, "setup")
	if err != nil {
		return nil, err
	}
	cleanup, err := boolParam(params, "cleanup")
	if err != nil {
		return nil, err
	}
	followUps, err := resolveFollowUps(params)
	if err != nil {
		return nil, err
	}

	return &model.JobConfiguration{
		Kind:        kind,
		Label:       label,
		Description: description,
		Priority:    intParam(params, "priority", 1000),
		Inputs:      inputs,
		Context:     context,
		TtlSeconds:  ttlSeconds,
		Setup:       setup,
		Cleanup:     cleanup,
		FollowUps:   followUps,
	}, nil
}

// resolvePartials reads `partials: [{kind: ...}, ...]` as the kinds the
// pushed Job's PartialJobs run as. An absent `partials:` is fine: the Job
// then has whatever PartialJobs its own kind implies.
//
// A kind is not checked against this configuration's job definitions. One
// of them binds a kind to a workflow xtralink runs itself, but a Job can
// just as well be composed partly of parts another service processes, and
// requiring a definition would make those impossible to name.
func resolvePartials(params map[string]any) ([]string, error) {
	raw, ok := params["partials"]
	if !ok {
		return nil, nil
	}

	entries, _ := raw.([]any)
	if len(entries) == 0 {
		return nil, fmt.Errorf("partials: at least one entry is required")
	}

	kinds := make([]string, 0, len(entries))
	for i, item := range entries {
		entry, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("partials[%d]: invalid entry", i)
		}
		kind, _ := entry["kind"].(string)
		if kind == "" {
			return nil, fmt.Errorf("partials[%d]: \"kind\" is required", i)
		}
		kinds = append(kinds, kind)
	}
	return kinds, nil
}

// resolveFollowUps reads `followUps: [{kind, label, inputs}, ...]` as the
// nested JobConfigurations the model carries, so a follow-up accepts the
// same parameters the step itself does - recursively, since a follow-up may
// declare follow-ups of its own.
//
// A follow-up's kind is not resolved against the JobDefinitions: the
// backend pushes follow-ups as plain Jobs, so any kind is legitimate there,
// the same freedom a direct `job push <kind>` has.
func resolveFollowUps(params map[string]any) ([]model.JobConfiguration, error) {
	raw, ok := params["followUps"]
	if !ok {
		return nil, nil
	}

	entries, _ := raw.([]any)
	if len(entries) == 0 {
		return nil, fmt.Errorf("followUps: at least one entry is required")
	}

	followUps := make([]model.JobConfiguration, 0, len(entries))
	for i, item := range entries {
		entry, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("followUps[%d]: invalid entry", i)
		}
		cfg, err := jobConfiguration(entry)
		if err != nil {
			return nil, fmt.Errorf("followUps[%d]: %w", i, err)
		}
		followUps = append(followUps, *cfg)
	}
	return followUps, nil
}

// boolParam reads an optional boolean parameter, refusing anything that
// only looks like one - a "true" typed as a string is a mistake worth
// reporting, not a value to coerce.
func boolParam(params map[string]any, key string) (bool, error) {
	raw, ok := params[key]
	if !ok || raw == nil {
		return false, nil
	}

	value, ok := raw.(bool)
	if !ok {
		return false, fmt.Errorf("%s: must be true or false, got %T", key, raw)
	}
	return value, nil
}

// mapParam reads a parameter that goes onto the Job as an opaque
// map[string]any (inputs, context) and passes it straight through - the
// model carries exactly what the workflow wrote, no name/value wrapping in
// between.
func mapParam(params map[string]any, key string) (map[string]any, error) {
	raw, ok := params[key]
	if !ok || raw == nil {
		return nil, nil
	}

	value, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s: must be a map of names to values, got %T", key, raw)
	}
	if len(value) == 0 {
		return nil, nil
	}
	return value, nil
}

// intParam reads an integer parameter, tolerating the float64 a JSON- or
// YAML-decoded number can arrive as.
func intParam(params map[string]any, key string, fallback int) int {
	switch v := params[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return fallback
}

// optionalIntParam is intParam for a field the model keeps as a pointer,
// where "not set" and "set to zero" are different things.
func optionalIntParam(params map[string]any, key string) (*int, error) {
	raw, ok := params[key]
	if !ok || raw == nil {
		return nil, nil
	}
	switch raw.(type) {
	case int, int64, float64:
		value := intParam(params, key, 0)
		return &value, nil
	}
	return nil, fmt.Errorf("%s: must be a number, got %T", key, raw)
}
