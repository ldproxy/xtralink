// Package workflows wires the generic lib/workflows engine to *app.AppContext:
// it builds the Action registry, resolves a workflow from Settings, runs the
// action-aware validation that app/settings.go could not (would require an
// import cycle, s. Validate's doc comment below), and executes it.
package workflows

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/app/workflows/actions"
	"github.com/ldproxy/xtralink/lib/workflows"
)

// NewRegistry builds the Action registry for all supported Actions, wired
// to appCtx.
func NewRegistry(appCtx *app.AppContext) *workflows.Registry {
	registry := workflows.NewRegistry()
	registry.Register(&actions.FindAnyAction{AppCtx: appCtx})
	registry.Register(&actions.FindEachAction{AppCtx: appCtx})
	registry.Register(&actions.MvFileAction{AppCtx: appCtx})
	registry.Register(&actions.JobPushAction{AppCtx: appCtx})
	registry.Register(&actions.PullAction{AppCtx: appCtx})
	registry.Register(&actions.PushAction{AppCtx: appCtx})
	registry.Register(&actions.WriteFileAction{AppCtx: appCtx})
	registry.Register(&actions.CmdExecAction{})
	registry.Register(&actions.UUIDGenAction{})
	return registry
}

// Run resolves workflowId from appCtx.Settings, resolves params (overrides
// merged with declared defaults - a missing required param aborts here,
// before anything else happens, not after the lock is already claimed),
// validates the workflow, claims the per-workflow-ID lock (two different
// workflow IDs may run at the same time, the same one may not run twice
// concurrently), and executes it - the single entry point cli/flow.go
// calls.
func Run(appCtx *app.AppContext, workflowId string, overrides map[string]string) error {
	wf, err := appCtx.Settings.GetWorkflow(workflowId)
	if err != nil {
		return err
	}

	params, err := workflows.ResolveParameters(*wf, overrides)
	if err != nil {
		return fmt.Errorf("workflow %q: %w", workflowId, err)
	}

	registry := NewRegistry(appCtx)
	if err := Validate(appCtx, *wf, registry); err != nil {
		return fmt.Errorf("workflow %q is invalid: %w", workflowId, err)
	}

	release, ok, err := appCtx.Locks.Acquire(context.Background(), workflowId)
	if err != nil {
		return fmt.Errorf("could not acquire lock for workflow %q: %w", workflowId, err)
	}
	if !ok {
		return fmt.Errorf("workflow %q is already running", workflowId)
	}
	defer release()

	vars := map[string]any{
		"packages":   packageVars(appCtx.Settings.Packages),
		"parameters": params,
	}

	logger := appCtx.Logger.With().Str("workflow", workflowId).Logger()
	started := time.Now()
	if err := workflows.Run(*wf, registry, vars, logger); err != nil {
		return err
	}
	logger.Info().Int("steps", len(wf.Steps)).Dur("duration", time.Since(started)).Msg("workflow completed")

	return nil
}

// ParseOverrides turns "name=value" strings, as collected from repeated
// --input flags (s. cli/flow.go), into a map for ResolveParameters.
func ParseOverrides(raw []string) (map[string]string, error) {
	overrides := make(map[string]string, len(raw))
	for _, entry := range raw {
		name, value, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("invalid --input %q, expected name=value", entry)
		}
		overrides[name] = value
	}
	return overrides, nil
}

// Validate runs the action-aware checks app/settings.go's load-time
// validation could not perform (it would need the Action registry, which
// in turn needs *app.AppContext - an import cycle from app/settings.go):
// every Step's action must be registered, and pkg/from/to must reference an
// existing package - pkg:mv_file's from/to and pkg:push's pkg additionally
// must be FS/S3. Template-valued params (containing "${") are skipped -
// their actual value is only known once earlier Steps have run.
func Validate(appCtx *app.AppContext, wf workflows.Workflow, registry *workflows.Registry) error {
	if err := validateSteps(appCtx, wf.Steps, registry, "step"); err != nil {
		return err
	}
	if wf.Handlers == nil {
		return nil
	}

	// Handler steps are checked the same way and by name, so a typo in one
	// is caught before the run rather than only when something fails and
	// the handler is finally reached (s. workflows.Handlers).
	for _, handler := range []struct {
		label string
		steps []workflows.Step
	}{
		{"handlers.failure", wf.Handlers.Failure},
		{"handlers.success", wf.Handlers.Success},
		{"handlers.always", wf.Handlers.Always},
	} {
		if err := validateSteps(appCtx, handler.steps, registry, handler.label); err != nil {
			return err
		}
	}
	return nil
}

func validateSteps(appCtx *app.AppContext, steps []workflows.Step, registry *workflows.Registry, label string) error {
	for i, step := range steps {
		where := fmt.Sprintf("%s %d (%s)", label, i, step.EffectiveId(i))

		if _, err := registry.Lookup(step.Action); err != nil {
			return fmt.Errorf("%s: %w", where, err)
		}

		switch step.Action {
		case "pkg:find_any", "pkg:find_each", "pkg:pull":
			if err := validatePackageRef(appCtx, step.Params, "pkg"); err != nil {
				return fmt.Errorf("%s: %w", where, err)
			}
		case "pkg:mv_file":
			if err := validateSyncBackPackageRef(appCtx, step.Params, "from"); err != nil {
				return fmt.Errorf("%s: %w", where, err)
			}
			if err := validateSyncBackPackageRef(appCtx, step.Params, "to"); err != nil {
				return fmt.Errorf("%s: %w", where, err)
			}
		case "pkg:push", "pkg:write_file":
			if err := validateSyncBackPackageRef(appCtx, step.Params, "pkg"); err != nil {
				return fmt.Errorf("%s: %w", where, err)
			}
		case "job:push":
			if err := validateJobPush(appCtx, step.Params); err != nil {
				return fmt.Errorf("%s: %w", where, err)
			}
		}
	}
	return nil
}

// validateJobPush checks a job:push Step ahead of the run: `partials:`
// must reference kinds that already exist under jobs: (the same check
// validateJobDefinitions performs), and every other parameter must be of
// the type the Job model expects. It is optional - without partials,
// job:push falls back to a bare Job, unchanged from before.
//
// `followUps:` kinds are deliberately not checked against the
// JobDefinitions: the backend pushes follow-ups as plain Jobs with no
// PartialJobs of their own, so any kind is legitimate there, exactly as for
// a direct `job push <kind>`.
func validateJobPush(appCtx *app.AppContext, params map[string]any) error {
	if err := validateJobPushPartials(appCtx, params); err != nil {
		return err
	}
	return validateJobPushShapes(params)
}

// validateJobPushShapes rejects a parameter of the wrong kind here rather
// than at the step itself, so a job:push at the end of a long workflow
// fails before any of it runs. A whole-value template is skipped: its type
// is only known once earlier steps have.
func validateJobPushShapes(params map[string]any) error {
	for _, key := range []string{"inputs", "context"} {
		value, ok := params[key]
		if !ok || isTemplate(value) {
			continue
		}
		if _, ok := value.(map[string]any); !ok {
			return fmt.Errorf("%s: must be a map of names to values, got %T", key, value)
		}
	}

	if value, ok := params["ttlSeconds"]; ok && !isTemplate(value) {
		switch value.(type) {
		case int, int64, float64:
		default:
			return fmt.Errorf("ttlSeconds: must be a number, got %T", value)
		}
	}

	for _, key := range []string{"sequential", "setup", "cleanup"} {
		value, ok := params[key]
		if !ok || isTemplate(value) {
			continue
		}
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("%s: must be true or false, got %T", key, value)
		}
	}

	return nil
}

func isTemplate(value any) bool {
	s, ok := value.(string)
	return ok && strings.Contains(s, "${")
}

func validateJobPushPartials(appCtx *app.AppContext, params map[string]any) error {
	raw, ok := params["partials"]
	if !ok {
		return nil
	}
	entries, _ := raw.([]any)
	if len(entries) == 0 {
		return fmt.Errorf("partials: at least one entry is required")
	}
	for i, item := range entries {
		entry, ok := item.(map[string]any)
		if !ok {
			return fmt.Errorf("partials[%d]: invalid entry", i)
		}
		kind, _ := entry["kind"].(string)
		if kind == "" {
			return fmt.Errorf("partials[%d]: \"kind\" is required", i)
		}
		if strings.Contains(kind, "${") {
			continue // only known once earlier steps have run
		}
		if _, err := appCtx.Settings.GetJobDefinition(kind); err != nil {
			return fmt.Errorf("partials[%d]: %w", i, err)
		}
	}
	return nil
}

func validatePackageRef(appCtx *app.AppContext, params map[string]any, key string) error {
	v, ok := params[key].(string)
	if !ok || v == "" {
		return fmt.Errorf("%q parameter is required", key)
	}
	if strings.Contains(v, "${") {
		return nil // only known once earlier steps have run
	}
	if !appCtx.Settings.HasPackage(v) {
		return fmt.Errorf("%q references unknown package %q", key, v)
	}
	return nil
}

func validateSyncBackPackageRef(appCtx *app.AppContext, params map[string]any, key string) error {
	if err := validatePackageRef(appCtx, params, key); err != nil {
		return err
	}
	v, _ := params[key].(string)
	if v == "" || strings.Contains(v, "${") {
		return nil
	}
	p, err := appCtx.Settings.GetPackage(v)
	if err != nil {
		return err
	}
	if !actions.SupportsSyncBack(p.Type) {
		return fmt.Errorf("%q references package %q of type %q - only FS/S3 packages support sync-back", key, v, p.Type)
	}
	return nil
}

func packageVars(pkgs []app.Package) map[string]any {
	out := make(map[string]any, len(pkgs))
	for _, p := range pkgs {
		out[p.Id] = map[string]any{
			"id":        p.Id,
			"type":      p.Type,
			"url":       p.URL,
			"tag":       p.Tag,
			"path":      p.Path,
			"localPath": p.LocalPath,
		}
	}
	return out
}
