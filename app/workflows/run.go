// Package workflows wires the generic lib/workflows engine to *app.AppContext:
// it builds the Action registry, resolves a workflow from Settings, runs the
// action-aware validation that app/settings.go could not (would require an
// import cycle, s. Validate's doc comment below), and executes it.
package workflows

import (
	"context"
	"fmt"
	"strings"

	"github.com/ldproxy/xtrasync/app"
	"github.com/ldproxy/xtrasync/app/workflows/actions"
	"github.com/ldproxy/xtrasync/lib/workflows"
)

// NewRegistry builds the Action registry for the four supported Actions,
// wired to appCtx.
func NewRegistry(appCtx *app.AppContext) *workflows.Registry {
	registry := workflows.NewRegistry()
	registry.Register(&actions.FindAnyAction{AppCtx: appCtx})
	registry.Register(&actions.FindEachAction{AppCtx: appCtx})
	registry.Register(&actions.MvFileAction{AppCtx: appCtx})
	registry.Register(&actions.JobPushAction{AppCtx: appCtx})
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

	params, err := workflows.ResolveParams(*wf, overrides)
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
		"packages": packageVars(appCtx.Settings.Packages),
		"params":   params,
	}
	return workflows.Run(*wf, registry, vars)
}

// ParseOverrides turns "name=value" strings, as collected from repeated
// --input flags (s. cli/flow.go), into a map for ResolveParams.
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
// existing package - pkg:mv_file's from/to additionally must be FS/S3.
// Template-valued params (containing "${") are skipped - their actual value
// is only known once earlier Steps have run.
func Validate(appCtx *app.AppContext, wf workflows.Workflow, registry *workflows.Registry) error {
	for i, step := range wf.Steps {
		if _, err := registry.Lookup(step.Action); err != nil {
			return fmt.Errorf("step %d (%s): %w", i, step.EffectiveId(i), err)
		}

		switch step.Action {
		case "pkg:find_any", "pkg:find_each":
			if err := validatePackageRef(appCtx, step.Params, "pkg"); err != nil {
				return fmt.Errorf("step %d (%s): %w", i, step.EffectiveId(i), err)
			}
		case "pkg:mv_file":
			if err := validateMvFilePackageRef(appCtx, step.Params, "from"); err != nil {
				return fmt.Errorf("step %d (%s): %w", i, step.EffectiveId(i), err)
			}
			if err := validateMvFilePackageRef(appCtx, step.Params, "to"); err != nil {
				return fmt.Errorf("step %d (%s): %w", i, step.EffectiveId(i), err)
			}
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

func validateMvFilePackageRef(appCtx *app.AppContext, params map[string]any, key string) error {
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
	if !actions.SupportsMvFile(p.Type) {
		return fmt.Errorf("%q references package %q of type %q - pkg:mv_file only supports FS/S3", key, v, p.Type)
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
