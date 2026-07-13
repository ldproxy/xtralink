package actions

import (
	"fmt"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/app/pkg"
	"github.com/ldproxy/xtralink/lib/workflows"
)

// PullAction implements "pkg:pull": explicitly pulls a package's remote into
// its local mirror and exposes the resulting local path as an output, for
// steps that need the path without also matching a file (pkg:find_any/
// pkg:find_each already pull implicitly as part of finding).
type PullAction struct {
	AppCtx *app.AppContext
}

func (a *PullAction) Type() string { return "pkg:pull" }

func (a *PullAction) Run(ctx *workflows.StepContext) (workflows.StepResult, error) {
	pkgId, ok := ctx.Params["pkg"].(string)
	if !ok || pkgId == "" {
		return workflows.StepResult{}, fmt.Errorf(`pkg:pull: "pkg" parameter is required`)
	}

	if err := pkg.Pull(a.AppCtx, pkgId); err != nil {
		return workflows.StepResult{}, fmt.Errorf("pkg:pull: %w", err)
	}

	p, err := a.AppCtx.Settings.GetPackage(pkgId)
	if err != nil {
		return workflows.StepResult{}, err
	}

	return workflows.One(map[string]any{"path": p.ResolvedLocalPath}), nil
}
