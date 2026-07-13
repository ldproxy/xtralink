package actions

import (
	"fmt"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/app/pkg"
	"github.com/ldproxy/xtralink/lib/workflows"
)

// PushAction implements "pkg:push": mirrors a package's local changes back
// to its own remote in place (SyncBack), the same mechanism pkg:mv_file
// already uses on its from/to packages - only FS/S3 support it. This is a
// deliberately different meaning from the CLI's "xtrasync pkg push" command,
// which builds and publishes a new OCI artifact rather than syncing a
// package back to its own remote.
type PushAction struct {
	AppCtx *app.AppContext
}

func (a *PushAction) Type() string { return "pkg:push" }

func (a *PushAction) Run(ctx *workflows.StepContext) (workflows.StepResult, error) {
	pkgId, ok := ctx.Params["pkg"].(string)
	if !ok || pkgId == "" {
		return workflows.StepResult{}, fmt.Errorf(`pkg:push: "pkg" parameter is required`)
	}

	p, err := a.AppCtx.Settings.GetPackage(pkgId)
	if err != nil {
		return workflows.StepResult{}, err
	}
	if !SupportsSyncBack(p.Type) {
		return workflows.StepResult{}, fmt.Errorf("pkg:push only supports FS/S3 packages, got %s(%s)", pkgId, p.Type)
	}

	driver, err := a.AppCtx.Drivers.SyncBackFor(p.Type)
	if err != nil {
		return workflows.StepResult{}, err
	}
	if err := driver.SyncBack(pkg.RemoteFor(*p)); err != nil {
		return workflows.StepResult{}, fmt.Errorf("pkg:push: could not sync %q back: %w", pkgId, err)
	}

	return workflows.Success(), nil
}
