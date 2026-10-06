package actions

import (
	"fmt"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/workflows"
)

// CpFileAction implements "pkg:cp_file": copies a single file from one
// package's local mirror to another's, leaving the source in place. It
// takes the same parameters as pkg:mv_file (from, to, path, targetPath),
// and like it changes local mirrors only - a pkg:push step for the target
// syncs the copy back.
//
// Since the source is only read, `from` may be any pulled package, GIT and
// OCI included; only `to` has to be FS/S3. An existing target file is
// overwritten, so a re-run lands in the same place.
type CpFileAction struct {
	AppCtx *app.AppContext
}

func (a *CpFileAction) Type() string { return "pkg:cp_file" }

func (a *CpFileAction) Run(ctx *workflows.StepContext) (workflows.StepResult, error) {
	t, err := resolveFileTransfer(a.Type(), a.AppCtx, ctx.Params, false)
	if err != nil {
		return workflows.StepResult{}, err
	}

	// copyFile truncates its destination before reading the source, so
	// copying a file onto itself would empty it.
	if t.srcPath != t.dstPath {
		if err := copyFile(t.srcPath, t.dstPath); err != nil {
			return workflows.StepResult{}, fmt.Errorf("could not copy %q from %q to %q as %q: %w",
				t.sourceRelPath, t.fromId, t.toId, t.targetRelPath, err)
		}
	}

	ctx.Logger.Debug().Str("from", t.fromId).Str("to", t.toId).
		Str("path", t.sourceRelPath).Str("target_path", t.targetRelPath).Msg("copied file between mirrors")

	return workflows.Success(), nil
}
