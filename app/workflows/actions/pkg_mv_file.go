package actions

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/ldproxy/xtrasync/app"
	"github.com/ldproxy/xtrasync/app/pkg"
	"github.com/ldproxy/xtrasync/lib/workflows"
)

// SupportsMvFile reports whether pkgType may be used as pkg:mv_file's
// from/to package - only FS and S3 have a SyncBack implementation; GIT and
// OCI are a deliberate non-goal, not a gap (s. lib/drivers/driver.go's
// SyncBackDriver doc comment). Exported so app/workflows.Validate can
// reject a GIT/OCI reference before a workflow ever runs, not just when
// mv_file itself hits it.
func SupportsMvFile(pkgType string) bool {
	switch strings.ToUpper(strings.TrimSpace(pkgType)) {
	case "FS", "S3":
		return true
	default:
		return false
	}
}

// MvFileAction implements "pkg:mv_file": moves a single file from one
// package's local mirror to another's, deleting the source, then syncs both
// packages back to their own remote independently.
type MvFileAction struct {
	AppCtx *app.AppContext
}

func (a *MvFileAction) Type() string { return "pkg:mv_file" }

func (a *MvFileAction) Run(ctx *workflows.StepContext) (workflows.StepResult, error) {
	fromId, ok := ctx.Params["from"].(string)
	if !ok || fromId == "" {
		return workflows.StepResult{}, fmt.Errorf(`pkg:mv_file: "from" parameter is required`)
	}
	toId, ok := ctx.Params["to"].(string)
	if !ok || toId == "" {
		return workflows.StepResult{}, fmt.Errorf(`pkg:mv_file: "to" parameter is required`)
	}
	relPath, ok := ctx.Params["path"].(string)
	if !ok || relPath == "" {
		return workflows.StepResult{}, fmt.Errorf(`pkg:mv_file: "path" parameter is required`)
	}

	fromPkg, err := a.AppCtx.Settings.GetPackage(fromId)
	if err != nil {
		return workflows.StepResult{}, err
	}
	toPkg, err := a.AppCtx.Settings.GetPackage(toId)
	if err != nil {
		return workflows.StepResult{}, err
	}
	if !SupportsMvFile(fromPkg.Type) || !SupportsMvFile(toPkg.Type) {
		return workflows.StepResult{}, fmt.Errorf("pkg:mv_file only supports FS/S3 packages, got from=%s(%s) to=%s(%s)",
			fromId, fromPkg.Type, toId, toPkg.Type)
	}

	if err := pkg.Pull(a.AppCtx, fromId); err != nil {
		return workflows.StepResult{}, fmt.Errorf("could not pull %q: %w", fromId, err)
	}
	if err := pkg.Pull(a.AppCtx, toId); err != nil {
		return workflows.StepResult{}, fmt.Errorf("could not pull %q: %w", toId, err)
	}

	srcPath := filepath.Join(fromPkg.ResolvedLocalPath, filepath.FromSlash(relPath))
	dstPath := filepath.Join(toPkg.ResolvedLocalPath, filepath.FromSlash(relPath))
	if err := moveFile(srcPath, dstPath); err != nil {
		return workflows.StepResult{}, fmt.Errorf("could not move %q from %q to %q: %w", relPath, fromId, toId, err)
	}

	fromDriver, err := a.AppCtx.Drivers.SyncBackFor(fromPkg.Type)
	if err != nil {
		return workflows.StepResult{}, err
	}
	if err := fromDriver.SyncBack(pkg.RemoteFor(*fromPkg)); err != nil {
		return workflows.StepResult{}, fmt.Errorf("could not sync %q back after removing %q: %w", fromId, relPath, err)
	}

	toDriver, err := a.AppCtx.Drivers.SyncBackFor(toPkg.Type)
	if err != nil {
		return workflows.StepResult{}, err
	}
	if err := toDriver.SyncBack(pkg.RemoteFor(*toPkg)); err != nil {
		return workflows.StepResult{}, fmt.Errorf("could not sync %q back after adding %q: %w", toId, relPath, err)
	}

	return workflows.Success(), nil
}

// moveFile renames src to dst, falling back to copy+remove if they're on
// different filesystems (os.Rename's "invalid cross-device link").
func moveFile(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	if err := copyFile(src, dst); err != nil {
		return err
	}
	return os.Remove(src)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}
