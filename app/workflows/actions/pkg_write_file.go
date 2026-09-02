package actions

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/workflows"
)

// WriteFileAction implements "pkg:write_file": writes content to a path in
// a package's local mirror, for the small files a workflow produces itself
// rather than finds - a manifest naming what it just moved, a marker the
// next run looks for, a rendered config.
//
// The package must already have been pulled and, like pkg:mv_file, must be
// FS or S3: writing into a mirror that can never be synced back would be a
// file nobody ever sees. Nothing is synced back here either - a workflow
// adds a pkg:push step for that.
//
// An existing file is overwritten, so a re-run of the same workflow lands
// in the same place rather than piling up variants, and missing parent
// directories are created.
type WriteFileAction struct {
	AppCtx *app.AppContext
}

func (a *WriteFileAction) Type() string { return "pkg:write_file" }

func (a *WriteFileAction) Run(ctx *workflows.StepContext) (workflows.StepResult, error) {
	pkgId, ok := ctx.Params["pkg"].(string)
	if !ok || pkgId == "" {
		return workflows.StepResult{}, fmt.Errorf(`pkg:write_file: "pkg" parameter is required`)
	}
	relPath, ok := ctx.Params["path"].(string)
	if !ok || relPath == "" {
		return workflows.StepResult{}, fmt.Errorf(`pkg:write_file: "path" parameter is required`)
	}
	content, err := fileContent(ctx.Params)
	if err != nil {
		return workflows.StepResult{}, fmt.Errorf("pkg:write_file: %w", err)
	}

	p, err := a.AppCtx.Settings.GetPackage(pkgId)
	if err != nil {
		return workflows.StepResult{}, err
	}
	if !SupportsSyncBack(p.Type) {
		return workflows.StepResult{}, fmt.Errorf("pkg:write_file only supports FS/S3 packages, got %s(%s)", pkgId, p.Type)
	}
	if err := requireLocalMirror(p); err != nil {
		return workflows.StepResult{}, fmt.Errorf("pkg:write_file: %w", err)
	}

	path, err := mirrorPath(p, relPath)
	if err != nil {
		return workflows.StepResult{}, fmt.Errorf("pkg:write_file: path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return workflows.StepResult{}, fmt.Errorf("pkg:write_file: could not create the directory for %q: %w", relPath, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return workflows.StepResult{}, fmt.Errorf("pkg:write_file: could not write %q: %w", relPath, err)
	}

	ctx.Logger.Debug().Str("pkg", pkgId).Str("path", relPath).Int("bytes", len(content)).Msg("wrote file")

	return workflows.One(map[string]any{"path": path}), nil
}

// fileContent reads the "content" parameter. A string is taken as written,
// including an empty one - an empty file is a legitimate marker. A number
// or boolean is formatted, since a template expression resolving to one is
// no mistake; anything structured is refused, because guessing between
// JSON and YAML for it would be exactly that, a guess.
func fileContent(params map[string]any) (string, error) {
	raw, ok := params["content"]
	if !ok {
		return "", fmt.Errorf(`"content" parameter is required`)
	}

	switch value := raw.(type) {
	case string:
		return value, nil
	case int, int64, float64, bool:
		return fmt.Sprint(value), nil
	default:
		return "", fmt.Errorf("content: must be text, a number or a boolean, got %T", raw)
	}
}
