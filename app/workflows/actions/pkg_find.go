package actions

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/workflows"
)

// findMatches glob-matches pattern against pkgId's local mirror as it
// currently stands, returning matches as package-root-relative, slash-form
// paths, sorted alphabetically for a reproducible order.
//
// It does not pull: how fresh the mirror is, is decided by where the
// workflow puts its pkg:pull step (s. requireLocalMirror).
func findMatches(appCtx *app.AppContext, pkgId, pattern string) ([]string, error) {
	p, err := appCtx.Settings.GetPackage(pkgId)
	if err != nil {
		return nil, err
	}
	if err := requireLocalMirror(p); err != nil {
		return nil, err
	}

	matches, err := filepath.Glob(filepath.Join(p.ResolvedLocalPath, filepath.FromSlash(pattern)))
	if err != nil {
		return nil, fmt.Errorf("invalid glob pattern %q: %w", pattern, err)
	}

	rels := make([]string, 0, len(matches))
	for _, m := range matches {
		info, err := os.Stat(m)
		if err != nil {
			return nil, err
		}
		if info.IsDir() {
			continue
		}
		rel, err := filepath.Rel(p.ResolvedLocalPath, m)
		if err != nil {
			return nil, err
		}
		rels = append(rels, filepath.ToSlash(rel))
	}
	sort.Strings(rels)

	return rels, nil
}

// findOutput describes one match: its package-root-relative path, and the
// directory that path sits in.
//
// dir is empty for a file at the package root rather than path.Dir's ".",
// which a template would only have to special-case away - and empty is
// also what a workflow wants there, since joining it back on gives the
// root itself.
func findOutput(relPath string) map[string]any {
	dir := path.Dir(relPath)
	if dir == "." || dir == "/" {
		dir = ""
	}
	return map[string]any{"path": relPath, "dir": dir}
}

func findParams(params map[string]any) (pkgId, pattern string, err error) {
	pkgId, ok := params["pkg"].(string)
	if !ok || pkgId == "" {
		return "", "", fmt.Errorf(`"pkg" parameter is required`)
	}
	pattern, ok = params["path"].(string)
	if !ok || pattern == "" {
		return "", "", fmt.Errorf(`"path" parameter is required`)
	}
	return pkgId, pattern, nil
}

// FindAnyAction implements "pkg:find_any": exactly one output set if
// anything matches (the alphabetically first match; further matches are
// silently ignored - never a fan-out), zero if nothing does. The package
// must already have been pulled.
//
// Outputs are ${outputs.<step>.path}, the match relative to the package
// root, and ${outputs.<step>.dir}, the directory it sits in.
type FindAnyAction struct {
	AppCtx *app.AppContext
}

func (a *FindAnyAction) Type() string { return "pkg:find_any" }

func (a *FindAnyAction) Run(ctx *workflows.StepContext) (workflows.StepResult, error) {
	pkgId, pattern, err := findParams(ctx.Params)
	if err != nil {
		return workflows.StepResult{}, err
	}

	matches, err := findMatches(a.AppCtx, pkgId, pattern)
	if err != nil {
		return workflows.StepResult{}, err
	}
	if len(matches) == 0 {
		return workflows.Halt(), nil
	}
	return workflows.One(findOutput(matches[0])), nil
}

// FindEachAction implements "pkg:find_each": one output set per match,
// fanning the remaining Steps out once per match. The package must already
// have been pulled.
//
// Each branch gets ${outputs.<step>.path}, its own match relative to the
// package root, and ${outputs.<step>.dir}, the directory that match sits
// in.
type FindEachAction struct {
	AppCtx *app.AppContext
}

func (a *FindEachAction) Type() string { return "pkg:find_each" }

func (a *FindEachAction) Run(ctx *workflows.StepContext) (workflows.StepResult, error) {
	pkgId, pattern, err := findParams(ctx.Params)
	if err != nil {
		return workflows.StepResult{}, err
	}

	matches, err := findMatches(a.AppCtx, pkgId, pattern)
	if err != nil {
		return workflows.StepResult{}, err
	}

	outputs := make([]map[string]any, len(matches))
	for i, m := range matches {
		outputs[i] = findOutput(m)
	}
	return workflows.Many(outputs), nil
}
