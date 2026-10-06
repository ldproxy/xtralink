package actions

import (
	"fmt"

	"github.com/ldproxy/xtralink/app"
)

// fileTransfer is the part pkg:mv_file and pkg:cp_file share: a file at
// `path:` in the `from` package's local mirror, and where it lands in the
// `to` package's - at the same path, unless `targetPath:` says otherwise.
type fileTransfer struct {
	fromId, toId                 string
	sourceRelPath, targetRelPath string
	srcPath, dstPath             string
}

// resolveFileTransfer reads and checks the parameters both actions take.
// The target package is written to, so it always has to support sync-back;
// the source only has to as well if the action changes it, which a copy
// does not.
func resolveFileTransfer(action string, appCtx *app.AppContext, params map[string]any, modifiesSource bool) (fileTransfer, error) {
	var t fileTransfer
	var ok bool

	if t.fromId, ok = params["from"].(string); !ok || t.fromId == "" {
		return t, fmt.Errorf(`%s: "from" parameter is required`, action)
	}
	if t.toId, ok = params["to"].(string); !ok || t.toId == "" {
		return t, fmt.Errorf(`%s: "to" parameter is required`, action)
	}
	if t.sourceRelPath, ok = params["path"].(string); !ok || t.sourceRelPath == "" {
		return t, fmt.Errorf(`%s: "path" parameter is required`, action)
	}
	t.targetRelPath = t.sourceRelPath
	if raw, ok := params["targetPath"]; ok {
		t.targetRelPath, ok = raw.(string)
		if !ok || t.targetRelPath == "" {
			return t, fmt.Errorf(`%s: "targetPath" must be a non-empty path, got %v`, action, raw)
		}
	}

	fromPkg, err := appCtx.Settings.GetPackage(t.fromId)
	if err != nil {
		return t, err
	}
	toPkg, err := appCtx.Settings.GetPackage(t.toId)
	if err != nil {
		return t, err
	}
	if (modifiesSource && !SupportsSyncBack(fromPkg.Type)) || !SupportsSyncBack(toPkg.Type) {
		return t, fmt.Errorf("%s only supports FS/S3 packages as %s, got from=%s(%s) to=%s(%s)",
			action, syncBackRoles(modifiesSource), t.fromId, fromPkg.Type, t.toId, toPkg.Type)
	}
	if err := requireLocalMirror(fromPkg); err != nil {
		return t, fmt.Errorf("%s: from: %w", action, err)
	}
	if err := requireLocalMirror(toPkg); err != nil {
		return t, fmt.Errorf("%s: to: %w", action, err)
	}

	if t.srcPath, err = mirrorPath(fromPkg, t.sourceRelPath); err != nil {
		return t, fmt.Errorf("%s: path: %w", action, err)
	}
	if t.dstPath, err = mirrorPath(toPkg, t.targetRelPath); err != nil {
		return t, fmt.Errorf("%s: targetPath: %w", action, err)
	}
	return t, nil
}

func syncBackRoles(modifiesSource bool) string {
	if modifiesSource {
		return "from and to"
	}
	return "to"
}
