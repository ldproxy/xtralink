package actions

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ldproxy/xtralink/app"
)

// requireLocalMirror insists that p's local mirror already exists, i.e.
// that the workflow pulled the package in an earlier step.
//
// No action pulls implicitly - a workflow says what it does - and that
// makes an absent mirror something to refuse rather than work around. A
// find over a directory that was never pulled would report zero matches
// and halt the workflow as a success, and a push from one would mirror
// nothing back over a populated remote. Both are worse than stopping here
// with an error that names the missing step.
//
// Call it after any check on the package's configuration (s.
// SupportsSyncBack): a package that could never work belongs in an error
// about the workflow, not one about how far it got.
func requireLocalMirror(p *app.Package) error {
	info, err := os.Stat(p.ResolvedLocalPath)
	if os.IsNotExist(err) {
		return fmt.Errorf("package %q has no local mirror at %s yet - add a pkg:pull step for it first",
			p.Id, p.ResolvedLocalPath)
	}
	if err != nil {
		return fmt.Errorf("could not read the local mirror of package %q: %w", p.Id, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("the local mirror of package %q is not a directory: %s", p.Id, p.ResolvedLocalPath)
	}

	return nil
}

// mirrorPath resolves a package-root-relative, slash-form path (the form
// pkg:find_any/pkg:find_each hand out) to an absolute path inside p's local
// mirror, refusing one that would land outside it.
//
// Containment is checked rather than assumed: a path can also come from a
// hand-written workflow or an interpolated --input, and one climbing out
// with ".." would have a package's action write somewhere the package does
// not own - somewhere SyncBack would never carry to the remote, and that a
// move could delete from.
func mirrorPath(p *app.Package, relPath string) (string, error) {
	resolved := filepath.Join(p.ResolvedLocalPath, filepath.FromSlash(relPath))

	rel, err := filepath.Rel(p.ResolvedLocalPath, resolved)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q resolves outside the local mirror of package %q", relPath, p.Id)
	}
	return resolved, nil
}
