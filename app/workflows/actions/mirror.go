package actions

import (
	"fmt"
	"os"

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
