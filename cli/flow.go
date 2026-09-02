package cli

import (
	"fmt"
	"strings"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/app/workflows"
)

type Flow struct {
	Run  FlowRunCmd  `cmd:"" help:"Run a workflow"`
	List FlowListCmd `cmd:"" help:"List configured workflows"`
}

type FlowRunCmd struct {
	Id     string   `arg:"" help:"Workflow id"`
	Inputs []string `name:"input" sep:"none" help:"Parameter override as name=value (repeatable)"`
}

func (c *FlowRunCmd) Run(appCtx *app.AppContext) error {
	overrides, err := workflows.ParseOverrides(c.Inputs)
	if err != nil {
		appCtx.Logger.Error().Err(err).Str("id", c.Id).Msg("invalid --input")
		return err
	}
	if err := workflows.Run(appCtx, c.Id, overrides); err != nil {
		appCtx.Logger.Error().Err(err).Str("id", c.Id).Strs("inputs", inputNames(c.Inputs)).Msg("workflow run failed")
		return err
	}
	return nil
}

// inputNames reduces the --input flags to the parameter names they set.
// Their values are deliberately left out: they come from whoever invoked
// the CLI and may well be credentials, and a log line is exactly the wrong
// place for those.
func inputNames(inputs []string) []string {
	names := make([]string, 0, len(inputs))
	for _, entry := range inputs {
		name, _, _ := strings.Cut(entry, "=")
		names = append(names, name)
	}
	return names
}

type FlowListCmd struct{}

func (c *FlowListCmd) Run(appCtx *app.AppContext) error {
	for _, wf := range appCtx.Settings.Workflows {
		fmt.Printf("%s\t%d steps\n", wf.Id, len(wf.Steps))
	}
	return nil
}
