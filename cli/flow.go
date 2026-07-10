package cli

import (
	"fmt"

	"github.com/ldproxy/xtrasync/app"
	"github.com/ldproxy/xtrasync/app/workflows"
)

type Flow struct {
	Run  FlowRunCmd  `cmd:"" help:"Run a workflow"`
	List FlowListCmd `cmd:"" help:"List configured workflows"`
}

type FlowRunCmd struct {
	Id string `arg:"" help:"Workflow id"`
}

func (c *FlowRunCmd) Run(appCtx *app.AppContext) error {
	if err := workflows.Run(appCtx, c.Id); err != nil {
		appCtx.Logger.Error().Err(err).Str("id", c.Id).Msg("workflow run failed")
		return err
	}
	return nil
}

type FlowListCmd struct{}

func (c *FlowListCmd) Run(appCtx *app.AppContext) error {
	for _, wf := range appCtx.Settings.Workflows {
		fmt.Printf("%s\t%d steps\n", wf.Id, len(wf.Steps))
	}
	return nil
}
