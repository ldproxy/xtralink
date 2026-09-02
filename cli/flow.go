package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/app/workflows"
)

type Flow struct {
	Run  FlowRunCmd  `cmd:"" help:"Run a workflow"`
	Get  FlowGetCmd  `cmd:"" help:"Print full details of a workflow as JSON"`
	List FlowListCmd `cmd:"" help:"List configured workflows"`
}

type FlowRunCmd struct {
	Id      string   `arg:"" help:"Workflow id"`
	Inputs  []string `name:"input" sep:"none" help:"Parameter override as name=value (repeatable)"`
	Workers bool     `help:"After the workflow, also process the jobs it pushed, until interrupted - for development, where running \"job process\" alongside is a nuisance"`
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
	if !c.Workers {
		return nil
	}

	// job:push is fire-and-forget, so the workflow is already done here and
	// whatever it pushed is sitting in the queue: the workers can simply
	// take over the foreground. Nothing is lost by starting them second.
	//
	// A production deployment runs `job process` as its own process, sized
	// and restarted independently; this flag exists so a single command is
	// enough to watch a workflow through end to end while writing it.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	appCtx.Logger.Info().Str("id", c.Id).Msg("workflow pushed its jobs, processing them until interrupted")
	return RunJobWorkers(appCtx, ctx, "*")
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

type FlowGetCmd struct {
	Id string `arg:"" help:"Workflow id"`
}

func (c *FlowGetCmd) Run(appCtx *app.AppContext) error {
	wf, err := appCtx.Settings.GetWorkflow(c.Id)
	if err != nil {
		appCtx.Logger.Error().Err(err).Str("id", c.Id).Msg("get failed")
		return err
	}

	raw, err := json.MarshalIndent(wf, "", "  ")
	if err != nil {
		return fmt.Errorf("could not encode workflow as json: %w", err)
	}

	fmt.Println(string(raw))
	return nil
}

type FlowListCmd struct{}

func (c *FlowListCmd) Run(appCtx *app.AppContext) error {
	for _, wf := range appCtx.Settings.Workflows {
		fmt.Printf("%s\t%s\n", wf.Id, wf.Description)
	}
	return nil
}
