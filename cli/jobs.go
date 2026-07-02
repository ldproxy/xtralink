package cli

import (
	"encoding/json"
	"fmt"

	"github.com/ldproxy/xtrasync/app"
	"github.com/ldproxy/xtrasync/app/jobs"
)

type Jobs struct {
	Push   JobsPushCmd   `cmd:"" help:"Create a new job set"`
	Status JobsStatusCmd `cmd:"" help:"Print status/progress of a job set"`
	Get    JobsGetCmd    `cmd:"" help:"Print full details of a job set as JSON"`
	List   JobsListCmd   `cmd:"" help:"List all job sets"`
}

type JobsPushCmd struct {
	Type     string `arg:"" help:"Job type"`
	Inputs   string `help:"Inputs as JSON" default:""`
	Label    string `help:"Human readable label"`
	Entity   string `help:"Entity/provider id this job set belongs to"`
	Priority int    `help:"Priority, higher runs first" default:"1000"`
}

func (c *JobsPushCmd) Run(appCtx *app.AppContext) error {
	js, err := jobs.Push(appCtx, c.Type, c.Label, c.Entity, c.Priority, c.Inputs)
	if err != nil {
		appCtx.Logger.Error().Err(err).Str("type", c.Type).Msg("push failed")
		return err
	}

	fmt.Printf("%s\t%s\n", js.ID, js.Status())
	return nil
}

type JobsStatusCmd struct {
	Id string `arg:"" help:"Job set id"`
}

func (c *JobsStatusCmd) Run(appCtx *app.AppContext) error {
	status, err := jobs.Status(appCtx, c.Id)
	if err != nil {
		appCtx.Logger.Error().Err(err).Str("id", c.Id).Msg("status failed")
		return err
	}

	fmt.Printf("%s\t%d%%\t%s\n", status.Status, status.Percent, status.Message)
	return nil
}

type JobsGetCmd struct {
	Id string `arg:"" help:"Job set id"`
}

func (c *JobsGetCmd) Run(appCtx *app.AppContext) error {
	js, err := jobs.Get(appCtx, c.Id)
	if err != nil {
		appCtx.Logger.Error().Err(err).Str("id", c.Id).Msg("get failed")
		return err
	}

	raw, err := json.MarshalIndent(js, "", "  ")
	if err != nil {
		return fmt.Errorf("could not encode job set as json: %w", err)
	}

	fmt.Println(string(raw))
	return nil
}

type JobsListCmd struct{}

func (c *JobsListCmd) Run(appCtx *app.AppContext) error {
	views, err := jobs.List(appCtx)
	if err != nil {
		appCtx.Logger.Error().Err(err).Msg("list failed")
		return err
	}

	for _, v := range views {
		fmt.Printf("%s\t%s\t%s\t%d%%\n", v.ID, v.Type, v.Status, v.Percent)
	}
	return nil
}
