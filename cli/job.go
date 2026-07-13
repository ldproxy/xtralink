package cli

import (
	"encoding/json"
	"fmt"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/app/jobs"
)

// JobBase holds the commands present in every build. The full Job type
// (JobBase plus, only with -tags demo, a Demo field) is defined per build -
// job_demo.go or job_nodemo.go - so that without the tag, `job demo`
// doesn't just fail at runtime but is absent from the command tree/--help
// entirely, exactly like it never existed.
type JobBase struct {
	Push   JobPushCmd   `cmd:"" help:"Create a new job set"`
	Status JobStatusCmd `cmd:"" help:"Print status/progress of a job set"`
	Get    JobGetCmd    `cmd:"" help:"Print full details of a job set as JSON"`
	List   JobListCmd   `cmd:"" help:"List all job sets"`
}

type JobPushCmd struct {
	Type     string `arg:"" help:"Job type"`
	Inputs   string `help:"Inputs as JSON" default:""`
	Label    string `help:"Human readable label"`
	Entity   string `help:"Entity/provider id this job set belongs to"`
	Priority int    `help:"Priority, higher runs first" default:"1000"`
}

func (c *JobPushCmd) Run(appCtx *app.AppContext) error {
	js, err := jobs.Push(appCtx, c.Type, c.Label, c.Entity, c.Priority, c.Inputs)
	if err != nil {
		appCtx.Logger.Error().Err(err).Str("type", c.Type).Msg("push failed")
		return err
	}

	fmt.Printf("%s\t%s\n", js.ID, js.Status())
	return nil
}

type JobStatusCmd struct {
	Id string `arg:"" help:"Job set id"`
}

func (c *JobStatusCmd) Run(appCtx *app.AppContext) error {
	status, err := jobs.Status(appCtx, c.Id)
	if err != nil {
		appCtx.Logger.Error().Err(err).Str("id", c.Id).Msg("status failed")
		return err
	}

	fmt.Printf("%s\t%d%%\t%s\n", status.Status, status.Percent, status.Message)
	return nil
}

type JobGetCmd struct {
	Id string `arg:"" help:"Job set id"`
}

func (c *JobGetCmd) Run(appCtx *app.AppContext) error {
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

type JobListCmd struct{}

func (c *JobListCmd) Run(appCtx *app.AppContext) error {
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
