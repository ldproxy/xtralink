package cli

import (
	"encoding/json"
	"fmt"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/app/jobs"
)

type Job struct {
	Push   JobPushCmd   `cmd:"" help:"Create a new job"`
	Status JobStatusCmd `cmd:"" help:"Print status/progress of a job"`
	Get    JobGetCmd    `cmd:"" help:"Print full details of a job as JSON"`
	List   JobListCmd   `cmd:"" help:"List all jobs"`
}

type JobPushCmd struct {
	Type     string `arg:"" help:"Job type"`
	Inputs   string `help:"Inputs as JSON" default:""`
	Label    string `help:"Human readable label"`
	Priority int    `help:"Priority, higher runs first" default:"1000"`
}

func (c *JobPushCmd) Run(appCtx *app.AppContext) error {
	job, err := jobs.Push(appCtx, c.Type, c.Label, c.Priority, c.Inputs)
	if err != nil {
		appCtx.Logger.Error().Err(err).Str("type", c.Type).Msg("push failed")
		return err
	}

	fmt.Printf("%s\t%s\n", job.ID, job.Status())
	return nil
}

type JobStatusCmd struct {
	Id string `arg:"" help:"Job id"`
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
	Id string `arg:"" help:"Job id"`
}

func (c *JobGetCmd) Run(appCtx *app.AppContext) error {
	job, err := jobs.Get(appCtx, c.Id)
	if err != nil {
		appCtx.Logger.Error().Err(err).Str("id", c.Id).Msg("get failed")
		return err
	}

	raw, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		return fmt.Errorf("could not encode job as json: %w", err)
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
