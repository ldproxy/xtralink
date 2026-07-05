package cli

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ldproxy/xtrasync/app"
	"github.com/ldproxy/xtrasync/app/jobs"
	"github.com/ldproxy/xtrasync/app/jobs/counterdemo"
	"github.com/ldproxy/xtrasync/app/jobs/tileseedingdemo"
)

type Jobs struct {
	Push   JobsPushCmd   `cmd:"" help:"Create a new job set"`
	Status JobsStatusCmd `cmd:"" help:"Print status/progress of a job set"`
	Get    JobsGetCmd    `cmd:"" help:"Print full details of a job set as JSON"`
	List   JobsListCmd   `cmd:"" help:"List all job sets"`
	Demo   JobsDemo      `cmd:"" help:"Run demo job processors (not for production use)"`
}

type JobsDemo struct {
	Tileseeding JobsDemoTileseedingCmd `cmd:"" help:"Simulated tile-seeding run: setup splits into sub-jobs, workers fake-render them, cleanup writes a report"`
	Counter     JobsDemoCounterCmd     `cmd:"" help:"Minimal single-job run: no setup/cleanup, no progressDetails - proves the core doesn't assume the tile-seeding shape"`
}

type JobsDemoCounterCmd struct {
	Steps   int           `help:"How many steps to count" default:"5"`
	Step    time.Duration `name:"step-duration" help:"Simulated time per step" default:"200ms"`
	FailAt  int           `name:"fail-at" help:"Simulate a permanent failure at this step (0 = never)" default:"0"`
	Timeout time.Duration `help:"Give up waiting for the job set to finish after this" default:"30s"`
}

func (c *JobsDemoCounterCmd) Run(appCtx *app.AppContext) error {
	result, err := counterdemo.Run(appCtx, counterdemo.Options{
		Steps:        c.Steps,
		StepDuration: c.Step,
		FailAt:       c.FailAt,
		Timeout:      c.Timeout,
	})
	if result != nil {
		if raw, encErr := json.MarshalIndent(result, "", "  "); encErr == nil {
			fmt.Println(string(raw))
		}
	}
	if err != nil {
		appCtx.Logger.Error().Err(err).Msg("counter demo failed")
		return err
	}
	return nil
}

type JobsDemoTileseedingCmd struct {
	Entity   string        `help:"Entity/provider id" default:"demo-tiles"`
	Tilesets []string      `help:"Tileset names" default:"vineyards" sep:","`
	Timeout  time.Duration `help:"Give up waiting for the job set to finish after this" default:"30s"`
	Tile     time.Duration `name:"tile-duration" help:"Simulated time per tile" default:"50ms"`
	FollowUp bool          `name:"with-follow-up" help:"Attach a second job set as a followUp, to exercise that code path too"`
}

func (c *JobsDemoTileseedingCmd) Run(appCtx *app.AppContext) error {
	result, err := tileseedingdemo.Run(appCtx, tileseedingdemo.Options{
		Entity:       c.Entity,
		TileSets:     c.Tilesets,
		Timeout:      c.Timeout,
		TileDuration: c.Tile,
		WithFollowUp: c.FollowUp,
	})
	if result != nil {
		if raw, encErr := json.MarshalIndent(result, "", "  "); encErr == nil {
			fmt.Println(string(raw))
		}
	}
	if err != nil {
		appCtx.Logger.Error().Err(err).Msg("tile-seeding demo failed")
		return err
	}
	return nil
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
