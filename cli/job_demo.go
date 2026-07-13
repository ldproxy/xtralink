//go:build demo

package cli

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/app/jobs/counterdemo"
	"github.com/ldproxy/xtralink/app/jobs/tileseedingdemo"
)

// Job (this build: compiled with -tags demo) adds the Demo command on top
// of JobBase. JobDemo wires up the example job processors that prove
// lib/jobs generalizes beyond tile-seeding. Not meant for production use,
// which is why it - and the tileseedingdemo/counterdemo packages it depends
// on - only exist in the binary (and only show up in --help) when explicitly
// built with -tags demo.
type Job struct {
	JobBase
	Demo JobDemo `cmd:"" help:"Run demo job processors (not for production use)"`
}

type JobDemo struct {
	Tileseeding JobDemoTileseedingCmd `cmd:"" help:"Simulated tile-seeding run: setup splits into sub-jobs, workers fake-render them, cleanup writes a report"`
	Counter     JobDemoCounterCmd     `cmd:"" help:"Minimal single-job run: no setup/cleanup, no progressDetails - proves the core doesn't assume the tile-seeding shape"`
}

type JobDemoCounterCmd struct {
	Steps   int           `help:"How many steps to count" default:"5"`
	Step    time.Duration `name:"step-duration" help:"Simulated time per step" default:"200ms"`
	FailAt  int           `name:"fail-at" help:"Simulate a permanent failure at this step (0 = never)" default:"0"`
	Timeout time.Duration `help:"Give up waiting for the job set to finish after this" default:"30s"`
}

func (c *JobDemoCounterCmd) Run(appCtx *app.AppContext) error {
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

type JobDemoTileseedingCmd struct {
	Entity   string        `help:"Entity/provider id" default:"demo-tiles"`
	Tilesets []string      `help:"Tileset names" default:"vineyards" sep:","`
	Timeout  time.Duration `help:"Give up waiting for the job set to finish after this" default:"30s"`
	Tile     time.Duration `name:"tile-duration" help:"Simulated time per tile" default:"50ms"`
	FollowUp bool          `name:"with-follow-up" help:"Attach a second job set as a followUp, to exercise that code path too"`
}

func (c *JobDemoTileseedingCmd) Run(appCtx *app.AppContext) error {
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
