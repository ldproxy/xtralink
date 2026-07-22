package cli

import (
	"encoding/json"
	"fmt"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/app/pkg"
)

type Pkg struct {
	Pull    PullCmd    `cmd:"" help:"Synchronize packages"`
	Push    PushCmd    `cmd:"" help:"Push a package to an OCI registry"`
	Inspect InspectCmd `cmd:"" help:"Inspect one package and print analysis as JSON"`
}

type PullCmd struct {
	Id string `arg:"" help:"Package id" optional:""`
}

func (c *PullCmd) Run(appCtx *app.AppContext) error {
	appCtx.Logger.Info().Msg("starting synchronization")
	if err := pkg.Pull(appCtx, c.Id); err != nil {
		appCtx.Logger.Error().Err(err).Msg("pull failed")
		return err
	}
	return nil
}

type PushCmd struct {
	SourceRemoteId string `arg:"" name:"source" help:"Source package id" required:""`
	TargetRemoteId string `arg:"" name:"target" help:"Target package id" required:""`
	TargetTag      string `arg:"" name:"tag" help:"Target OCI artifact tag" required:""`
	NoSync         bool   `short:"n" help:"Do not synchronize the source package"`
}

func (c *PushCmd) Run(root *CLI, appCtx *app.AppContext) error {
	if err := pkg.Push(appCtx, c.SourceRemoteId, c.TargetRemoteId, c.TargetTag, c.NoSync); err != nil {
		appCtx.Logger.Error().
			Err(err).
			Str("config", root.Config).
			Str("source_remote_id", c.SourceRemoteId).
			Str("target_remote_id", c.TargetRemoteId).
			Str("target_tag", c.TargetTag).
			Msg("push failed")
		return err
	}
	return nil
}

func (c *PushCmd) Help() string {
	//return "Examples:\n xtralink pkg push my-package-id example.com/repo/image:tag"
	return "Example: xtralink pkg push my-package-id example.com/repo/image:tag"
}

type InspectCmd struct {
	Id string `arg:"" help:"Package id" required:""`
}

func (c *InspectCmd) Run(appCtx *app.AppContext) error {
	result, err := pkg.Inspect(appCtx, c.Id)
	if err != nil {
		appCtx.Logger.Error().Err(err).Str("id", c.Id).Msg("inspect failed")
		return err
	}

	raw, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("could not encode inspect result as json: %w", err)
	}

	fmt.Println(string(raw))
	return nil
}
