package cli

import (
	"strings"

	"github.com/ldproxy/xtrasync/app"
	"github.com/ldproxy/xtrasync/app/pkg"
)

type Pkg struct {
	Pull PullCmd `cmd:"" help:"Synchronize packages"`
	Push PushCmd `cmd:"" help:"Push a package to an OCI registry"`
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
	RemoteID  string `arg:"" name:"id" help:"Source package id" required:""`
	ImageName string `arg:"" name:"image" help:"Target OCI artifact URL" required:""`
}

func (c *PushCmd) Run(root *CLI, appCtx *app.AppContext) error {
	image := c.ImageName
	tag := "latest"
	if strings.Contains(c.ImageName, ":") {
		parts := strings.SplitN(c.ImageName, ":", 2)
		image = parts[0]
		tag = parts[1]
	}

	if err := pkg.Push(appCtx, c.RemoteID, image, tag); err != nil {
		appCtx.Logger.Error().
			Err(err).
			Str("config", root.Config).
			Str("remote_id", c.RemoteID).
			Str("image", image).
			Str("tag", tag).
			Msg("push failed")
		return err
	}
	return nil
}

func (c *PushCmd) Help() string {
	//return "Examples:\n xtrasync pkg push my-package-id example.com/repo/image:tag"
	return "Example: xtrasync pkg push my-package-id example.com/repo/image:tag"
}
