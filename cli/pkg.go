package cli

import (
	"fmt"

	"github.com/ldproxy/xtrasync/app"
)

type Pkg struct {
	Pull PullCmd `cmd:"" help:"Synchronize packages"`
	Push PushCmd `cmd:"" help:"Push a package to an OCI registry"`
}

type PullCmd struct {
	Id string `arg:"" help:"Package id" optional:""`
}

func (c *PullCmd) Run(root *CLI) error {
	svc := app.NewService()
	logger := svc.Logger()
	logger.Info().Str("verbosity", fmt.Sprintf("%d", root.Verbose)).Msg("starting synchronization")
	if err := svc.Run(root.Config, c.Id); err != nil {

		logger.Error().Err(err).Str("config", root.Config).Msg("sync failed")
		return err
	}
	return nil
}

type PushCmd struct {
	RemoteID  string `arg:"" name:"id" help:"Source package id" required:""`
	ImageName string `name:"image" help:"Target OCI artifactory URL" required:""`
	Tag       string `name:"tag" help:"OCI-Tag (default: latest)."`
}

func (c *PushCmd) Run(root *CLI) error {
	svc := app.NewService()
	if err := svc.RunPush(root.Config, c.RemoteID, c.ImageName, c.Tag); err != nil {
		logger := svc.Logger()
		logger.Error().
			Err(err).
			Str("config", root.Config).
			Str("remote_id", c.RemoteID).
			Str("image", c.ImageName).
			Str("tag", c.Tag).
			Msg("push failed")
		return err
	}
	return nil
}
