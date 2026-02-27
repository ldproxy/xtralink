package cli

import (
	"github.com/ldproxy/xtrasync/app"
)

type CLI struct {
	Config string  `help:"Path to the control configuration file." default:"config/exampleConfig.yaml" global:"true"`
	Sync   SyncCmd `cmd:"" help:"Loads the control configuration and starts synchronization."`
	Push   PushCmd `cmd:"" help:"Synchronizes a remote by ID and pushes the result as an OCI artifact."`
}

type SyncCmd struct{}
type PushCmd struct {
	RemoteID  string `name:"id" help:"ID of the source remote from the control configuration." required:""`
	ImageName string `name:"image" help:"Target image name under docker.ci.interactive-instruments.de/xtrasync/." required:""`
	Tag       string `name:"tag" help:"OCI-Tag (default: latest)."`
}

func (c *SyncCmd) Run(root *CLI) error {
	svc := app.NewService()
	if err := svc.Run(root.Config); err != nil {
		logger := svc.Logger()
		logger.Error().Err(err).Str("config", root.Config).Msg("sync failed")
		return err
	}
	return nil
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
