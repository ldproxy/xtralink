package cli

import (
	"xtra-sync/app"
)

type CLI struct {
	Config string  `help:"Pfad zur Steuer-Konfigurationsdatei." default:"config/exampleConfig.yaml" global:"true"`
	Sync   SyncCmd `cmd:"" help:"Lädt die Steuer-Config und startet die Synchronisierung."`
	Push   PushCmd `cmd:"" help:"Synchronisiert ein Remote per ID und pusht das Ergebnis als OCI-Artifact."`
}

type SyncCmd struct{}
type PushCmd struct {
	RemoteID  string `name:"id" help:"ID des Quell-Remotes aus der Steuer-Konfiguration." required:""`
	ImageName string `name:"image" help:"Ziel-Image-Name unter docker.ci.interactive-instruments.de/xtrasync/." required:""`
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
