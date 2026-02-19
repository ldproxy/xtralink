package cli

import "xtra-sync/app"

type CLI struct {
	Config string  `help:"Pfad zur Steuer-Konfigurationsdatei." default:"config/exampleConfig.yaml" global:"true"`
	Sync   SyncCmd `cmd:"" help:"Lädt die Steuer-Config und startet die Synchronisierung."`
}

type SyncCmd struct{}

func (c *SyncCmd) Run(root *CLI) error {
	svc := app.NewService()
	return svc.Run(root.Config)
}
