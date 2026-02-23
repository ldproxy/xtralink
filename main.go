package main

import (
	"github.com/alecthomas/kong"

	"xtra-sync/cli"
)

func main() {
	var appCLI cli.CLI
	ctx := kong.Parse(&appCLI,
		kong.Name("xtra-sync"),
		kong.Description("Synchronizes configuration sources (MVP: Git)"),
	)
	ctx.FatalIfErrorf(ctx.Run())
}

// go run . --config config/exampleConfig.yaml sync
// go build -o xtra-sync . && ./xtra-sync --config config/exampleConfig.yaml sync

// Cache Ordner: echo $TMPDIR.   + /xtra-sync-cache/git
