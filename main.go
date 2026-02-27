package main

import (
	"github.com/alecthomas/kong"

	"github.com/ldproxy/xtrasync/cli"
)

func main() {
	var appCLI cli.CLI
	ctx := kong.Parse(&appCLI,
		kong.Name("xtrasync"),
		kong.Description("Synchronizes configuration sources (MVP: Git)"),
	)
	ctx.FatalIfErrorf(ctx.Run())
}

// go run . --config config/exampleConfig.yaml sync
// go build -o xtra-sync . && ./xtra-sync --config config/exampleConfig.yaml sync
// go run . --config config/all.yaml push --id bplan --image test-bplan --tag latest

// Cache Ordner: echo $TMPDIR.   + /xtrasync-cache/git
