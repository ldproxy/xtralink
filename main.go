package main

import (
	"github.com/alecthomas/kong"
	"github.com/mew-sh/dotenv"

	"github.com/ldproxy/xtrasync/app"
	"github.com/ldproxy/xtrasync/cli"
)

const (
	// Name is the name of the application.
	Name = "xtrasync"
	// Description is the description of the application.
	Description = "A glue tool for distributed applications"
)

var gitTag string
var gitSha string
var gitBranch = "unknown"

func version() string {
	if len(gitTag) > 0 {
		return gitTag
	} else if len(gitSha) > 0 {
		return gitBranch + "-" + gitSha
	}

	return "DEV"
}

func main() {
	version := version()
	cli := cli.CLI{}

	ctx := kong.Parse(&cli,
		kong.Name(Name),
		kong.Description(Description),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact:             true,
			FlagsLast:           true,
			NoExpandSubcommands: false,
		}),
		kong.Vars{
			"version": version,
		})

	ctx.Bind(&cli.Globals)

	initialize(ctx, cli.Config, cli.Verbose, version)

	err := ctx.Run()

	ctx.FatalIfErrorf(err)
}

func initialize(ctx *kong.Context, config string, verbosity uint, version string) {
	dotenv.Load()

	settings, err := app.LoadSettings(config)

	if err != nil {
		panic(err)
	}

	appCtx := app.NewAppContext(Name, version, verbosity, settings)

	ctx.Bind(appCtx)
}
