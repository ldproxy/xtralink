package cli

import (
	"fmt"

	"github.com/alecthomas/kong"
)

type Globals struct {
	Config  string      `short:"c" help:"Path to the configuration file" default:".xtralink.yml"`
	Verbose uint        `short:"v" type:"counter" help:"Enable verbose mode"`
	Version VersionFlag `name:"version" help:"Print version information"`
}

type VersionFlag string

func (v VersionFlag) Decode(ctx *kong.DecodeContext) error { return nil }
func (v VersionFlag) IsBool() bool                         { return true }

func (v VersionFlag) BeforeApply(app *kong.Kong, vars kong.Vars) error {
	fmt.Println(vars["version"])
	app.Exit(0)
	return nil
}

type CLI struct {
	Globals

	Pkg  Pkg  `cmd:"" help:"Manage packages"`
	Job  Job  `cmd:"" help:"Manage jobs"`
	Flow Flow `cmd:"" help:"Manage workflows"`
}
