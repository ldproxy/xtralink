package actions

import (
	"github.com/google/uuid"

	"github.com/ldproxy/xtralink/lib/workflows"
)

// UUIDGenAction implements "uuid:gen": generates one random (version 4)
// UUID and exposes it as ${outputs.<step>.uuid}, for a workflow that needs
// a name nothing else can collide with - a scratch directory, an artifact
// tag, a correlation id carried into a pushed Job.
//
// It takes no parameters. Where the step sits relative to a forking step
// (pkg:find_each and the like) decides what the UUID means, and both
// placements are legitimate: before the fork every branch shares one UUID,
// after it each branch generates its own. Nothing warns either way, so it
// is worth being deliberate about.
type UUIDGenAction struct{}

func (a *UUIDGenAction) Type() string { return "uuid:gen" }

func (a *UUIDGenAction) Run(ctx *workflows.StepContext) (workflows.StepResult, error) {
	generated := uuid.NewString()
	ctx.Logger.Debug().Str("uuid", generated).Msg("generated uuid")

	return workflows.One(map[string]any{"uuid": generated}), nil
}
