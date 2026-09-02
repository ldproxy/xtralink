package workflows

import (
	"fmt"

	"github.com/rs/zerolog"
)

// StepContext is what the engine hands to an Action.Run call: the Step's
// parameters, already template-resolved against the current branch's
// outputs/packages state (s. template.go) - Actions never see raw ${...}
// placeholders or the outputs of other steps directly - plus the logger to
// report through.
//
// Logger is a child of the run-scoped logger, already carrying the step id
// and action type as fields, so an Action logs its own detail without
// having to know which step it is running as. Actions get their logger
// here rather than from an injected *app.AppContext so that an Action
// needing nothing else from the app (s. CmdExecAction) stays free of it.
type StepContext struct {
	Params map[string]any
	Logger zerolog.Logger
}

// Action is the plugin contract for a Step type, analogous to JobProcessor
// in lib/jobs: it declares which action name it handles and does the actual
// work. New Action types register themselves (s. Registry) without the
// engine needing to change.
type Action interface {
	Type() string
	Run(ctx *StepContext) (StepResult, error)
}

// Registry resolves a Step's Action string to its Action implementation.
type Registry struct {
	actions map[string]Action
}

func NewRegistry() *Registry {
	return &Registry{actions: map[string]Action{}}
}

func (r *Registry) Register(a Action) {
	r.actions[a.Type()] = a
}

func (r *Registry) Lookup(actionType string) (Action, error) {
	a, ok := r.actions[actionType]
	if !ok {
		return nil, fmt.Errorf("unknown action type: %s", actionType)
	}
	return a, nil
}
