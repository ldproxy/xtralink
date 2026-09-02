package workflows

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// handlerRecorder registers a "record" action noting every step that ran,
// and a "fail" action that never succeeds.
type handlerRecorder struct {
	registry *Registry
	ran      []string
	seen     map[string]map[string]any
}

func newHandlerRecorder() *handlerRecorder {
	rec := &handlerRecorder{registry: NewRegistry(), seen: map[string]map[string]any{}}
	rec.registry.Register(&funcAction{actionType: "record", run: func(ctx *StepContext) (StepResult, error) {
		name, _ := ctx.Params["name"].(string)
		rec.ran = append(rec.ran, name)
		rec.seen[name] = ctx.Params
		return one(map[string]any{}), nil
	}})
	rec.registry.Register(&funcAction{actionType: "fail", run: func(ctx *StepContext) (StepResult, error) {
		return StepResult{}, fmt.Errorf("boom")
	}})
	return rec
}

func recordStep(name string) Step {
	return Step{Action: "record", Id: name, Params: map[string]any{"name": name}}
}

func TestHandlers_FailureRunsAndSuccessDoesNot(t *testing.T) {
	rec := newHandlerRecorder()
	wf := Workflow{
		Steps: []Step{{Id: "doomed", Action: "fail"}},
		Handlers: &Handlers{
			Failure: []Step{recordStep("on-failure")},
			Success: []Step{recordStep("on-success")},
			Always:  []Step{recordStep("always")},
		},
	}

	err := Run(wf, rec.registry, map[string]any{}, zerolog.Nop())
	if err == nil {
		t.Fatal("expected the run to fail")
	}
	if got := strings.Join(rec.ran, ","); got != "on-failure,always" {
		t.Errorf("handlers ran %q, want \"on-failure,always\"", got)
	}
}

func TestHandlers_SuccessRunsAndFailureDoesNot(t *testing.T) {
	rec := newHandlerRecorder()
	wf := Workflow{
		Steps: []Step{recordStep("main")},
		Handlers: &Handlers{
			Failure: []Step{recordStep("on-failure")},
			Success: []Step{recordStep("on-success")},
			Always:  []Step{recordStep("always")},
		},
	}

	if err := Run(wf, rec.registry, map[string]any{}, zerolog.Nop()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := strings.Join(rec.ran, ","); got != "main,on-success,always" {
		t.Errorf("steps ran %q, want \"main,on-success,always\"", got)
	}
}

func TestHandlers_ErrorNamespaceNamesTheFailingStep(t *testing.T) {
	rec := newHandlerRecorder()
	wf := Workflow{
		Steps: []Step{recordStep("first"), {Id: "doomed", Action: "fail"}},
		Handlers: &Handlers{Failure: []Step{{
			Action: "record",
			Id:     "on-failure",
			Params: map[string]any{
				"name":    "on-failure",
				"message": "${error.message}",
				"step":    "${error.step}",
				"action":  "${error.action}",
			},
		}}},
	}

	if err := Run(wf, rec.registry, map[string]any{}, zerolog.Nop()); err == nil {
		t.Fatal("expected the run to fail")
	}

	seen := rec.seen["on-failure"]
	if seen["step"] != "doomed" {
		t.Errorf("error.step = %v, want doomed", seen["step"])
	}
	if seen["action"] != "fail" {
		t.Errorf("error.action = %v, want fail", seen["action"])
	}
	message, _ := seen["message"].(string)
	if !strings.Contains(message, "boom") || !strings.Contains(message, "id=doomed") {
		t.Errorf("error.message = %q, want the step and its cause", message)
	}
}

func TestHandlers_ErrorNamespaceIsEmptyOnSuccess(t *testing.T) {
	rec := newHandlerRecorder()
	wf := Workflow{
		Steps: []Step{recordStep("main")},
		Handlers: &Handlers{Always: []Step{{
			Action: "record",
			Id:     "always",
			Params: map[string]any{"name": "always", "message": "${error.message}"},
		}}},
	}

	if err := Run(wf, rec.registry, map[string]any{}, zerolog.Nop()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := rec.seen["always"]["message"]; got != "" {
		t.Errorf("error.message = %q, want empty when nothing failed", got)
	}
}

func TestHandlers_SeeParamsAndPackagesButNotOutputs(t *testing.T) {
	rec := newHandlerRecorder()
	vars := map[string]any{
		"parameters": map[string]any{"who": "me"},
		"packages":   map[string]any{"foo": map[string]any{"id": "foo"}},
	}

	wf := Workflow{
		Steps: []Step{recordStep("main")},
		Handlers: &Handlers{Success: []Step{{
			Action: "record",
			Id:     "on-success",
			Params: map[string]any{"name": "on-success", "who": "${parameters.who}", "pkg": "${packages.foo.id}"},
		}}},
	}
	if err := Run(wf, rec.registry, vars, zerolog.Nop()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if seen := rec.seen["on-success"]; seen["who"] != "me" || seen["pkg"] != "foo" {
		t.Errorf("handler saw %+v, want params and packages resolved", seen)
	}

	// A handler naming ${outputs...} is an error, not a silent empty: after
	// a forking step there is no single outputs tree to show it.
	wf.Handlers = &Handlers{Success: []Step{{
		Action: "record",
		Id:     "on-success",
		Params: map[string]any{"name": "on-success", "leaked": "${outputs.main.anything}"},
	}}}
	err := Run(wf, rec.registry, vars, zerolog.Nop())
	if err == nil {
		t.Fatal("expected referencing outputs from a handler to fail")
	}
	if !strings.Contains(err.Error(), "handlers.success") {
		t.Errorf("error %q should name the handler that failed", err.Error())
	}
}

func TestHandlers_FailingHandlerDoesNotMaskTheRunsOwnError(t *testing.T) {
	rec := newHandlerRecorder()
	wf := Workflow{
		Steps: []Step{{Id: "doomed", Action: "fail"}},
		Handlers: &Handlers{
			Failure: []Step{{Id: "handler-too", Action: "fail"}},
			Always:  []Step{recordStep("always")},
		},
	}

	err := Run(wf, rec.registry, map[string]any{}, zerolog.Nop())
	if err == nil {
		t.Fatal("expected the run to fail")
	}
	message := err.Error()
	if !strings.Contains(message, "id=doomed") {
		t.Errorf("error %q must still report the run's own failing step", message)
	}
	if !strings.Contains(message, "handlers.failure") {
		t.Errorf("error %q should also report the failed handler", message)
	}
	// Always still runs even though the failure handler blew up.
	if got := strings.Join(rec.ran, ","); got != "always" {
		t.Errorf("ran %q, want always to have run anyway", got)
	}
}

func TestHandlers_AbsentOrEmptyIsANoOp(t *testing.T) {
	for name, handlers := range map[string]*Handlers{
		"absent": nil,
		"empty":  {},
	} {
		t.Run(name, func(t *testing.T) {
			rec := newHandlerRecorder()
			wf := Workflow{Steps: []Step{recordStep("main")}, Handlers: handlers}
			if err := Run(wf, rec.registry, map[string]any{}, zerolog.Nop()); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if got := strings.Join(rec.ran, ","); got != "main" {
				t.Errorf("ran %q, want just main", got)
			}
		})
	}
}

func TestStepError_IsFoundThroughTheWrappedChain(t *testing.T) {
	rec := newHandlerRecorder()
	// A forking step wraps its branch error around the StepError.
	rec.registry.Register(&funcAction{actionType: "fork", run: func(ctx *StepContext) (StepResult, error) {
		return one(map[string]any{"i": 1}, map[string]any{"i": 2}), nil
	}})

	wf := Workflow{Steps: []Step{{Id: "each", Action: "fork"}, {Id: "doomed", Action: "fail"}}}
	err := Run(wf, rec.registry, map[string]any{}, zerolog.Nop())
	if err == nil {
		t.Fatal("expected the run to fail")
	}

	var stepErr *StepError
	if !errors.As(err, &stepErr) {
		t.Fatalf("expected a *StepError in %v", err)
	}
	if stepErr.StepId != "doomed" || stepErr.Action != "fail" {
		t.Errorf("StepError = %+v, want the doomed step", stepErr)
	}
}
