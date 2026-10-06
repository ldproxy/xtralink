package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/ldproxy/xtralink/app"
	appworkflows "github.com/ldproxy/xtralink/app/workflows"
	libjobs "github.com/ldproxy/xtralink/lib/jobs"
)

type JobProcessCmd struct {
	Kind string `arg:"" help:"PartialJob kind to process, or \"*\" for every kind configured under jobs:"`
}

func (c *JobProcessCmd) Run(appCtx *app.AppContext) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	return c.run(appCtx, ctx)
}

// run holds the actual logic, taking ctx as a parameter (rather than
// building it from OS signals itself) so tests can drive/cancel it
// directly instead of having to send a real signal to the whole test
// process.
func (c *JobProcessCmd) run(appCtx *app.AppContext, ctx context.Context) error {
	return RunJobWorkers(appCtx, ctx, c.Kind)
}

// RunJobWorkers registers a WorkflowJobProcessor for kind ("*" for every
// kind configured under jobs:) and processes queued PartialJobs until ctx
// is cancelled. Shared by `job process` and by `flow run --workers`, which
// is the same thing in the same process (s. FlowRunCmd).
func RunJobWorkers(appCtx *app.AppContext, ctx context.Context, kind string) error {
	kinds, err := kindsToProcess(appCtx, kind)
	if err != nil {
		return err
	}

	runner := libjobs.NewRunner(appCtx.Jobs, executorId())
	runner.Concurrency = appCtx.Settings.General.MaxConcurrent
	runner.Logger = appCtx.Logger.With().Str("component", "jobs").Logger()
	runner.OnError = func(err error) {
		appCtx.Logger.Error().Err(err).Msg("job runner error")
	}

	for _, kind := range kinds {
		processor, err := appworkflows.WorkflowJobProcessor(appCtx, kind)
		if err != nil {
			return err
		}
		runner.Register(processor)
	}

	appCtx.Logger.Info().Strs("kinds", kinds).Int("concurrency", runner.Concurrency).Msg("job runner starting")
	if err := runner.Run(ctx); err != nil {
		return err
	}
	appCtx.Logger.Info().Msg("job runner stopped")
	return nil
}

// kindsToProcess resolves the command's argument ("*" for every configured
// JobDefinition, or one specific kind) into the PartialJob kinds job
// process should register a WorkflowJobProcessor for.
func kindsToProcess(appCtx *app.AppContext, kind string) ([]string, error) {
	if kind != "*" {
		if _, err := appCtx.Settings.GetJobDefinition(kind); err != nil {
			return nil, err
		}
		return []string{kind}, nil
	}

	var kinds []string
	for _, def := range appCtx.Settings.JobDefinitions {
		kinds = append(kinds, def.Kind)
	}
	if len(kinds) == 0 {
		return nil, fmt.Errorf("no jobs configured")
	}
	return kinds, nil
}

// executorId identifies this Runner instance to the Backend (e.g. shown as
// PartialJob.Executor) - host+pid is enough to tell separate job process
// instances apart without needing any external coordination.
func executorId() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + "-" + strconv.Itoa(os.Getpid())
}
