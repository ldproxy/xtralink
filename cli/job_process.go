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
	Id string `arg:"" help:"Job step id to process, or \"*\" for every configured step"`
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
	stepIds, err := stepIdsToProcess(appCtx, c.Id)
	if err != nil {
		return err
	}

	runner := libjobs.NewRunner(appCtx.Jobs, executorId())
	runner.Concurrency = appCtx.Settings.Jobs.MaxConcurrent
	runner.OnError = func(err error) {
		appCtx.Logger.Error().Err(err).Msg("job runner error")
	}

	for _, stepId := range stepIds {
		processor, err := appworkflows.NewWorkflowJobProcessor(appCtx, stepId)
		if err != nil {
			return err
		}
		runner.Register(processor)
	}

	appCtx.Logger.Info().Strs("steps", stepIds).Int("concurrency", runner.Concurrency).Msg("job runner starting")
	if err := runner.Run(ctx); err != nil {
		return err
	}
	appCtx.Logger.Info().Msg("job runner stopped")
	return nil
}

// stepIdsToProcess resolves id ("*" for every step across every configured
// JobDefinition, or one specific step id) into the PartialJob types job
// process should register a WorkflowJobProcessor for.
func stepIdsToProcess(appCtx *app.AppContext, id string) ([]string, error) {
	if id != "*" {
		if _, _, err := appCtx.Settings.GetJobStep(id); err != nil {
			return nil, err
		}
		return []string{id}, nil
	}

	var ids []string
	for _, def := range appCtx.Settings.JobDefinitions {
		for _, step := range def.Steps {
			ids = append(ids, step.Id)
		}
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("no jobDefinitions configured")
	}
	return ids, nil
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
