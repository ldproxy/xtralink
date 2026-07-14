//go:build demo

package counterdemo

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/jobs"
)

// Options configures a demo run.
type Options struct {
	Steps        int
	StepDuration time.Duration
	FailAt       int
	Timeout      time.Duration
}

// Run pushes a Job with no Setup/Cleanup - just a single PartialJob, pushed
// directly via Backend.PushPartialJob, since PushJob only auto-pushes a
// Setup PartialJob (which this job type doesn't have) - and drives it to
// completion with a Runner registered with CounterProcessor.
func Run(appCtx *app.AppContext, opts Options) (*jobs.Job, error) {
	job := jobs.NewJob(uuid.NewString(), Type, 1000, "Counter demo", nil)
	if err := appCtx.Jobs.PushJob(job); err != nil {
		return nil, fmt.Errorf("could not push job: %w", err)
	}

	partialJob := jobs.NewPartialJob(uuid.NewString(), Type, job.Priority, job.ID)
	partialJob.Total = opts.Steps
	if err := appCtx.Jobs.InitJob(job.ID, opts.Steps, nil); err != nil {
		return nil, fmt.Errorf("could not init job total: %w", err)
	}
	if err := appCtx.Jobs.PushPartialJob(partialJob, false); err != nil {
		return nil, fmt.Errorf("could not push partial job: %w", err)
	}

	runner := jobs.NewRunner(appCtx.Jobs, "demo")
	runner.OnError = func(err error) {
		appCtx.Logger.Error().Err(err).Msg("job runner error")
	}
	runner.Register(CounterProcessor{StepDuration: opts.StepDuration, FailAt: opts.FailAt})

	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
	defer cancel()

	runnerDone := make(chan error, 1)
	go func() { runnerDone <- runner.Run(ctx) }()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			current, err := appCtx.Jobs.GetJob(job.ID)
			if err != nil {
				return nil, err
			}
			// No Cleanup PartialJob exists for this job type, so unlike
			// tileseedingdemo there is no extra step to wait for once
			// finishedAt is set.
			if current != nil && current.FinishedAt > 0 {
				cancel()
				<-runnerDone
				return current, nil
			}
		case <-ctx.Done():
			<-runnerDone
			final, _ := appCtx.Jobs.GetJob(job.ID)
			return final, fmt.Errorf("timed out after %s waiting for job to finish", opts.Timeout)
		}
	}
}
