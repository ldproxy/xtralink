//go:build demo

package counterdemo

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/ldproxy/xtrasync/app"
	"github.com/ldproxy/xtrasync/lib/jobs"
)

// Options configures a demo run.
type Options struct {
	Steps        int
	StepDuration time.Duration
	FailAt       int
	Timeout      time.Duration
}

// Run pushes a JobSet with no Setup/Cleanup - just a single Job, pushed
// directly via Backend.PushJob, since PushJobSet only auto-pushes a Setup
// Job (which this job type doesn't have) - and drives it to completion with
// a Runner registered with CounterProcessor.
func Run(appCtx *app.AppContext, opts Options) (*jobs.JobSet, error) {
	js := jobs.NewJobSet(uuid.NewString(), Type, 1000, "Counter demo", "", nil)
	if err := appCtx.Jobs.PushJobSet(js); err != nil {
		return nil, fmt.Errorf("could not push job set: %w", err)
	}

	job := jobs.NewJob(uuid.NewString(), Type, js.Priority, js.ID)
	job.Total = opts.Steps
	if err := appCtx.Jobs.InitJobSet(js.ID, opts.Steps, nil); err != nil {
		return nil, fmt.Errorf("could not init job set total: %w", err)
	}
	if err := appCtx.Jobs.PushJob(job, false); err != nil {
		return nil, fmt.Errorf("could not push job: %w", err)
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
			current, err := appCtx.Jobs.GetSet(js.ID)
			if err != nil {
				return nil, err
			}
			// No Cleanup Job exists for this job type, so unlike
			// tileseedingdemo there is no extra step to wait for once
			// finishedAt is set.
			if current != nil && current.FinishedAt > 0 {
				cancel()
				<-runnerDone
				return current, nil
			}
		case <-ctx.Done():
			<-runnerDone
			final, _ := appCtx.Jobs.GetSet(js.ID)
			return final, fmt.Errorf("timed out after %s waiting for job set to finish", opts.Timeout)
		}
	}
}
