package tileseedingdemo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/ldproxy/xtrasync/app"
	"github.com/ldproxy/xtrasync/lib/jobs"
)

// Options configures a demo run.
type Options struct {
	Entity       string
	TileSets     []string
	Timeout      time.Duration
	TileDuration time.Duration
	// WithFollowUp attaches a second, independent tile-seeding JobSet as a
	// followUp of the main one (Diagram §2/§4: getFollowUps()), to exercise
	// the followUps-push path in RedisBackend.onJobDone, which nothing else
	// in this demo triggers otherwise.
	WithFollowUp bool
}

// Result is the outcome of a demo Run.
type Result struct {
	JobSet *jobs.JobSet `json:"jobSet"`
	// FollowUp is set only if Options.WithFollowUp was requested; it starts
	// out nil and only becomes populated once the main JobSet's cleanup Job
	// has pushed it (RedisBackend.onJobDone).
	FollowUp *jobs.JobSet `json:"followUp,omitempty"`
}

// Run pushes a simulated tile-seeding JobSet, drives it (and, if requested,
// its followUp) to completion with a Runner registered with
// SetupProcessor/VectorWorkerProcessor, and returns the final state -
// successful/failed, or whatever was reached by the timeout.
func Run(appCtx *app.AppContext, opts Options) (*Result, error) {
	setupDetails, err := json.Marshal(SetupDetails{IsCleanup: false})
	if err != nil {
		return nil, err
	}
	cleanupDetails, err := json.Marshal(SetupDetails{IsCleanup: true})
	if err != nil {
		return nil, err
	}

	newJobSet := func(label string) (*jobs.JobSet, error) {
		inputs, err := json.Marshal(Inputs{TileProvider: opts.Entity, TileSets: opts.TileSets})
		if err != nil {
			return nil, err
		}
		js := jobs.NewJobSet(uuid.NewString(), TypeSet, 1000, label, opts.Entity, inputs)
		js.Setup = jobs.NewJob(uuid.NewString(), TypeSetup, js.Priority, js.ID)
		js.Setup.Details = setupDetails
		js.Cleanup = jobs.NewJob(uuid.NewString(), TypeSetup, js.Priority, js.ID)
		js.Cleanup.Details = cleanupDetails
		return js, nil
	}

	js, err := newJobSet("Tile cache seeding")
	if err != nil {
		return nil, err
	}

	var followUp *jobs.JobSet
	if opts.WithFollowUp {
		followUp, err = newJobSet("Tile cache seeding (follow-up)")
		if err != nil {
			return nil, err
		}
		js.FollowUps = []*jobs.JobSet{followUp}
	}

	if err := appCtx.Jobs.PushJobSet(js); err != nil {
		return nil, fmt.Errorf("could not push demo job set: %w", err)
	}

	runner := jobs.NewRunner(appCtx.Jobs, "demo")
	runner.OnError = func(err error) {
		appCtx.Logger.Error().Err(err).Msg("job runner error")
	}
	runner.Register(SetupProcessor{})
	runner.Register(VectorWorkerProcessor{TileDuration: opts.TileDuration})

	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
	defer cancel()

	runnerDone := make(chan error, 1)
	go func() { runnerDone <- runner.Run(ctx) }()

	result := &Result{}

	main, err := waitForCompletion(ctx, appCtx.Jobs, js.ID, opts.Timeout)
	result.JobSet = main
	if err != nil {
		cancel()
		<-runnerDone
		return result, err
	}

	if opts.WithFollowUp {
		fu, err := waitForCompletion(ctx, appCtx.Jobs, followUp.ID, opts.Timeout)
		result.FollowUp = fu
		if err != nil {
			cancel()
			<-runnerDone
			return result, err
		}
	}

	cancel()
	<-runnerDone
	return result, nil
}

// waitForCompletion polls a JobSet until it is finished. finishedAt is set
// as soon as every sub-Job is done (Diagram: mirrors JobSet.done()), which
// happens *before* the cleanup Job that was just pushed actually runs - so
// this also gives cleanup a brief grace period to write its output before
// treating the run as over. If a permanently failed setup Job forced
// finishedAt instead (RedisBackend.forceFail - no sub-Jobs were ever
// created, so cleanup is never pushed at all), that grace period just
// expires unused and the result is returned without cleanup output.
func waitForCompletion(ctx context.Context, backend jobs.Backend, id string, timeout time.Duration) (*jobs.JobSet, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	const cleanupGracePeriod = 2 * time.Second
	var finishedObservedAt time.Time

	for {
		select {
		case <-ticker.C:
			current, err := backend.GetSet(id)
			if err != nil {
				return nil, err
			}
			if current == nil || current.FinishedAt <= 0 {
				continue
			}
			if finishedObservedAt.IsZero() {
				finishedObservedAt = time.Now()
			}
			cleanupDone := current.Cleanup == nil || len(current.Outputs) > 0
			if cleanupDone || time.Since(finishedObservedAt) > cleanupGracePeriod {
				return current, nil
			}
		case <-ctx.Done():
			final, _ := backend.GetSet(id)
			return final, fmt.Errorf("timed out after %s waiting for job set %s to finish", timeout, id)
		}
	}
}
