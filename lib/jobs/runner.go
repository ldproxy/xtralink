package jobs

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Runner is a polling dispatch loop, analogous to JobRunner.java: for each
// registered JobProcessor's job type (highest priority first) it takes open
// Jobs, executes them concurrently up to Concurrency, and applies the
// returned JobResult (Done/Error) to the Backend. Unlike Java it polls
// instead of reacting to a push notification (no pub/sub in this iteration).
type Runner struct {
	Backend      Backend
	Executor     string
	Concurrency  int
	PollInterval time.Duration
	// OnHoldRetryInterval is how long an OnHold Job waits before being
	// re-queued (Job.Retry semantics reused for this, since PushJob(job,
	// true) is the same untake+requeue used elsewhere). This is a
	// simplified stand-in for Java's event-driven "resource became
	// available" callback, which needs a concrete resource to hook into
	// that this generic Runner doesn't have.
	OnHoldRetryInterval time.Duration
	// OnError receives errors from background job processing that would
	// otherwise be silently dropped (Take/Done/Error/StartJobSet failures).
	OnError func(error)

	processors map[string]JobProcessor
}

func NewRunner(backend Backend, executor string) *Runner {
	return &Runner{
		Backend:             backend,
		Executor:            executor,
		Concurrency:         2,
		PollInterval:        200 * time.Millisecond,
		OnHoldRetryInterval: 2 * time.Second,
		processors:          map[string]JobProcessor{},
	}
}

func (r *Runner) Register(p JobProcessor) {
	r.processors[p.JobType()] = p
}

// Run dispatches jobs until ctx is cancelled, then waits for in-flight jobs
// to finish before returning.
func (r *Runner) Run(ctx context.Context) error {
	sem := make(chan struct{}, r.Concurrency)
	var wg sync.WaitGroup
	types := r.orderedTypes()

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return nil
		default:
		}

		assigned := false
		for _, jobType := range types {
			select {
			case sem <- struct{}{}:
			default:
				continue // at concurrency limit, try the next type
			}

			job, err := r.Backend.Take(jobType, r.Executor)
			if err != nil {
				<-sem
				r.reportError(err)
				continue
			}
			if job == nil {
				<-sem
				continue
			}

			assigned = true
			processor := r.processors[jobType]
			wg.Add(1)
			go func(job *Job, processor JobProcessor) {
				defer wg.Done()
				defer func() { <-sem }()
				r.process(ctx, job, processor)
			}(job, processor)
		}

		if !assigned {
			select {
			case <-ctx.Done():
				wg.Wait()
				return nil
			case <-time.After(r.PollInterval):
			}
		}
	}
}

func (r *Runner) orderedTypes() []string {
	types := make([]string, 0, len(r.processors))
	for t := range r.processors {
		types = append(types, t)
	}
	sort.Slice(types, func(i, j int) bool {
		return r.processors[types[i]].Priority() > r.processors[types[j]].Priority()
	})
	return types
}

// process runs a single Job through its processor and applies the result,
// including the JobSet.start() call for the first non-setup Job of a set
// (Diagram: mirrors handleJobSetStartup in JobRunner.java).
func (r *Runner) process(ctx context.Context, job *Job, processor JobProcessor) {
	var jobSet *JobSet
	if job.PartOf != "" {
		var err error
		jobSet, err = r.Backend.GetSet(job.PartOf)
		r.reportError(err)

		if jobSet != nil && !jobSet.IsStarted() && !(jobSet.Setup != nil && jobSet.Setup.ID == job.ID) {
			r.reportError(r.Backend.StartJobSet(job.PartOf))
		}
	}

	result := processor.Process(job, jobSet, r.Backend)

	switch {
	case result.IsSuccess():
		r.reportError(r.Backend.Done(job.ID))
	case result.IsFailure():
		r.reportError(r.Backend.Error(job.ID, result.Error, result.Retry))
	case result.OnHold:
		r.scheduleOnHoldRetry(ctx, job)
	}
}

// scheduleOnHoldRetry re-queues job after OnHoldRetryInterval, simulating
// "the resource became available again" without an actual event source. It
// respects ctx so it never outlives the Runner it belongs to.
func (r *Runner) scheduleOnHoldRetry(ctx context.Context, job *Job) {
	go func() {
		select {
		case <-time.After(r.OnHoldRetryInterval):
			r.reportError(r.Backend.PushJob(job, true))
		case <-ctx.Done():
		}
	}()
}

func (r *Runner) reportError(err error) {
	if err != nil && r.OnError != nil {
		r.OnError(err)
	}
}
