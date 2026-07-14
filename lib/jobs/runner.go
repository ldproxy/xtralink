package jobs

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Runner is a polling dispatch loop, analogous to JobRunner.java: for each
// registered JobProcessor's PartialJob type (highest priority first) it
// takes open PartialJobs, executes them concurrently up to Concurrency, and
// applies the returned JobResult (Done/Error) to the Backend. Unlike Java it
// polls instead of reacting to a push notification (no pub/sub in this
// iteration).
type Runner struct {
	Backend      Backend
	Executor     string
	Concurrency  int
	PollInterval time.Duration
	// OnHoldRetryInterval is how long an OnHold PartialJob waits before
	// being re-queued (PartialJob.Retry semantics reused for this, since
	// PushPartialJob(partialJob, true) is the same untake+requeue used
	// elsewhere). This is a simplified stand-in for Java's event-driven
	// "resource became available" callback, which needs a concrete
	// resource to hook into that this generic Runner doesn't have.
	OnHoldRetryInterval time.Duration
	// OnError receives errors from background job processing that would
	// otherwise be silently dropped (Take/Done/Error/StartJob failures).
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

// Run dispatches partial jobs until ctx is cancelled, then waits for
// in-flight ones to finish before returning.
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
		for _, partialJobType := range types {
			select {
			case sem <- struct{}{}:
			default:
				continue // at concurrency limit, try the next type
			}

			partialJob, err := r.Backend.Take(partialJobType, r.Executor)
			if err != nil {
				<-sem
				r.reportError(err)
				continue
			}
			if partialJob == nil {
				<-sem
				continue
			}

			assigned = true
			processor := r.processors[partialJobType]
			wg.Add(1)
			go func(partialJob *PartialJob, processor JobProcessor) {
				defer wg.Done()
				defer func() { <-sem }()
				r.process(ctx, partialJob, processor)
			}(partialJob, processor)
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

// process runs a single PartialJob through its processor and applies the
// result, including the Job.start() call for the first non-setup PartialJob
// of a Job (mirrors handleJobSetStartup in JobRunner.java).
func (r *Runner) process(ctx context.Context, partialJob *PartialJob, processor JobProcessor) {
	var job *Job
	if partialJob.PartOf != "" {
		var err error
		job, err = r.Backend.GetJob(partialJob.PartOf)
		r.reportError(err)

		if job != nil && !job.IsStarted() && !(job.Setup != nil && job.Setup.ID == partialJob.ID) {
			r.reportError(r.Backend.StartJob(partialJob.PartOf))
		}
	}

	result := processor.Process(partialJob, job, r.Backend)

	switch {
	case result.IsSuccess():
		r.reportError(r.Backend.Done(partialJob.ID))
	case result.IsFailure():
		r.reportError(r.Backend.Error(partialJob.ID, result.Error, result.Retry))
	case result.OnHold:
		r.scheduleOnHoldRetry(ctx, partialJob)
	}
}

// scheduleOnHoldRetry re-queues partialJob after OnHoldRetryInterval,
// simulating "the resource became available again" without an actual event
// source. It respects ctx so it never outlives the Runner it belongs to.
func (r *Runner) scheduleOnHoldRetry(ctx context.Context, partialJob *PartialJob) {
	go func() {
		select {
		case <-time.After(r.OnHoldRetryInterval):
			r.reportError(r.Backend.PushPartialJob(partialJob, true))
		case <-ctx.Done():
		}
	}()
}

func (r *Runner) reportError(err error) {
	if err != nil && r.OnError != nil {
		r.OnError(err)
	}
}
