package jobs

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/ldproxy/xtralink/model"
)

type ProcessFunc func(partialJob *model.PartialJob, job *model.Job, backend Backend) model.JobResult

type JobProcessor struct {
	Kind     string
	Priority int
	Process  ProcessFunc
}

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

	// processors is guarded by mu: Register may be called from another thread
	// while Run is dispatching (the FFI binding registers whenever its consumer
	// gets around to it, before or after Start).
	mu         sync.RWMutex
	processors map[string]*JobProcessor
}

func NewRunner(backend Backend, executor string) *Runner {
	return &Runner{
		Backend:             backend,
		Executor:            executor,
		Concurrency:         2,
		PollInterval:        200 * time.Millisecond,
		OnHoldRetryInterval: -1, //2 * time.Second,
		processors:          make(map[string]*JobProcessor),
	}
}

func NewRunner2() *Runner {
	return &Runner{
		Backend:             nil,
		Executor:            "",
		Concurrency:         2,
		PollInterval:        200 * time.Millisecond,
		OnHoldRetryInterval: -1, //2 * time.Second,
		processors:          make(map[string]*JobProcessor),
	}
}

func (r *Runner) Register(p *JobProcessor) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.processors[p.Kind] = p
}

func (r *Runner) processor(partialJobType string) *JobProcessor {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.processors[partialJobType]
}

// Run dispatches partial jobs until ctx is cancelled, then waits for
// in-flight ones to finish before returning.
func (r *Runner) Run(ctx context.Context) error {
	sem := make(chan struct{}, r.Concurrency)
	var wg sync.WaitGroup

	for {
		// Re-read every pass instead of snapshotting: a processor registered
		// after Run started has to be dispatched too, not ignored until restart.
		types := r.orderedTypes()

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
			processor := r.processor(partialJobType)
			wg.Add(1)
			go func(partialJob *model.PartialJob, processor *JobProcessor) {
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
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]string, 0, len(r.processors))
	for t := range r.processors {
		types = append(types, t)
	}
	sort.Slice(types, func(i, j int) bool {
		return r.processors[types[i]].Priority > r.processors[types[j]].Priority
	})
	return types
}

// process runs a single PartialJob through its processor and applies the
// result, including the Job.start() call for the first non-setup PartialJob
// of a Job (mirrors handleJobSetStartup in JobRunner.java).
func (r *Runner) process(ctx context.Context, partialJob *model.PartialJob, processor *JobProcessor) {
	var job *model.Job
	if partialJob.PartOf != "" {
		var err error
		job, err = r.Backend.GetJob(partialJob.PartOf)
		r.reportError(err)

		if job != nil && !job.IsStarted() && !(job.Setup != nil && job.Setup.Id == partialJob.Id) {
			r.reportError(r.Backend.StartJob(partialJob.PartOf))
		}
	}

	result := processor.Process(partialJob, job, r.Backend)
	//fmt.Printf("JOBS: Processed partial job %s of type %s with result %v\n", partialJob.Id, partialJob.Kind, result)

	switch {
	case result.IsSuccess():
		r.reportError(r.Backend.Done(partialJob.Id))
	case result.IsFailure():
		r.reportError(r.Backend.Error(partialJob.Id, result.Message(), result.Status == model.ResultRETRY))
	case result.IsOnHold():
		r.scheduleOnHoldRetry(ctx, partialJob)
	}
}

// scheduleOnHoldRetry re-queues partialJob after OnHoldRetryInterval,
// simulating "the resource became available again" without an actual event
// source. It respects ctx so it never outlives the Runner it belongs to.
func (r *Runner) scheduleOnHoldRetry(ctx context.Context, partialJob *model.PartialJob) {
	if r.OnHoldRetryInterval <= 0 {
		return
	}
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
