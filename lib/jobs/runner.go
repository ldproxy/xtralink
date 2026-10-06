package jobs

import (
	"context"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/ldproxy/xtralink/model"
	"github.com/rs/zerolog"
)

type ProcessFunc func(partialJob *model.PartialJob, job *model.Job, backend Backend) model.JobResult

type JobProcessor struct {
	Kind     string
	Priority int
	Process  ProcessFunc
	// OrphanTimeout opts this processor's PartialJobs into being reclaimed
	// when they have been taken but not updated for that long, which means
	// their executor died mid-run. Zero (the default) never reclaims.
	//
	// It is opt-in because PartialJob.UpdatedAt is only a liveness signal
	// for a processor that reports progress regularly - a processor that
	// legitimately works for minutes without an update would be reclaimed
	// while still running. Java restricts its reaper to one hardcoded
	// PartialJob type for exactly this reason; this is the generic form of
	// that whitelist.
	OrphanTimeout time.Duration
}

// Runner is a polling dispatch loop, analogous to JobRunner.java: for each
// registered JobProcessor's PartialJob type (highest priority first) it
// takes open PartialJobs, executes them concurrently up to Concurrency, and
// applies the returned JobResult (Done/Error) to the Backend. Unlike Java it
// polls instead of reacting to a push notification (no pub/sub in this
// iteration).
//
// Alongside dispatch it runs the housekeeping sweeps from Java's
// checkOrphanedJobsAndCleanup: reclaiming orphaned PartialJobs and removing
// long-finished Jobs. See docs/jobs/runner-vs-jobrunner.md for the full
// list of deltas against the Java original.
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
	// HousekeepingInterval is how often the orphan and cleanup sweeps run
	// (Java schedules the same pair every minute). Zero or less disables
	// both.
	HousekeepingInterval time.Duration
	// JobRetention is how long a finished Job is kept before being removed
	// (Java: one hour). Zero removes it at the first sweep after it
	// finishes; a negative value keeps finished Jobs indefinitely.
	JobRetention time.Duration
	// OnError receives errors from background job processing that would
	// otherwise be silently dropped (Take/Done/Error/StartJob failures).
	OnError func(error)
	// Logger receives debug/trace output about dispatch and housekeeping.
	// Defaults to a no-op logger.
	Logger zerolog.Logger

	// processors is guarded by mu: Register may be called from another thread
	// while Run is dispatching (the FFI binding registers whenever its consumer
	// gets around to it, before or after Start).
	mu         sync.RWMutex
	processors map[string]*JobProcessor

	// inFlight holds the ids of the PartialJobs this Runner is processing
	// right now, so the orphan sweep never reclaims its own work.
	inFlightMu sync.Mutex
	inFlight   map[string]bool
}

func NewRunner(backend Backend, executor string) *Runner {
	return &Runner{
		Backend:              backend,
		Executor:             executor,
		Concurrency:          2,
		PollInterval:         200 * time.Millisecond,
		OnHoldRetryInterval:  -1, //2 * time.Second,
		HousekeepingInterval: time.Minute,
		JobRetention:         time.Hour,
		Logger:               zerolog.Nop(),
		processors:           make(map[string]*JobProcessor),
		inFlight:             make(map[string]bool),
	}
}

func NewRunner2() *Runner {
	return &Runner{
		Backend:              nil,
		Executor:             "",
		Concurrency:          2,
		PollInterval:         200 * time.Millisecond,
		OnHoldRetryInterval:  -1, //2 * time.Second,
		HousekeepingInterval: time.Minute,
		JobRetention:         time.Hour,
		Logger:               zerolog.Nop(),
		processors:           make(map[string]*JobProcessor),
		inFlight:             make(map[string]bool),
	}
}

func (r *Runner) Register(p *JobProcessor) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.processors[p.Kind] = p
	r.Logger.Debug().Str("kind", p.Kind).Int("priority", p.Priority).Dur("orphanTimeout", p.OrphanTimeout).Msg("processor registered")
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

	r.Logger.Debug().Str("executor", r.Executor).Int("concurrency", r.Concurrency).
		Dur("pollInterval", r.PollInterval).Dur("housekeepingInterval", r.HousekeepingInterval).
		Msg("runner started")

	if r.HousekeepingInterval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.housekeep(ctx)
		}()
	}

	for {
		// Re-read every pass instead of snapshotting: a processor registered
		// after Run started has to be dispatched too, not ignored until restart.
		types := r.orderedTypes()

		select {
		case <-ctx.Done():
			r.Logger.Debug().Msg("runner stopping, waiting for in-flight partial jobs")
			wg.Wait()
			r.Logger.Debug().Msg("runner stopped")
			return nil
		default:
		}

		assigned := false
		for _, partialJobType := range types {
			select {
			case sem <- struct{}{}:
			default:
				r.Logger.Trace().Str("kind", partialJobType).Msg("at concurrency limit, skipping take")
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
			r.Logger.Debug().Str("partialJob", partialJob.Id).Str("kind", partialJobType).Str("job", partialJob.PartOf).Msg("partial job taken")
			processor := r.processor(partialJobType)
			r.markInFlight(partialJob.Id)
			wg.Add(1)
			go func(partialJob *model.PartialJob, processor *JobProcessor) {
				defer wg.Done()
				defer func() { <-sem }()
				defer r.clearInFlight(partialJob.Id)
				r.process(ctx, partialJob, processor)
			}(partialJob, processor)
		}

		if !assigned {
			r.Logger.Trace().Strs("kinds", types).Msg("nothing to take, sleeping")
			select {
			case <-ctx.Done():
				r.Logger.Debug().Msg("runner stopping, waiting for in-flight partial jobs")
				wg.Wait()
				r.Logger.Debug().Msg("runner stopped")
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
			r.Logger.Debug().Str("job", partialJob.PartOf).Msg("starting job")
			r.reportError(r.Backend.StartJob(partialJob.PartOf))
		}
	}

	started := time.Now()
	result := r.invoke(partialJob, job, processor)
	r.Logger.Debug().Str("partialJob", partialJob.Id).Str("kind", partialJob.Kind).
		Str("status", string(result.Status)).Str("message", result.Message()).
		Dur("duration", time.Since(started)).Msg("partial job processed")

	switch {
	case result.IsSuccess():
		r.reportError(r.Backend.Done(partialJob.Id))
	case result.IsFailure():
		r.reportError(r.Backend.Error(partialJob.Id, result.Message(), result.Status == model.ResultRETRY))
	case result.IsOnHold():
		r.scheduleOnHoldRetry(ctx, partialJob)
	}
}

// invoke calls the processor and turns a panic into a non-retrying failure,
// mirroring executeJob in JobRunner.java, which catches Throwable and
// returns JobResult.error. Without this a single misbehaving processor takes
// the whole process down with it.
func (r *Runner) invoke(partialJob *model.PartialJob, job *model.Job, processor *JobProcessor) (result model.JobResult) {
	defer func() {
		if rec := recover(); rec != nil {
			r.Logger.Debug().Str("partialJob", partialJob.Id).Interface("panic", rec).Msg("processor panicked")
			result = model.Error(fmt.Sprintf("panic while processing partial job %s: %v\n%s", partialJob.Id, rec, debug.Stack()))
		}
	}()

	return processor.Process(partialJob, job, r.Backend)
}

// scheduleOnHoldRetry re-queues partialJob after OnHoldRetryInterval,
// simulating "the resource became available again" without an actual event
// source. It respects ctx so it never outlives the Runner it belongs to.
func (r *Runner) scheduleOnHoldRetry(ctx context.Context, partialJob *model.PartialJob) {
	if r.OnHoldRetryInterval <= 0 {
		r.Logger.Debug().Str("partialJob", partialJob.Id).Msg("partial job on hold, retry disabled")
		return
	}
	r.Logger.Debug().Str("partialJob", partialJob.Id).Dur("retryIn", r.OnHoldRetryInterval).Msg("partial job on hold, retry scheduled")
	go func() {
		select {
		case <-time.After(r.OnHoldRetryInterval):
			r.Logger.Debug().Str("partialJob", partialJob.Id).Msg("re-queueing on-hold partial job")
			r.reportError(r.Backend.PushPartialJob(partialJob, true))
		case <-ctx.Done():
		}
	}()
}

// housekeep runs the periodic sweeps until ctx is cancelled, mirroring
// checkOrphanedJobsAndCleanup in JobRunner.java.
func (r *Runner) housekeep(ctx context.Context) {
	ticker := time.NewTicker(r.HousekeepingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.Logger.Trace().Msg("housekeeping sweep")
			r.reapOrphans()
			r.cleanupFinishedJobs()
		}
	}
}

// reapOrphans re-queues PartialJobs that were taken but have not reported
// progress within their processor's OrphanTimeout - their executor died
// mid-run, and without this they stay taken forever.
func (r *Runner) reapOrphans() {
	taken, err := r.Backend.GetTaken()
	if err != nil {
		r.reportError(err)
		return
	}

	now := nowMillis()
	for _, partialJob := range taken {
		processor := r.processor(partialJob.Kind)
		if processor == nil || processor.OrphanTimeout <= 0 {
			continue
		}
		if r.isInFlight(partialJob.Id) {
			continue
		}
		if partialJob.UpdatedAt <= 0 || now-partialJob.UpdatedAt <= processor.OrphanTimeout.Milliseconds() {
			continue
		}

		r.Logger.Debug().Str("partialJob", partialJob.Id).Str("kind", partialJob.Kind).Str("executor", partialJob.Executor).
			Msg("reclaiming orphaned partial job")
		r.reportError(r.Backend.PushPartialJob(partialJob, true))
	}
}

// cleanupFinishedJobs removes Jobs that have been finished and idle for
// longer than their retention (mirrors cleanupOldJobSets in
// JobRunner.java). The gate is FinishedAt rather than Java's isDone(): a Job
// force-failed by a failing setup step never reaches current == total, so
// Java's gate would leak it forever.
func (r *Runner) cleanupFinishedJobs() {
	jobs, err := r.Backend.GetJobs()
	if err != nil {
		r.reportError(err)
		return
	}

	now := nowMillis()
	for _, job := range jobs {
		retention := r.retentionFor(job)
		if retention < 0 {
			continue
		}
		if job.FinishedAt > 0 && now-job.UpdatedAt >= retention.Milliseconds() {
			r.Logger.Debug().Str("job", job.Id).Dur("retention", retention).Msg("removing finished job")
			r.reportError(r.Backend.DoneJob(job.Id))
		}
	}
}

// retentionFor resolves how long job is kept after it finished: its own
// TtlSeconds if it carries one, otherwise the Runner-wide JobRetention. An
// explicit TTL wins in both directions, so it also applies when the default
// keeps Jobs indefinitely - and, like JobRetention, zero means "remove as
// soon as it has finished" while a negative value means "keep forever".
func (r *Runner) retentionFor(job *model.Job) time.Duration {
	if job.TtlSeconds != nil {
		return time.Duration(*job.TtlSeconds) * time.Second
	}
	return r.JobRetention
}

func (r *Runner) markInFlight(partialJobID string) {
	r.inFlightMu.Lock()
	defer r.inFlightMu.Unlock()

	if r.inFlight == nil {
		r.inFlight = make(map[string]bool)
	}
	r.inFlight[partialJobID] = true
}

func (r *Runner) clearInFlight(partialJobID string) {
	r.inFlightMu.Lock()
	defer r.inFlightMu.Unlock()

	delete(r.inFlight, partialJobID)
}

func (r *Runner) isInFlight(partialJobID string) bool {
	r.inFlightMu.Lock()
	defer r.inFlightMu.Unlock()

	return r.inFlight[partialJobID]
}

func (r *Runner) reportError(err error) {
	if err != nil && r.OnError != nil {
		r.OnError(err)
	}
}
