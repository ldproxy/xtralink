package jobs

// Backend is the storage/orchestration contract for the job queue, analogous
// to JobQueueBackend + JobQueueMin in xtraplatform-jobs. Unlike the Java
// interface's polymorphic push(BaseJob), Go exposes explicit PushJob /
// PushPartialJob methods.
type Backend interface {
	IsEnabled() bool

	// PushJob stores a new Job and, if it declares a setup PartialJob,
	// pushes that onto the queue.
	PushJob(job *Job) error
	// PushPartialJob enqueues a PartialJob. If untake is true, the partial
	// job is being re-queued after having been taken (e.g. a retry).
	PushPartialJob(partialJob *PartialJob, untake bool) error

	// Take removes and returns the highest-priority open PartialJob of the
	// given type, marking it started for executor.
	Take(partialJobType, executor string) (*PartialJob, error)
	// Done marks a taken PartialJob as finished successfully. If the
	// PartialJob belongs to a Job, this also runs the setup/cleanup/
	// followUps decision (mirrors JobSet.done(job) in Java): a finishing
	// regular PartialJob may push the cleanup PartialJob once the Job is
	// done, and a finishing cleanup PartialJob pushes the Job's followUps.
	Done(partialJobID string) error
	// Error marks a taken PartialJob as failed; if retry is true it is
	// re-queued instead.
	Error(partialJobID, message string, retry bool) error

	GetJobs() ([]*Job, error)
	GetJob(id string) (*Job, error)
	GetOpen(partialJobType string) ([]*PartialJob, error)
	GetTaken() ([]*PartialJob, error)
	GetFailed() ([]*PartialJob, error)

	// StartJob sets Job.startedAt to now, if not already started (mirrors
	// JobSet.start() in Java). Called by the Runner for the first
	// non-setup PartialJob of a Job that gets taken.
	StartJob(jobID string) error
	// SetProgressDetails overwrites Job.progressDetails wholesale. This is
	// the one-time, type-specific initial build done by a setup step;
	// ongoing per-delta updates go through InitJob/UpdateJob/
	// UpdatePartialJob instead.
	SetProgressDetails(jobID string, details any) error
	// SetOutput writes a single Job.outputs entry - typically called once
	// by a cleanup step to publish its result.
	SetOutput(jobID, key string, value OutputValue) error

	// InitJob grows Job.total by totalDelta and applies the same delta to
	// progressDetails via the declarative updates.
	InitJob(jobID string, totalDelta int, updates []ProgressUpdate) error
	// UpdateJob grows Job.current by currentDelta and applies the same
	// delta to progressDetails via the declarative updates.
	UpdateJob(jobID string, currentDelta int, updates []ProgressUpdate) error
	// UpdatePartialJob grows PartialJob.current by currentDelta and, if the
	// PartialJob carries a progress-update descriptor, fans the same delta
	// out to its Job's current/progressDetails - the single generic entry
	// point for worker progress reports.
	UpdatePartialJob(partialJobID string, currentDelta int) error
}
