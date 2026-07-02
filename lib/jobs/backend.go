package jobs

// Backend is the storage/orchestration contract for the job queue, analogous
// to JobQueueBackend + JobQueueMin in xtraplatform-jobs. Unlike the Java
// interface's polymorphic push(BaseJob), Go exposes explicit PushJobSet /
// PushJob methods.
type Backend interface {
	IsEnabled() bool

	// PushJobSet stores a new JobSet and, if it declares a setup Job, pushes
	// that onto the queue.
	PushJobSet(js *JobSet) error
	// PushJob enqueues a Job. If untake is true, the job is being re-queued
	// after having been taken (e.g. a retry).
	PushJob(job *Job, untake bool) error

	// Take removes and returns the highest-priority open Job of the given
	// type, marking it started for executor.
	Take(jobType, executor string) (*Job, error)
	// Done marks a taken Job as finished successfully.
	Done(jobID string) error
	// Error marks a taken Job as failed; if retry is true it is re-queued
	// instead.
	Error(jobID, message string, retry bool) error

	GetSets() ([]*JobSet, error)
	GetSet(id string) (*JobSet, error)
	GetOpen(jobType string) ([]*Job, error)
	GetTaken() ([]*Job, error)
	GetFailed() ([]*Job, error)

	// InitJobSet grows JobSet.total by totalDelta and applies the same delta
	// to progressDetails via the declarative updates (Diagram §4).
	InitJobSet(jobSetID string, totalDelta int, updates []ProgressUpdate) error
	// UpdateJobSet grows JobSet.current by currentDelta and applies the same
	// delta to progressDetails via the declarative updates (Diagram §4).
	UpdateJobSet(jobSetID string, currentDelta int, updates []ProgressUpdate) error
	// UpdateJob grows Job.current by currentDelta and, if the Job carries a
	// progress-update descriptor (Diagram §4), fans the same delta out to its
	// JobSet's current/progressDetails - the single generic entry point for
	// worker progress reports.
	UpdateJob(jobID string, currentDelta int) error
}
