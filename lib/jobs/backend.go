package jobs

import (
	"github.com/ldproxy/xtralink/model"
)

// Backend is the storage/orchestration contract for the job queue, analogous
// to JobQueueBackend + JobQueueMin in xtraplatform-jobs. Unlike the Java
// interface's polymorphic push(BaseJob), Go exposes explicit PushJob /
// PushPartialJob methods.
type Backend interface {
	IsEnabled() bool

	// PushJob stores a new Job and, if it declares a setup PartialJob,
	// pushes that onto the queue.
	PushJob(job *model.Job) error
	// PushPartialJob enqueues a PartialJob. If untake is true, the partial
	// job is being re-queued after having been taken (e.g. a retry).
	PushPartialJob(partialJob *model.PartialJob, untake bool) error

	// Take removes and returns the highest-priority open PartialJob of the
	// given type, marking it started for executor.
	Take(partialJobType, executor string) (*model.PartialJob, error)
	// Done marks a taken PartialJob as finished successfully. If the
	// PartialJob belongs to a Job, this also runs the setup/cleanup/
	// followUps decision (mirrors JobSet.done(job) in Java): a finishing
	// regular PartialJob may push the cleanup PartialJob once the Job is
	// done, and a finishing cleanup PartialJob pushes the Job's followUps.
	Done(partialJobID string) error
	// Error marks a taken PartialJob as failed; if retry is true it is
	// re-queued instead.
	Error(partialJobID, message string, retry bool) error

	GetJobs() ([]*model.Job, error)
	GetJob(id string) (*model.Job, error)
	GetOpen(partialJobType string) ([]*model.PartialJob, error)
	GetTaken() ([]*model.PartialJob, error)
	GetFailed() ([]*model.PartialJob, error)

	// StartJob sets Job.startedAt to now, if not already started (mirrors
	// JobSet.start() in Java). Called by the Runner for the first
	// non-setup PartialJob of a Job that gets taken.
	StartJob(jobID string) error
	// SetProgressDetails overwrites Job.progressDetails wholesale. This is
	// the one-time, type-specific initial build done by a setup step;
	// ongoing per-delta updates go through InitJob/UpdateJob/
	// UpdatePartialJob instead.
	SetProgressDetails(jobID string, details map[string]any) error
	// SetOutput writes a single Job.outputs entry - typically called once
	// by a cleanup step to publish its result.
	SetOutput(jobID, key string, value model.OutputValue) error

	// InitJob grows Job.total by totalDelta and applies the same delta to
	// progressDetails via the declarative updates.
	InitJob(jobID string, totalDelta int, updates []model.ProgressUpdate) error
	// UpdateJob grows Job.current by currentDelta and applies the same
	// delta to progressDetails via the declarative updates. Only for a Job
	// with no PartialJobs of its own - a PartialJob's worker reports
	// through UpdatePartialJob, which already covers the Job level.
	UpdateJob(jobID string, currentDelta int, updates []model.ProgressUpdate) error
	// UpdatePartialJob grows PartialJob.current by currentDelta and fans the
	// same delta out to its Job's current, plus the progressDetails paths
	// the PartialJob's progress-update descriptor declares (if any) - the
	// single generic entry point for worker progress reports.
	UpdatePartialJob(partialJobID string, currentDelta int) error
}
