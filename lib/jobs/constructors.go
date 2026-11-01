package jobs

import (
	"strings"

	"github.com/google/uuid"

	"github.com/ldproxy/xtralink/model"
)

// NewBaseJob returns a BaseJob in the not-yet-started ("accepted") state.
func NewBaseJob(id, jobType string, priority int) model.BaseJob {
	return *model.NewBaseJob(
		id,
		jobType,
		nowMillis(),
		-1,
		nowMillis(),
		-1,
		priority,
		model.JobProgress{
			Current: 0,
			Total:   0,
			Percent: 0,
		},
		model.StatusACCEPTED,
		[]string{},
		nil,
	)
}

func NewPartialJob(id, jobType string, priority int, partOf string) *model.PartialJob {
	return &model.PartialJob{
		BaseJob: NewBaseJob(id, jobType, priority),
		PartOf:  partOf,
	}
}

func NewJob(id, jobType string, priority int, label string, inputs map[string]any) *model.Job {
	return &model.Job{
		BaseJob:   NewBaseJob(id, jobType, priority),
		Label:     label,
		Inputs:    inputs,
		Outputs:   map[string]any{},
		Setup:     nil,
		Cleanup:   nil,
		FollowUps: []model.Job{},
	}
}

// SetupKind and CleanupKind name the PartialJob a Job's setup and cleanup
// step run as. The kinds follow from the Job's own kind by convention, so a
// JobConfiguration only has to say whether it wants them at all
// (s. NewJobFromConfiguration) - there is nothing to configure and nothing
// that can disagree with the processor registered to handle them.
func SetupKind(jobKind string) string { return jobKind + ":setup" }

func CleanupKind(jobKind string) string { return jobKind + ":cleanup" }

const finalizeSuffix = ":finalize"

// FinalizeKind names the processor that runs when a Job of jobKind is
// removed (s. Runner.DeleteJob). Unlike setup and cleanup it is not opted
// into per Job: only the processor knows whether the Job left anything
// behind that outlives it, so registering a processor for this kind is the
// whole opt-in.
func FinalizeKind(jobKind string) string { return jobKind + finalizeSuffix }

func isFinalizeKind(kind string) bool { return strings.HasSuffix(kind, finalizeSuffix) }

// NewJobFromConfiguration builds the Job a JobConfiguration asks for,
// including its setup and cleanup PartialJobs and, recursively, its
// follow-up Jobs. Every id is assigned here.
//
// Partials stay kinds rather than becoming PartialJobs here: the backend
// queues them only once the Job is ready for them, which for a Job with a
// setup step is when that step succeeds (s. Backend.PushJob).
//
// Setup and Cleanup are booleans on the way in and full PartialJobs on the
// way out, and both halves of that are deliberate: a caller has nothing to
// say about them beyond whether they run, while the Job keeps the whole
// PartialJob because it is the only lasting record of how they went - Done
// deletes the standalone document, and the backend patches this embedded
// copy in its place (s. syncEmbeddedPartialJob).
func NewJobFromConfiguration(cfg model.JobConfiguration) model.Job {
	job := *NewJob(uuid.NewString(), cfg.Kind, cfg.Priority, cfg.Label, cfg.Inputs)
	job.Description = cfg.Description
	job.Context = cfg.Context
	job.Progress = cfg.Progress
	job.TtlSeconds = cfg.TtlSeconds
	job.Partials = cfg.Partials

	if cfg.Sequential {
		job.Sequence = &model.JobSequence{Current: 0, Remaining: 0}
	}

	if cfg.Setup {
		job.Setup = NewPartialJob(uuid.NewString(), SetupKind(cfg.Kind), cfg.Priority, job.Id)
		job.Setup.Progress.Total = 1
	}
	if cfg.Cleanup {
		job.Cleanup = NewPartialJob(uuid.NewString(), CleanupKind(cfg.Kind), cfg.Priority, job.Id)
		job.Cleanup.Progress.Total = 1
	}

	job.FollowUps = make([]model.Job, len(cfg.FollowUps))
	for i, followUp := range cfg.FollowUps {
		job.FollowUps[i] = NewJobFromConfiguration(followUp)
	}

	return job
}

// newPartialJobs builds one PartialJob per kind the Job lists in Partials.
//
// Each one counts as exactly one unit of the Job's total - it either fully
// completes or it doesn't (s. WorkflowJobProcessor, which reports the
// matching +1 on success) - so whoever queues them grows that total by
// len(result) before queueing the first. Growing it one PartialJob at a
// time instead would let an early one finish while current == total and
// finalize a Job whose remaining parts are not even queued yet.
func newPartialJobs(job *model.Job) []*model.PartialJob {
	partialJobs := make([]*model.PartialJob, len(job.Partials))
	for i, kind := range job.Partials {
		partialJobs[i] = NewPartialJob(uuid.NewString(), kind, job.Priority, job.Id)
		partialJobs[i].Progress.Total = 1
	}
	return partialJobs
}
