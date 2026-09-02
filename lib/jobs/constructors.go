package jobs

import (
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

// NewJobFromConfiguration builds the Job a JobConfiguration asks for,
// including its setup and cleanup PartialJobs and, recursively, its
// follow-up Jobs. Every id is assigned here.
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
