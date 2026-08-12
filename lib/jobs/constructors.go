package jobs

import (
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
