package jobs

import (
	"fmt"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/jobs"
)

// StatusView is the compact status/progress view for a Job.
type StatusView struct {
	ID      string      `json:"id"`
	Type    string      `json:"type"`
	Status  jobs.Status `json:"status"`
	Percent int         `json:"percent"`
	Message string      `json:"message"`
}

// Status looks up a Job by id and returns its derived status.
func Status(appCtx *app.AppContext, id string) (*StatusView, error) {
	job, err := appCtx.Jobs.GetJob(id)
	if err != nil {
		return nil, fmt.Errorf("could not get job %s: %w", id, err)
	}
	if job == nil {
		return nil, fmt.Errorf("job not found: %s", id)
	}

	return &StatusView{
		ID:      job.ID,
		Type:    job.Type,
		Status:  job.Status(),
		Percent: job.Percent(),
		Message: job.Message(),
	}, nil
}
