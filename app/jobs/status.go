package jobs

import (
	"fmt"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/model"
)

// StatusView is the compact status/progress view for a Job. It is a view
// type deliberately decoupled from model.Job, so its field/JSON names stay
// stable regardless of what the model calls them.
type StatusView struct {
	ID      string       `json:"id"`
	Type    string       `json:"type"`
	Status  model.Status `json:"status"`
	Percent int          `json:"percent"`
	Message string       `json:"message"`
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
		ID:      job.Id,
		Type:    job.Kind,
		Status:  job.Status(),
		Percent: job.Percent(),
		Message: job.Message(),
	}, nil
}
