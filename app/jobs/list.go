package jobs

import (
	"fmt"

	"github.com/ldproxy/xtralink/app"
)

// List returns the compact status view for all known Jobs.
func List(appCtx *app.AppContext) ([]*StatusView, error) {
	all, err := appCtx.Jobs.GetJobs()
	if err != nil {
		return nil, fmt.Errorf("could not list jobs: %w", err)
	}

	views := make([]*StatusView, 0, len(all))
	for _, job := range all {
		views = append(views, &StatusView{
			ID:      job.ID,
			Type:    job.Type,
			Status:  job.Status(),
			Percent: job.Percent(),
			Message: job.Message(),
		})
	}
	return views, nil
}
