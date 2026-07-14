package jobs

import (
	"fmt"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/jobs"
)

// Get returns the full Job (inputs/outputs/progressDetails included).
func Get(appCtx *app.AppContext, id string) (*jobs.Job, error) {
	job, err := appCtx.Jobs.GetJob(id)
	if err != nil {
		return nil, fmt.Errorf("could not get job %s: %w", id, err)
	}
	if job == nil {
		return nil, fmt.Errorf("job not found: %s", id)
	}
	return job, nil
}
