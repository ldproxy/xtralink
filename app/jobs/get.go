package jobs

import (
	"fmt"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/jobs"
)

// Get returns the full JobSet (inputs/outputs/progressDetails included).
func Get(appCtx *app.AppContext, id string) (*jobs.JobSet, error) {
	js, err := appCtx.Jobs.GetSet(id)
	if err != nil {
		return nil, fmt.Errorf("could not get job set %s: %w", id, err)
	}
	if js == nil {
		return nil, fmt.Errorf("job set not found: %s", id)
	}
	return js, nil
}
