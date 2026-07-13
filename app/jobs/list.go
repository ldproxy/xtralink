package jobs

import (
	"fmt"

	"github.com/ldproxy/xtralink/app"
)

// List returns the compact status view for all known JobSets.
func List(appCtx *app.AppContext) ([]*StatusView, error) {
	sets, err := appCtx.Jobs.GetSets()
	if err != nil {
		return nil, fmt.Errorf("could not list job sets: %w", err)
	}

	views := make([]*StatusView, 0, len(sets))
	for _, js := range sets {
		views = append(views, &StatusView{
			ID:      js.ID,
			Type:    js.Type,
			Status:  js.Status(),
			Percent: js.Percent(),
			Message: js.Message(),
		})
	}
	return views, nil
}
