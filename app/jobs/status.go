package jobs

import (
	"fmt"

	"github.com/ldproxy/xtrasync/app"
	"github.com/ldproxy/xtrasync/lib/jobs"
)

// StatusView is the compact status/progress view for a JobSet.
type StatusView struct {
	ID      string      `json:"id"`
	Type    string      `json:"type"`
	Status  jobs.Status `json:"status"`
	Percent int         `json:"percent"`
	Message string      `json:"message"`
}

// Status looks up a JobSet by id and returns its derived status.
func Status(appCtx *app.AppContext, id string) (*StatusView, error) {
	js, err := appCtx.Jobs.GetSet(id)
	if err != nil {
		return nil, fmt.Errorf("could not get job set %s: %w", id, err)
	}
	if js == nil {
		return nil, fmt.Errorf("job set not found: %s", id)
	}

	return &StatusView{
		ID:      js.ID,
		Type:    js.Type,
		Status:  js.Status(),
		Percent: js.Percent(),
		Message: js.Message(),
	}, nil
}
