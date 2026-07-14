package jobs

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/jobs"
)

// Push builds a new Job from CLI input and pushes it onto the queue. A
// caller pushes a Job (the "order"), not a raw PartialJob.
func Push(appCtx *app.AppContext, jobType, label string, priority int, inputsRaw string) (*jobs.Job, error) {
	var inputs json.RawMessage
	if inputsRaw != "" {
		if !json.Valid([]byte(inputsRaw)) {
			return nil, fmt.Errorf("inputs is not valid json: %s", inputsRaw)
		}
		inputs = json.RawMessage(inputsRaw)
	}

	job := jobs.NewJob(uuid.NewString(), jobType, priority, label, inputs)

	if err := appCtx.Jobs.PushJob(job); err != nil {
		return nil, fmt.Errorf("could not push job: %w", err)
	}

	return job, nil
}
