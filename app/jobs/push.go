package jobs

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/ldproxy/xtrasync/app"
	"github.com/ldproxy/xtrasync/lib/jobs"
)

// Push builds a new JobSet from CLI input and pushes it onto the queue. A
// caller pushes a JobSet (the "Auftrag"), not a raw Job (Diagram §1).
func Push(appCtx *app.AppContext, jobType, label, entity string, priority int, inputsRaw string) (*jobs.JobSet, error) {
	var inputs json.RawMessage
	if inputsRaw != "" {
		if !json.Valid([]byte(inputsRaw)) {
			return nil, fmt.Errorf("inputs is not valid json: %s", inputsRaw)
		}
		inputs = json.RawMessage(inputsRaw)
	}

	js := jobs.NewJobSet(uuid.NewString(), jobType, priority, label, entity, inputs)

	if err := appCtx.Jobs.PushJobSet(js); err != nil {
		return nil, fmt.Errorf("could not push job set: %w", err)
	}

	return js, nil
}
