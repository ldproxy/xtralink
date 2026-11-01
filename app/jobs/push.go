package jobs

import (
	"encoding/json"
	"fmt"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/jobs"
	"github.com/ldproxy/xtralink/model"
)

// PushRequest is everything a caller can decide about a Job it pushes,
// which is exactly what the generated model.JobConfiguration describes.
// Only Kind is required; every other field has a working zero value.
//
// A kind in Partials does not have to be one of this configuration's own
// job definitions: a Job can be composed partly of parts some other service
// processes, and only whoever runs a processor for a kind needs to know
// what it means. Nothing here can tell that apart from a typo, though, and
// a kind no processor ever registers leaves the Job waiting until its TTL.
type PushRequest struct {
	model.JobConfiguration
}

// Push builds a Job from req and pushes it onto the queue - fire and
// forget, it never waits for the Job to finish. The backend queues the
// Job's partials (s. lib/jobs.Backend.PushJob).
//
// With no Partials, a Kind that matches a configured JobDefinition gets
// exactly one PartialJob of that same kind; anything else stays a bare Job
// with no PartialJobs at all, exactly as before JobDefinitions existed.
func Push(appCtx *app.AppContext, req PushRequest) (*model.Job, error) {
	if req.Kind == "" {
		return nil, fmt.Errorf(`"kind" is required`)
	}

	cfg := req.JobConfiguration
	if len(cfg.Partials) == 0 && appCtx.Settings != nil {
		if def, _ := appCtx.Settings.GetJobDefinition(cfg.Kind); def != nil {
			cfg.Partials = []string{def.Kind}
		}
	}

	job := jobs.NewJobFromConfiguration(cfg)
	if err := appCtx.Jobs.PushJob(&job); err != nil {
		return nil, fmt.Errorf("could not push job: %w", err)
	}

	return &job, nil
}

// ParseInputs decodes a raw inputs JSON object, as typed on the command
// line, into the opaque map the model carries. An empty string stays nil
// (no inputs at all), rather than an empty map.
func ParseInputs(inputsRaw string) (map[string]any, error) {
	if inputsRaw == "" {
		return nil, nil
	}
	var inputs map[string]any
	if err := json.Unmarshal([]byte(inputsRaw), &inputs); err != nil {
		return nil, fmt.Errorf("inputs is not a valid json object: %s", inputsRaw)
	}
	return inputs, nil
}
