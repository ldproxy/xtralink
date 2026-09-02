package jobs

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/jobs"
	"github.com/ldproxy/xtralink/model"
)

// PushRequest is everything a caller can decide about a Job it pushes: the
// Job itself as the generated model already describes it, plus the two
// things that model has no word for because they are xtralink's own -
// which JobDefinitions the Job's PartialJobs run, and whether they run in
// order.
//
// Only Kind is required; every other field has a working zero value.
type PushRequest struct {
	model.JobConfiguration

	// Partials are the kinds this Job is made of, one PartialJob each.
	// Empty means a bare Job with no PartialJobs of its own.
	//
	// A kind does not have to be one of this configuration's own job
	// definitions: a Job can be composed partly of parts some other service
	// processes, and only whoever runs a processor for a kind needs to know
	// what it means. Nothing here can tell that apart from a typo, though,
	// and a kind no processor ever registers leaves the Job waiting until
	// its TTL.
	Partials []string

	// Sequential makes the PartialJobs run strictly in the order Partials
	// lists them, each becoming takeable only once its predecessor has
	// finished.
	// The default runs them all at once. A Job's setup and cleanup sit
	// outside this ordering (s. lib/jobs.isSetupOrCleanup).
	Sequential bool
}

// Push builds a Job from req and pushes it onto the queue - fire and
// forget, it never waits for the Job to finish.
//
// With no Partials, a Kind that matches a configured JobDefinition gets
// exactly one PartialJob of that same kind; anything else stays a bare Job
// with no PartialJobs at all, exactly as before JobDefinitions existed.
func Push(appCtx *app.AppContext, req PushRequest) (*model.Job, error) {
	if req.Kind == "" {
		return nil, fmt.Errorf(`"kind" is required`)
	}

	partials := req.Partials
	if len(partials) == 0 && appCtx.Settings != nil {
		if def, _ := appCtx.Settings.GetJobDefinition(req.Kind); def != nil {
			partials = []string{def.Kind}
		}
	}

	job := jobs.NewJobFromConfiguration(req.JobConfiguration)
	if req.Sequential {
		job.Sequence = &model.JobSequence{Current: 0, Remaining: 0}
	}

	// PushJob pushes the setup PartialJob itself, so the Job has to be complete
	// before it goes in - NewJobFromConfiguration having already built
	// Setup is what makes that possible.
	if err := appCtx.Jobs.PushJob(&job); err != nil {
		return nil, fmt.Errorf("could not push job: %w", err)
	}

	for _, kind := range partials {
		partialJob := jobs.NewPartialJob(uuid.NewString(), kind, req.Priority, job.Id)
		partialJob.Progress.Total = 1

		// Each PartialJob counts as exactly one unit of the Job's total - it
		// either fully completes or it doesn't, there's no finer-grained
		// progress within it (s. WorkflowJobProcessor, which reports the
		// matching +1 on success). Without this, Job.Total/Current would
		// both still be 0 once the first one finishes, and IsDone()
		// (current==total) would trivially - and wrongly - already be
		// true.
		if err := appCtx.Jobs.InitJob(job.Id, 1, nil); err != nil {
			return nil, fmt.Errorf("could not grow job total for partial job %q: %w", kind, err)
		}
		if err := appCtx.Jobs.PushPartialJob(partialJob, false); err != nil {
			return nil, fmt.Errorf("could not push partial job %q: %w", kind, err)
		}
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
