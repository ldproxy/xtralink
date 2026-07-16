package jobs

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/jobs"
)

// Push builds a new Job from CLI input and pushes it onto the queue. A
// caller pushes a Job (the "order"), not a raw PartialJob. If jobType
// matches a configured JobDefinition (s. app.JobDefinition), the Job gets
// exactly one PartialJob of that same type (via PushPipeline) - a
// multi-step Job is only ever built ad-hoc, by the job:push workflow
// action's `partials:` support, never by a direct `job push <id>`.
// Otherwise (no matching JobDefinition - e.g. an ad-hoc type like
// "nba-apply") it stays a bare Job with no PartialJobs of its own, exactly
// as before JobDefinitions existed.
func Push(appCtx *app.AppContext, jobType, label string, priority int, inputsRaw string) (*jobs.Job, error) {
	var def *app.JobDefinition
	if appCtx.Settings != nil {
		def, _ = appCtx.Settings.GetJobDefinition(jobType)
	}
	if def == nil {
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

	return PushPipeline(appCtx, jobType, label, priority, inputsRaw, []app.JobDefinition{*def}, true)
}

// PushPipeline pushes a Job of jobType with one PartialJob per given
// JobDefinition (Type=def.Id, Sequence=its index in defs), all created
// together up front - there is no setup step that creates them dynamically,
// since the shape is already fully known by the caller. Shared by Push (a
// single JobDefinition match - exactly one PartialJob) and the job:push
// workflow action's `partials:` support (several JobDefinitions resolved
// ad-hoc via Settings.GetJobDefinition, without the pushed Job's own type
// needing to be a JobDefinition itself).
func PushPipeline(appCtx *app.AppContext, jobType, label string, priority int, inputsRaw string, defs []app.JobDefinition, parallel bool) (*jobs.Job, error) {
	var inputs json.RawMessage
	if inputsRaw != "" {
		if !json.Valid([]byte(inputsRaw)) {
			return nil, fmt.Errorf("inputs is not valid json: %s", inputsRaw)
		}
		inputs = json.RawMessage(inputsRaw)
	}

	job := jobs.NewJob(uuid.NewString(), jobType, priority, label, inputs)
	job.Parallel = parallel
	if err := appCtx.Jobs.PushJob(job); err != nil {
		return nil, fmt.Errorf("could not push job: %w", err)
	}

	for i, def := range defs {
		partialJob := jobs.NewPartialJob(uuid.NewString(), def.Id, priority, job.ID)
		partialJob.Sequence = i
		partialJob.Total = 1

		// Each step counts as exactly one unit of the Job's total - a step
		// either fully completes or it doesn't, there's no finer-grained
		// progress within it (s. WorkflowJobProcessor, which reports the
		// matching +1 on success). Without this, Job.Total/Current would
		// both still be 0 once the first step finishes, and IsDone()
		// (current==total) would trivially - and wrongly - already be
		// true.
		if err := appCtx.Jobs.InitJob(job.ID, 1, nil); err != nil {
			return nil, fmt.Errorf("could not grow job total for step %q: %w", def.Id, err)
		}
		if err := appCtx.Jobs.PushPartialJob(partialJob, false); err != nil {
			return nil, fmt.Errorf("could not push partial job for step %q: %w", def.Id, err)
		}
	}

	return job, nil
}
