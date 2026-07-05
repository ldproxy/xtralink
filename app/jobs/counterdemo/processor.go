// Package counterdemo is a second, deliberately minimal example job type -
// the structural opposite of tileseedingdemo. Where tile-seeding has a
// setup phase that fans out into many parallel sub-Jobs with a JSON-path
// progress descriptor, this is a single Job with no setup/cleanup and no
// progressDetails at all. It exists to prove that lib/jobs generalizes to
// other job shapes, not just the one shape it has actually been exercised
// with so far.
package counterdemo

import (
	"fmt"
	"time"

	"github.com/ldproxy/xtrasync/lib/jobs"
)

// Type is the demo-counter job type.
const Type = "demo-counter"

// CounterProcessor counts from 1 to Job.Total, one step at a time. It
// updates Job.current and JobSet.current directly via UpdateJob/UpdateJobSet
// with no updates descriptor, since there is no progressDetails structure
// to fan out into for this job type - a valid alternative to the
// declarative Job.UpdateTargets mechanism tile-seeding uses.
type CounterProcessor struct {
	StepDuration time.Duration
	// FailAt, if > 0, makes the processor return a permanent error once it
	// reaches this step, to also exercise onJobPermanentlyFailed without
	// any setup/cleanup/progressDetails involved.
	FailAt int
}

func (CounterProcessor) JobType() string { return Type }
func (CounterProcessor) Priority() int   { return 1000 }

func (p CounterProcessor) Process(job *jobs.Job, jobSet *jobs.JobSet, backend jobs.Backend) jobs.JobResult {
	for i := 1; i <= job.Total; i++ {
		time.Sleep(p.StepDuration)

		if p.FailAt > 0 && i == p.FailAt {
			return jobs.Error(fmt.Sprintf("simulated failure at step %d/%d", i, job.Total))
		}

		if err := backend.UpdateJob(job.ID, 1); err != nil {
			return jobs.Error(fmt.Sprintf("step %d/%d: %v", i, job.Total, err))
		}
		if job.PartOf != "" {
			if err := backend.UpdateJobSet(job.PartOf, 1, nil); err != nil {
				return jobs.Error(fmt.Sprintf("step %d/%d: %v", i, job.Total, err))
			}
		}
	}

	return jobs.Success()
}
