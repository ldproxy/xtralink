//go:build demo

// Package counterdemo is a second, deliberately minimal example job type -
// the structural opposite of tileseedingdemo. Where tile-seeding has a
// setup phase that fans out into many parallel PartialJobs with a JSON-path
// progress descriptor, this is a single PartialJob with no setup/cleanup and
// no progressDetails at all. It exists to prove that lib/jobs generalizes to
// other job shapes, not just the one shape it has actually been exercised
// with so far.
package counterdemo

import (
	"fmt"
	"time"

	"github.com/ldproxy/xtralink/lib/jobs"
)

// Type is the demo-counter job type.
const Type = "demo-counter"

// CounterProcessor counts from 1 to PartialJob.Total, one step at a time. It
// updates PartialJob.current and Job.current directly via
// UpdatePartialJob/UpdateJob with no updates descriptor, since there is no
// progressDetails structure to fan out into for this job type - a valid
// alternative to the declarative PartialJob.UpdateTargets mechanism
// tile-seeding uses.
type CounterProcessor struct {
	StepDuration time.Duration
	// FailAt, if > 0, makes the processor return a permanent error once it
	// reaches this step, to also exercise onPartialJobPermanentlyFailed
	// without any setup/cleanup/progressDetails involved.
	FailAt int
}

func (CounterProcessor) JobType() string { return Type }
func (CounterProcessor) Priority() int   { return 1000 }

func (p CounterProcessor) Process(partialJob *jobs.PartialJob, job *jobs.Job, backend jobs.Backend) jobs.JobResult {
	for i := 1; i <= partialJob.Total; i++ {
		time.Sleep(p.StepDuration)

		if p.FailAt > 0 && i == p.FailAt {
			return jobs.Error(fmt.Sprintf("simulated failure at step %d/%d", i, partialJob.Total))
		}

		if err := backend.UpdatePartialJob(partialJob.ID, 1); err != nil {
			return jobs.Error(fmt.Sprintf("step %d/%d: %v", i, partialJob.Total, err))
		}
		if partialJob.PartOf != "" {
			if err := backend.UpdateJob(partialJob.PartOf, 1, nil); err != nil {
				return jobs.Error(fmt.Sprintf("step %d/%d: %v", i, partialJob.Total, err))
			}
		}
	}

	return jobs.Success()
}
