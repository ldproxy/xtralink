//go:build demo

package tileseedingdemo

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ldproxy/xtralink/lib/jobs"
)

// VectorWorkerProcessor mirrors VectorSeedingJobProcessor.java, but instead
// of real tile rendering it simulates "tile-by-tile" work. It reports
// progress with a single UpdatePartialJob(partialJob.ID, 1) call per tile -
// the fan-out to Job.progressDetails happens generically via
// partialJob.UpdateTargets; unlike the current Java version, this worker
// needs no tile-seeding-specific update call of its own.
type VectorWorkerProcessor struct {
	TileDuration time.Duration
}

func (VectorWorkerProcessor) JobType() string { return TypeVector }
func (VectorWorkerProcessor) Priority() int   { return 1000 }

func (p VectorWorkerProcessor) Process(partialJob *jobs.PartialJob, job *jobs.Job, backend jobs.Backend) jobs.JobResult {
	var details WorkerDetails
	_ = json.Unmarshal(partialJob.Details, &details)

	for i := 0; i < partialJob.Total; i++ {
		time.Sleep(p.TileDuration)

		if err := backend.UpdatePartialJob(partialJob.ID, 1); err != nil {
			return jobs.Error(fmt.Sprintf(
				"tile %d/%d (%s, level %d): %v", i+1, partialJob.Total, details.TileSet, details.Level, err))
		}
	}

	return jobs.Success()
}
