//go:build demo

package tileseedingdemo

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ldproxy/xtrasync/lib/jobs"
)

// VectorWorkerProcessor mirrors VectorSeedingJobProcessor.java, but instead
// of real tile rendering it simulates "tile-by-tile" work. It reports
// progress with a single UpdateJob(job.ID, 1) call per tile - the fan-out to
// JobSet.progressDetails happens generically via job.UpdateTargets (Diagram
// §4); unlike the current Java version, this worker needs no
// tile-seeding-specific update call of its own.
type VectorWorkerProcessor struct {
	TileDuration time.Duration
}

func (VectorWorkerProcessor) JobType() string { return TypeVector }
func (VectorWorkerProcessor) Priority() int   { return 1000 }

func (p VectorWorkerProcessor) Process(job *jobs.Job, jobSet *jobs.JobSet, backend jobs.Backend) jobs.JobResult {
	var details WorkerDetails
	_ = json.Unmarshal(job.Details, &details)

	for i := 0; i < job.Total; i++ {
		time.Sleep(p.TileDuration)

		if err := backend.UpdateJob(job.ID, 1); err != nil {
			return jobs.Error(fmt.Sprintf(
				"tile %d/%d (%s, level %d): %v", i+1, job.Total, details.TileSet, details.Level, err))
		}
	}

	return jobs.Success()
}
