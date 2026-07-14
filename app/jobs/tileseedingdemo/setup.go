//go:build demo

package tileseedingdemo

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/ldproxy/xtralink/lib/jobs"
)

// SetupProcessor mirrors TileSeedingJobCreator.java: it handles both the
// setup and cleanup phase of a tile-seeding Job, distinguished by the
// isCleanup flag in PartialJob.details.
type SetupProcessor struct{}

func (SetupProcessor) JobType() string { return TypeSetup }
func (SetupProcessor) Priority() int   { return 1001 }

func (p SetupProcessor) Process(partialJob *jobs.PartialJob, job *jobs.Job, backend jobs.Backend) jobs.JobResult {
	if job == nil {
		// Can legitimately happen if the Job was deleted/expired (Java:
		// cleanupOldJobSets()) while an orphaned PartialJob for it still
		// lingered in the queue - fail the PartialJob instead of crashing.
		return jobs.Error(fmt.Sprintf("partial job %s has no job (partOf=%q)", partialJob.ID, partialJob.PartOf))
	}

	var details SetupDetails
	if len(partialJob.Details) > 0 {
		if err := json.Unmarshal(partialJob.Details, &details); err != nil {
			return jobs.Error(fmt.Sprintf("invalid setup partial job details: %v", err))
		}
	}

	if details.IsCleanup {
		return p.cleanup(job, backend)
	}
	return p.setup(job, backend)
}

// setup splits inputs.tileSets into a couple of fake sub-matrices per
// tileset (fakeLevels), initializes progressDetails once (type-specific),
// and pushes one PartialJob per (tileset, level) with the declarative
// progress-update descriptor attached.
func (p SetupProcessor) setup(job *jobs.Job, backend jobs.Backend) jobs.JobResult {
	var inputs Inputs
	if err := json.Unmarshal(job.Inputs, &inputs); err != nil {
		return jobs.Error(fmt.Sprintf("invalid inputs: %v", err))
	}
	if len(inputs.TileSets) == 0 {
		return jobs.Error("no tileSets to seed")
	}

	progress := ProgressDetails{TileSets: map[string]TilesetProgress{}}
	for _, tileSet := range inputs.TileSets {
		levels := make([]int, levelsArrayLength)
		for i := range levels {
			levels[i] = -1
		}
		for _, fl := range fakeLevels {
			levels[fl.Level] = fl.Tiles
		}
		progress.TileSets[tileSet] = TilesetProgress{
			Progress: LevelProgress{Current: 0, Levels: map[string][]int{tileMatrixSet: levels}},
		}
	}
	if err := backend.SetProgressDetails(job.ID, progress); err != nil {
		return jobs.Error(fmt.Sprintf("could not init progress details: %v", err))
	}

	for _, tileSet := range inputs.TileSets {
		for _, fl := range fakeLevels {
			worker := jobs.NewPartialJob(uuid.NewString(), TypeVector, job.Priority, job.ID)
			worker.Total = fl.Tiles
			worker.UpdateTargets = []jobs.ProgressUpdate{
				{Path: fmt.Sprintf("tileSets.%s.progress.current", tileSet), Op: jobs.ProgressOpAdd},
				{Path: fmt.Sprintf("tileSets.%s.progress.levels.%s[%d]", tileSet, tileMatrixSet, fl.Level), Op: jobs.ProgressOpSubtract},
			}
			detailsRaw, err := json.Marshal(WorkerDetails{TileSet: tileSet, Level: fl.Level})
			if err != nil {
				return jobs.Error(fmt.Sprintf("could not encode worker details: %v", err))
			}
			worker.Details = detailsRaw

			if err := backend.InitJob(job.ID, fl.Tiles, nil); err != nil {
				return jobs.Error(fmt.Sprintf("could not grow job total: %v", err))
			}
			if err := backend.PushPartialJob(worker, false); err != nil {
				return jobs.Error(fmt.Sprintf("could not push worker partial job: %v", err))
			}
		}
	}

	return jobs.Success()
}

// cleanup writes the seeding report output; it does not itself decide
// whether the Job succeeded/failed - that (finishedAt/status) is already
// settled by RedisBackend.onPartialJobDone once the last PartialJob
// finished.
func (p SetupProcessor) cleanup(job *jobs.Job, backend jobs.Backend) jobs.JobResult {
	current, err := backend.GetJob(job.ID)
	if err != nil || current == nil {
		return jobs.Error(fmt.Sprintf("could not reload job for cleanup: %v", err))
	}

	var inputs Inputs
	_ = json.Unmarshal(current.Inputs, &inputs)

	duration := time.Duration(current.UpdatedAt-current.StartedAt) * time.Millisecond
	report := SeedingReport{
		TileProvider:   inputs.TileProvider,
		TilesGenerated: current.Current,
		Errors:         len(current.Errors),
		Duration:       duration.String(),
	}

	if err := backend.SetOutput(job.ID, "seedingReport", jobs.OutputValue{Value: report}); err != nil {
		return jobs.Error(fmt.Sprintf("could not write output: %v", err))
	}

	return jobs.Success()
}
