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
// setup and cleanup phase of a tile-seeding JobSet, distinguished by the
// isCleanup flag in Job.details.
type SetupProcessor struct{}

func (SetupProcessor) JobType() string { return TypeSetup }
func (SetupProcessor) Priority() int   { return 1001 }

func (p SetupProcessor) Process(job *jobs.Job, jobSet *jobs.JobSet, backend jobs.Backend) jobs.JobResult {
	if jobSet == nil {
		// Can legitimately happen if the JobSet was deleted/expired (Java:
		// cleanupOldJobSets()) while an orphaned Job for it still lingered
		// in the queue - fail the Job instead of crashing.
		return jobs.Error(fmt.Sprintf("job %s has no job set (partOf=%q)", job.ID, job.PartOf))
	}

	var details SetupDetails
	if len(job.Details) > 0 {
		if err := json.Unmarshal(job.Details, &details); err != nil {
			return jobs.Error(fmt.Sprintf("invalid setup job details: %v", err))
		}
	}

	if details.IsCleanup {
		return p.cleanup(jobSet, backend)
	}
	return p.setup(jobSet, backend)
}

// setup splits inputs.tileSets into a couple of fake sub-matrices per
// tileset (fakeLevels), initializes progressDetails once (type-specific),
// and pushes one sub-Job per (tileset, level) with the declarative
// progress-update descriptor attached.
func (p SetupProcessor) setup(jobSet *jobs.JobSet, backend jobs.Backend) jobs.JobResult {
	var inputs Inputs
	if err := json.Unmarshal(jobSet.Inputs, &inputs); err != nil {
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
	if err := backend.SetProgressDetails(jobSet.ID, progress); err != nil {
		return jobs.Error(fmt.Sprintf("could not init progress details: %v", err))
	}

	for _, tileSet := range inputs.TileSets {
		for _, fl := range fakeLevels {
			subJob := jobs.NewJob(uuid.NewString(), TypeVector, jobSet.Priority, jobSet.ID)
			subJob.Total = fl.Tiles
			subJob.UpdateTargets = []jobs.ProgressUpdate{
				{Path: fmt.Sprintf("tileSets.%s.progress.current", tileSet), Op: jobs.ProgressOpAdd},
				{Path: fmt.Sprintf("tileSets.%s.progress.levels.%s[%d]", tileSet, tileMatrixSet, fl.Level), Op: jobs.ProgressOpSubtract},
			}
			detailsRaw, err := json.Marshal(WorkerDetails{TileSet: tileSet, Level: fl.Level})
			if err != nil {
				return jobs.Error(fmt.Sprintf("could not encode worker details: %v", err))
			}
			subJob.Details = detailsRaw

			if err := backend.InitJobSet(jobSet.ID, fl.Tiles, nil); err != nil {
				return jobs.Error(fmt.Sprintf("could not grow job set total: %v", err))
			}
			if err := backend.PushJob(subJob, false); err != nil {
				return jobs.Error(fmt.Sprintf("could not push sub-job: %v", err))
			}
		}
	}

	return jobs.Success()
}

// cleanup writes the seeding report output; it does not itself decide
// whether the JobSet succeeded/failed - that (finishedAt/status) is already
// settled by RedisBackend.onJobDone once the last sub-Job finished.
func (p SetupProcessor) cleanup(jobSet *jobs.JobSet, backend jobs.Backend) jobs.JobResult {
	current, err := backend.GetSet(jobSet.ID)
	if err != nil || current == nil {
		return jobs.Error(fmt.Sprintf("could not reload job set for cleanup: %v", err))
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

	if err := backend.SetOutput(jobSet.ID, "seedingReport", jobs.OutputValue{Value: report}); err != nil {
		return jobs.Error(fmt.Sprintf("could not write output: %v", err))
	}

	return jobs.Success()
}
