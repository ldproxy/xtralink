package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The tests in this file are a simulated stand-in for xtraplatform-tiles'
// TileSeedingJobSet/TileSeedingJobCreator/VectorSeedingJobProcessor, proving
// Runner/Backend/JobProcessor handle the full shape: a setup step that
// dynamically fans out into several parallel PartialJobs, a declarative
// progressDetails-fan-out via PartialJob.UpdateTargets, a cleanup step, and
// a followUp Job. Nothing here renders real tiles - only the job
// types/inputs/progressDetails shape and setup/worker/cleanup structure
// mirror the Java original. Previously a `-tags demo` CLI command
// (app/jobs/tileseedingdemo), now just a test since there's no need for it
// to ship in the binary.

const (
	tileSeedingType       = "tile-seeding"
	tileSeedingSetupType  = "tile-seeding:setup"
	tileSeedingVectorType = "tile-seeding:vector:mvt"

	tileSeedingTMS    = "demo"
	tileSeedingLevels = 10
)

// tileSeedingFakeLevels stand in for real TileMatrixPartitions/coverage
// computation - a couple of small, fixed zoom levels with a handful of
// "tiles" each.
var tileSeedingFakeLevels = []struct {
	Level int
	Tiles int
}{
	{Level: 5, Tiles: 3},
	{Level: 6, Tiles: 5},
}

type tileSeedingInputs struct {
	TileProvider string   `json:"tileProvider"`
	TileSets     []string `json:"tileSets"`
}

// tileSeedingSetupDetails is the PartialJob.details flag distinguishing
// setup from cleanup - both phases share the tileSeedingSetupType.
type tileSeedingSetupDetails struct {
	IsCleanup bool `json:"isCleanup"`
}

type tileSeedingWorkerDetails struct {
	TileSet string `json:"tileSet"`
	Level   int    `json:"level"`
}

type tileSeedingProgress struct {
	TileSets map[string]tileSeedingTilesetProgress `json:"tileSets"`
}

type tileSeedingTilesetProgress struct {
	Progress tileSeedingLevelProgress `json:"progress"`
}

type tileSeedingLevelProgress struct {
	Current int              `json:"current"`
	Levels  map[string][]int `json:"levels"`
}

type tileSeedingReport struct {
	TileProvider   string `json:"tileProvider"`
	TilesGenerated int    `json:"tilesGenerated"`
	Errors         int    `json:"errors"`
	Duration       string `json:"duration"`
}

// tileSeedingSetupProcessor mirrors TileSeedingJobCreator.java: it handles
// both the setup and cleanup phase, distinguished by the isCleanup flag in
// PartialJob.details.
type tileSeedingSetupProcessor struct{}

func (tileSeedingSetupProcessor) JobType() string { return tileSeedingSetupType }
func (tileSeedingSetupProcessor) Priority() int   { return 1001 }

func (p tileSeedingSetupProcessor) Process(partialJob *PartialJob, job *Job, backend Backend) JobResult {
	if job == nil {
		return Error(fmt.Sprintf("partial job %s has no job (partOf=%q)", partialJob.ID, partialJob.PartOf))
	}

	var details tileSeedingSetupDetails
	if len(partialJob.Details) > 0 {
		if err := json.Unmarshal(partialJob.Details, &details); err != nil {
			return Error(fmt.Sprintf("invalid setup partial job details: %v", err))
		}
	}

	if details.IsCleanup {
		return p.cleanup(job, backend)
	}
	return p.setup(job, backend)
}

// setup splits inputs.tileSets into a couple of fake sub-matrices per
// tileset (tileSeedingFakeLevels), initializes progressDetails once
// (type-specific), and pushes one PartialJob per (tileset, level) with the
// declarative progress-update descriptor attached.
func (p tileSeedingSetupProcessor) setup(job *Job, backend Backend) JobResult {
	var inputs tileSeedingInputs
	if err := json.Unmarshal(job.Inputs, &inputs); err != nil {
		return Error(fmt.Sprintf("invalid inputs: %v", err))
	}
	if len(inputs.TileSets) == 0 {
		return Error("no tileSets to seed")
	}

	progress := tileSeedingProgress{TileSets: map[string]tileSeedingTilesetProgress{}}
	for _, tileSet := range inputs.TileSets {
		levels := make([]int, tileSeedingLevels)
		for i := range levels {
			levels[i] = -1
		}
		for _, fl := range tileSeedingFakeLevels {
			levels[fl.Level] = fl.Tiles
		}
		progress.TileSets[tileSet] = tileSeedingTilesetProgress{
			Progress: tileSeedingLevelProgress{Current: 0, Levels: map[string][]int{tileSeedingTMS: levels}},
		}
	}
	if err := backend.SetProgressDetails(job.ID, progress); err != nil {
		return Error(fmt.Sprintf("could not init progress details: %v", err))
	}

	for _, tileSet := range inputs.TileSets {
		for _, fl := range tileSeedingFakeLevels {
			worker := NewPartialJob(uuid.NewString(), tileSeedingVectorType, job.Priority, job.ID)
			worker.Total = fl.Tiles
			worker.UpdateTargets = []ProgressUpdate{
				{Path: fmt.Sprintf("tileSets.%s.progress.current", tileSet), Op: ProgressOpAdd},
				{Path: fmt.Sprintf("tileSets.%s.progress.levels.%s[%d]", tileSet, tileSeedingTMS, fl.Level), Op: ProgressOpSubtract},
			}
			detailsRaw, err := json.Marshal(tileSeedingWorkerDetails{TileSet: tileSet, Level: fl.Level})
			if err != nil {
				return Error(fmt.Sprintf("could not encode worker details: %v", err))
			}
			worker.Details = detailsRaw

			if err := backend.InitJob(job.ID, fl.Tiles, nil); err != nil {
				return Error(fmt.Sprintf("could not grow job total: %v", err))
			}
			if err := backend.PushPartialJob(worker, false); err != nil {
				return Error(fmt.Sprintf("could not push worker partial job: %v", err))
			}
		}
	}

	return Success()
}

// cleanup writes the seeding report output; it does not itself decide
// whether the Job succeeded/failed - that (finishedAt/status) is already
// settled by the backend once the last PartialJob finished.
func (p tileSeedingSetupProcessor) cleanup(job *Job, backend Backend) JobResult {
	current, err := backend.GetJob(job.ID)
	if err != nil || current == nil {
		return Error(fmt.Sprintf("could not reload job for cleanup: %v", err))
	}

	var inputs tileSeedingInputs
	_ = json.Unmarshal(current.Inputs, &inputs)

	duration := time.Duration(current.UpdatedAt-current.StartedAt) * time.Millisecond
	report := tileSeedingReport{
		TileProvider:   inputs.TileProvider,
		TilesGenerated: current.Current,
		Errors:         len(current.Errors),
		Duration:       duration.String(),
	}

	if err := backend.SetOutput(job.ID, "seedingReport", OutputValue{Value: report}); err != nil {
		return Error(fmt.Sprintf("could not write output: %v", err))
	}

	return Success()
}

// tileSeedingVectorWorkerProcessor mirrors VectorSeedingJobProcessor.java,
// but instead of real tile rendering it simulates "tile-by-tile" work. It
// reports progress with a single UpdatePartialJob(partialJob.ID, 1) call
// per tile - the fan-out to Job.progressDetails happens generically via
// partialJob.UpdateTargets.
type tileSeedingVectorWorkerProcessor struct {
	tileDuration time.Duration
}

func (tileSeedingVectorWorkerProcessor) JobType() string { return tileSeedingVectorType }
func (tileSeedingVectorWorkerProcessor) Priority() int   { return 1000 }

func (p tileSeedingVectorWorkerProcessor) Process(partialJob *PartialJob, job *Job, backend Backend) JobResult {
	var details tileSeedingWorkerDetails
	_ = json.Unmarshal(partialJob.Details, &details)

	for i := 0; i < partialJob.Total; i++ {
		time.Sleep(p.tileDuration)

		if err := backend.UpdatePartialJob(partialJob.ID, 1); err != nil {
			return Error(fmt.Sprintf(
				"tile %d/%d (%s, level %d): %v", i+1, partialJob.Total, details.TileSet, details.Level, err))
		}
	}

	return Success()
}

func newTileSeedingJob(label, tileProvider string, tileSets []string) *Job {
	setupDetails, _ := json.Marshal(tileSeedingSetupDetails{IsCleanup: false})
	cleanupDetails, _ := json.Marshal(tileSeedingSetupDetails{IsCleanup: true})
	inputs, _ := json.Marshal(tileSeedingInputs{TileProvider: tileProvider, TileSets: tileSets})

	job := NewJob(uuid.NewString(), tileSeedingType, 1000, label, inputs)
	job.Setup = NewPartialJob(uuid.NewString(), tileSeedingSetupType, job.Priority, job.ID)
	job.Setup.Details = setupDetails
	job.Cleanup = NewPartialJob(uuid.NewString(), tileSeedingSetupType, job.Priority, job.ID)
	job.Cleanup.Details = cleanupDetails
	return job
}

// waitForTileSeedingCompletion polls a Job until it is finished.
// finishedAt is set as soon as every PartialJob is done, which happens
// *before* the cleanup PartialJob that was just pushed actually runs - so
// this also gives cleanup a brief grace period to write its output before
// treating the run as over.
func waitForTileSeedingCompletion(t *testing.T, b Backend, id string, timeout time.Duration) *Job {
	t.Helper()

	deadline := time.Now().Add(timeout)
	const cleanupGracePeriod = 300 * time.Millisecond
	var finishedObservedAt time.Time

	for {
		current, err := b.GetJob(id)
		if err != nil {
			t.Fatalf("GetJob: %v", err)
		}
		if current != nil && current.FinishedAt > 0 {
			if finishedObservedAt.IsZero() {
				finishedObservedAt = time.Now()
			}
			cleanupDone := current.Cleanup == nil || len(current.Outputs) > 0
			if cleanupDone || time.Since(finishedObservedAt) > cleanupGracePeriod {
				return current
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for job %s to finish", timeout, id)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestTileSeedingDemo_FullLifecycleWithFollowUp(t *testing.T) {
	b := NewMemoryBackend()

	main := newTileSeedingJob("Tile cache seeding", "demo-tiles", []string{"vineyards"})
	followUp := newTileSeedingJob("Tile cache seeding (follow-up)", "demo-tiles", []string{"vineyards"})
	main.FollowUps = []*Job{followUp}

	if err := b.PushJob(main); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	var runnerErrs []error
	r := NewRunner(b, "test")
	r.PollInterval = 5 * time.Millisecond
	r.OnError = func(err error) { runnerErrs = append(runnerErrs, err) }
	r.Register(tileSeedingSetupProcessor{})
	r.Register(tileSeedingVectorWorkerProcessor{tileDuration: 2 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runnerDone := make(chan error, 1)
	go func() { runnerDone <- r.Run(ctx) }()

	final := waitForTileSeedingCompletion(t, b, main.ID, 5*time.Second)
	if final.Status() != StatusSuccessful {
		t.Fatalf("main job Status() = %s, want successful (errors=%v)", final.Status(), final.Errors)
	}
	report, ok := final.Outputs["seedingReport"]
	if !ok {
		t.Fatal("expected a seedingReport output on the main job")
	}
	raw, err := json.Marshal(report.Value)
	if err != nil {
		t.Fatalf("marshal seedingReport: %v", err)
	}
	var parsed tileSeedingReport
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("unmarshal seedingReport: %v", err)
	}
	wantTiles := 0
	for _, fl := range tileSeedingFakeLevels {
		wantTiles += fl.Tiles
	}
	if parsed.TilesGenerated != wantTiles {
		t.Errorf("seedingReport.tilesGenerated = %d, want %d", parsed.TilesGenerated, wantTiles)
	}

	followUpFinal := waitForTileSeedingCompletion(t, b, followUp.ID, 5*time.Second)
	if followUpFinal.Status() != StatusSuccessful {
		t.Errorf("follow-up job Status() = %s, want successful (errors=%v)", followUpFinal.Status(), followUpFinal.Errors)
	}

	cancel()
	<-runnerDone
	for _, err := range runnerErrs {
		t.Errorf("runner error: %v", err)
	}
}

// TestTileSeedingDemo_EmptyTileSetsFailsSetup exercises the permanent setup
// failure path (onPartialJobPermanentlyFailed forcing the Job to a failed
// end state directly, since a failed setup never created any PartialJobs
// for isDone() to trigger on).
func TestTileSeedingDemo_EmptyTileSetsFailsSetup(t *testing.T) {
	b := NewMemoryBackend()

	job := newTileSeedingJob("Tile cache seeding", "demo-tiles", nil)
	if err := b.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	r := NewRunner(b, "test")
	r.PollInterval = 5 * time.Millisecond
	r.Register(tileSeedingSetupProcessor{})
	r.Register(tileSeedingVectorWorkerProcessor{tileDuration: 2 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	runnerDone := make(chan error, 1)
	go func() { runnerDone <- r.Run(ctx) }()

	final := waitForTileSeedingCompletion(t, b, job.ID, 2*time.Second)
	cancel()
	<-runnerDone

	if final.Status() != StatusFailed {
		t.Errorf("Status() = %s, want failed (errors=%v)", final.Status(), final.Errors)
	}
	found := false
	for _, e := range final.Errors {
		if e == "no tileSets to seed" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected setup's error message in Job.Errors, got %v", final.Errors)
	}
}
