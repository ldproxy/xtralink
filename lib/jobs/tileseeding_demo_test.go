package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ldproxy/xtralink/model"
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
	tileSeedingType        = "tile-seeding"
	tileSeedingSetupType   = "tile-seeding:setup"
	tileSeedingCleanupType = "tile-seeding:cleanup"
	tileSeedingVectorType  = "tile-seeding:vector:mvt"

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

func tileSeedingTilesForLevel(level int) int {
	for _, fl := range tileSeedingFakeLevels {
		if fl.Level == level {
			return fl.Tiles
		}
	}
	return 0
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

// decodeDetails re-decodes an opaque details map (Job.Inputs,
// PartialJob.Context) into a typed struct, mirroring how the Java
// processors deserialize their details type. The backend round-trips jobs
// through JSON, so the map's values are always generic JSON kinds - a
// []string comes back as []any, an int as float64 - and asserting the
// original Go types on them panics.
func decodeDetails(details map[string]any, target any) error {
	raw, err := json.Marshal(details)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, target)
}

// tileSeedingSetupProcessor mirrors TileSeedingJobCreator.java: it handles
// both the setup and cleanup phase, distinguished by the isCleanup flag in
// PartialJob.details.
func tileSeedingSetupProcessor(partialJob *model.PartialJob, job *model.Job, backend Backend) model.JobResult {
	if job == nil {
		return model.Error(fmt.Sprintf("partial job %s has no job (partOf=%q)", partialJob.Id, partialJob.PartOf))
	}

	if partialJob.Kind == tileSeedingCleanupType {
		return tileSeedingSetupCleanup(job, backend)
	}
	return tileSeedingSetupSetup(job, backend)
}

// setup splits inputs.tileSets into a couple of fake sub-matrices per
// tileset (tileSeedingFakeLevels), initializes progressDetails once
// (type-specific), and pushes one PartialJob per (tileset, level) with the
// declarative progress-update descriptor attached.
func tileSeedingSetupSetup(job *model.Job, backend Backend) model.JobResult {
	if job.Inputs == nil {
		return model.Error(fmt.Sprintf("invalid inputs: nil inputs"))
	}

	var inputs tileSeedingInputs
	if err := decodeDetails(job.Inputs, &inputs); err != nil {
		return model.Error(fmt.Sprintf("invalid inputs: %v", err))
	}
	if inputs.TileProvider == "" {
		return model.Error(fmt.Sprintf("invalid inputs: missing tileProvider"))
	}
	if len(inputs.TileSets) == 0 {
		return model.Error("no tileSets to seed")
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
	progressDetailsRaw, err := json.Marshal(progress)
	if err != nil {
		return model.Error(fmt.Sprintf("could not encode progress details: %v", err))
	}
	progressDetailsMap := map[string]any{}
	if err := json.Unmarshal(progressDetailsRaw, &progressDetailsMap); err != nil {
		return model.Error(fmt.Sprintf("could not decode progress details: %v", err))
	}
	if err := backend.SetProgressDetails(job.Id, progressDetailsMap); err != nil {
		return model.Error(fmt.Sprintf("could not init progress details: %v", err))
	}

	for _, tileSet := range inputs.TileSets {
		for _, fl := range tileSeedingFakeLevels {
			worker := NewPartialJob(uuid.NewString(), tileSeedingVectorType, job.Priority, job.Id)
			worker.Progress.Total = fl.Tiles
			worker.ProgressUpdates = []model.ProgressUpdate{
				{Path: fmt.Sprintf("tileSets.%s.progress.current", tileSet), Operation: model.ProgressOperationADD},
				{Path: fmt.Sprintf("tileSets.%s.progress.levels.%s[%d]", tileSet, tileSeedingTMS, fl.Level), Operation: model.ProgressOperationSUBTRACT},
			}
			detailsRaw, err := json.Marshal(tileSeedingWorkerDetails{TileSet: tileSet, Level: fl.Level})
			if err != nil {
				return model.Error(fmt.Sprintf("could not encode worker details: %v", err))
			}
			detailsMap := map[string]any{}
			if err := json.Unmarshal(detailsRaw, &detailsMap); err != nil {
				return model.Error(fmt.Sprintf("could not decode worker details: %v", err))
			}
			worker.Context = detailsMap

			if err := backend.InitJob(job.Id, fl.Tiles, nil); err != nil {
				return model.Error(fmt.Sprintf("could not grow job total: %v", err))
			}
			if err := backend.PushPartialJob(worker, false); err != nil {
				return model.Error(fmt.Sprintf("could not push worker partial job: %v", err))
			}
		}
	}

	return model.Success()
}

// cleanup writes the seeding report output; it does not itself decide
// whether the Job succeeded/failed - that (finishedAt/status) is already
// settled by the backend once the last PartialJob finished.
func tileSeedingSetupCleanup(job *model.Job, backend Backend) model.JobResult {
	current, err := backend.GetJob(job.Id)
	if err != nil || current == nil {
		return model.Error(fmt.Sprintf("could not reload job for cleanup: %v", err))
	}

	var inputs tileSeedingInputs
	if err := decodeDetails(current.Inputs, &inputs); err != nil {
		return model.Error(fmt.Sprintf("invalid inputs: %v", err))
	}

	duration := time.Duration(current.UpdatedAt-current.StartedAt) * time.Millisecond
	report := tileSeedingReport{
		TileProvider:   inputs.TileProvider,
		TilesGenerated: current.Progress.Current,
		Errors:         len(current.Errors),
		Duration:       duration.String(),
	}

	if err := backend.SetOutput(job.Id, "seedingReport", model.OutputValue{Value: report}); err != nil {
		return model.Error(fmt.Sprintf("could not write output: %v", err))
	}

	return model.Success()
}

// tileSeedingVectorWorkerProcessor mirrors VectorSeedingJobProcessor.java,
// but instead of real tile rendering it simulates "tile-by-tile" work. It
// reports progress with a single UpdatePartialJob(partialJob.ID, 1) call
// per tile - the fan-out to Job.progressDetails happens generically via
// partialJob.UpdateTargets.
func tileSeedingVectorWorkerProcessor(tileDuration time.Duration) ProcessFunc {
	return func(partialJob *model.PartialJob, job *model.Job, backend Backend) model.JobResult {
		var details tileSeedingWorkerDetails
		if err := decodeDetails(partialJob.Context, &details); err != nil {
			return model.Error(fmt.Sprintf("invalid worker partial job details: %v", err))
		}

		// One UpdatePartialJob per "rendered" tile - that single call is what
		// also advances the parent Job's progress and fans out to its
		// progressDetails, so there is deliberately no extra UpdateJob here.
		tiles := tileSeedingTilesForLevel(details.Level)
		for tile := 1; tile <= tiles; tile++ {
			time.Sleep(tileDuration)

			if err := backend.UpdatePartialJob(partialJob.Id, 1); err != nil {
				return model.Error(fmt.Sprintf(
					"tile %d/%d (%s, level %d): %v", tile, tiles, details.TileSet, details.Level, err))
			}
		}

		return model.Success()
	}
}

func newTileSeedingJob(label, tileProvider string, tileSets []string) *model.Job {
	inputs := map[string]any{
		"tileProvider": tileProvider,
		"tileSets":     tileSets,
	}

	job := NewJob(uuid.NewString(), tileSeedingType, 1000, label, inputs)
	job.Setup = NewPartialJob(uuid.NewString(), tileSeedingSetupType, job.Priority, job.Id)
	job.Cleanup = NewPartialJob(uuid.NewString(), tileSeedingCleanupType, job.Priority, job.Id)
	return job
}

// waitForTileSeedingCompletion polls a Job until it is finished.
// finishedAt is set as soon as every PartialJob is done, which happens
// *before* the cleanup PartialJob that was just pushed actually runs - so
// this also gives cleanup a brief grace period to write its output before
// treating the run as over.
func waitForTileSeedingCompletion(t *testing.T, b Backend, id string, timeout time.Duration) *model.Job {
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
	main.FollowUps = []model.Job{*followUp}

	if err := b.PushJob(main); err != nil {
		t.Fatalf("PushJob: %v", err)
	}

	var runnerErrs []error
	r := NewRunner(b, "test")
	r.PollInterval = 5 * time.Millisecond
	r.OnError = func(err error) { runnerErrs = append(runnerErrs, err) }
	r.Register(&JobProcessor{Kind: tileSeedingSetupType, Priority: 1001, Process: tileSeedingSetupProcessor})
	r.Register(&JobProcessor{Kind: tileSeedingCleanupType, Priority: 1001, Process: tileSeedingSetupProcessor})
	r.Register(&JobProcessor{Kind: tileSeedingVectorType, Priority: 1000, Process: tileSeedingVectorWorkerProcessor(2 * time.Millisecond)})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runnerDone := make(chan error, 1)
	go func() { runnerDone <- r.Run(ctx) }()

	final := waitForTileSeedingCompletion(t, b, main.Id, 5*time.Second)
	if final.Status != model.StatusSUCCESSFUL {
		t.Fatalf("main job Status = %s, want successful (errors=%v)", final.Status, final.Errors)
	}
	// Outputs is an opaque map that the backend round-trips through JSON,
	// so the stored OutputValue comes back as generic JSON, not as a
	// model.OutputValue - decode it rather than asserting the type.
	if _, ok := final.Outputs["seedingReport"]; !ok {
		t.Fatal("expected a seedingReport output on the main job")
	}
	raw, err := json.Marshal(final.Outputs["seedingReport"])
	if err != nil {
		t.Fatalf("marshal seedingReport: %v", err)
	}
	var report model.OutputValue
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("unmarshal seedingReport: %v", err)
	}
	var parsed tileSeedingReport
	rawValue, err := json.Marshal(report.Value)
	if err != nil {
		t.Fatalf("marshal seedingReport value: %v", err)
	}
	if err := json.Unmarshal(rawValue, &parsed); err != nil {
		t.Fatalf("unmarshal seedingReport value: %v", err)
	}
	wantTiles := 0
	for _, fl := range tileSeedingFakeLevels {
		wantTiles += fl.Tiles
	}
	if parsed.TilesGenerated != wantTiles {
		t.Errorf("seedingReport.tilesGenerated = %d, want %d", parsed.TilesGenerated, wantTiles)
	}

	followUpFinal := waitForTileSeedingCompletion(t, b, followUp.Id, 5*time.Second)
	if followUpFinal.Status != model.StatusSUCCESSFUL {
		t.Errorf("follow-up job Status = %s, want successful (errors=%v)", followUpFinal.Status, followUpFinal.Errors)
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
	r.Register(&JobProcessor{Kind: tileSeedingSetupType, Priority: 1001, Process: tileSeedingSetupProcessor})
	r.Register(&JobProcessor{Kind: tileSeedingCleanupType, Priority: 1001, Process: tileSeedingSetupProcessor})
	r.Register(&JobProcessor{Kind: tileSeedingVectorType, Priority: 1000, Process: tileSeedingVectorWorkerProcessor(2 * time.Millisecond)})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	runnerDone := make(chan error, 1)
	go func() { runnerDone <- r.Run(ctx) }()

	final := waitForTileSeedingCompletion(t, b, job.Id, 2*time.Second)
	cancel()
	<-runnerDone

	if final.Status != model.StatusFAILED {
		t.Errorf("Status = %s, want failed (errors=%v)", final.Status, final.Errors)
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
