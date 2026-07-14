//go:build demo

// Package tileseedingdemo is a simulated stand-in for xtraplatform-tiles'
// TileSeedingJobSet/TileSeedingJobCreator/VectorSeedingJobProcessor. xtrasync
// has no tile provider and cannot render real tiles, so the tile-by-tile
// work is faked; only the job types, inputs/progressDetails shape and
// setup/worker/cleanup structure mirror the Java original. Deliberately kept
// out of lib/jobs: this is example code, not part of the generic, reusable
// queue core.
package tileseedingdemo

const (
	TypeSet    = "tile-seeding"
	TypeSetup  = "tile-seeding:setup"
	TypeVector = "tile-seeding:vector:mvt"

	tileMatrixSet     = "demo"
	levelsArrayLength = 10
)

// fakeLevels stand in for real TileMatrixPartitions/coverage computation -
// a couple of small, fixed zoom levels with a handful of "tiles" each.
var fakeLevels = []struct {
	Level int
	Tiles int
}{
	{Level: 5, Tiles: 3},
	{Level: 6, Tiles: 5},
}

// Inputs mirrors Job.inputs: tileSets normalized to a plain list of names,
// isReseed -> reseed.
type Inputs struct {
	TileProvider string   `json:"tileProvider"`
	TileSets     []string `json:"tileSets"`
	Reseed       bool     `json:"reseed"`
}

// SetupDetails is the PartialJob.details flag distinguishing setup from
// cleanup - both phases share the TypeSetup partial job type.
type SetupDetails struct {
	IsCleanup bool `json:"isCleanup"`
}

// WorkerDetails is attached to each vector PartialJob so the worker knows
// what it is (simulated) rendering, for logging/error messages only.
type WorkerDetails struct {
	TileSet string `json:"tileSet"`
	Level   int    `json:"level"`
}

// ProgressDetails mirrors TileSeedingJobSet's tileSets/progress/levels shape,
// scaled down to fakeLevels.
type ProgressDetails struct {
	TileSets map[string]TilesetProgress `json:"tileSets"`
}

type TilesetProgress struct {
	Progress LevelProgress `json:"progress"`
}

type LevelProgress struct {
	Current int              `json:"current"`
	Levels  map[string][]int `json:"levels"`
}

// SeedingReport is written to Job.outputs by the cleanup step.
type SeedingReport struct {
	TileProvider   string `json:"tileProvider"`
	TilesGenerated int    `json:"tilesGenerated"`
	Errors         int    `json:"errors"`
	Duration       string `json:"duration"`
}
