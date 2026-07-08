//go:build demo

// Package tileseedingdemo is a simulated stand-in for xtraplatform-tiles'
// TileSeedingJobSet/TileSeedingJobCreator/VectorSeedingJobProcessor (the
// example the concept diagram is built around). xtrasync has no tile
// provider and cannot render real tiles, so the tile-by-tile work is faked;
// only the job types, inputs/progressDetails shape and setup/worker/cleanup
// structure are taken from the diagram. Deliberately kept out of lib/jobs:
// this is example code, not part of the generic, reusable queue core.
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

// Inputs mirrors JobSet.inputs (Diagram §5.2): tileSets normalized to a
// plain list of names, isReseed -> reseed.
type Inputs struct {
	TileProvider string   `json:"tileProvider"`
	TileSets     []string `json:"tileSets"`
	Reseed       bool     `json:"reseed"`
}

// SetupDetails is the Job.details flag distinguishing setup from cleanup -
// both phases share the TypeSetup job type (Diagram §3 read notes).
type SetupDetails struct {
	IsCleanup bool `json:"isCleanup"`
}

// WorkerDetails is attached to each vector sub-Job so the worker knows what
// it is (simulated) rendering, for logging/error messages only.
type WorkerDetails struct {
	TileSet string `json:"tileSet"`
	Level   int    `json:"level"`
}

// ProgressDetails mirrors TileSeedingJobSet's tileSets/progress/levels shape
// (Diagram §3/§5.3), scaled down to fakeLevels.
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

// SeedingReport is written to JobSet.outputs by the cleanup step (Diagram §5.4).
type SeedingReport struct {
	TileProvider   string `json:"tileProvider"`
	TilesGenerated int    `json:"tilesGenerated"`
	Errors         int    `json:"errors"`
	Duration       string `json:"duration"`
}
