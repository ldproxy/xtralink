//go:build !demo

package cli

// Job (this build: no -tags demo, the default) is just JobBase - no Demo
// field at all, so `job demo` is absent from the command tree/--help
// entirely, not merely non-functional. The example job processors
// (app/jobs/tileseedingdemo, app/jobs/counterdemo) are proof-of-concept code
// for showing lib/jobs generalizes beyond tile-seeding, not meant to ship in
// production.
type Job struct {
	JobBase
}
