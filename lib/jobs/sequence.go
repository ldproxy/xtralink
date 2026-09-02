package jobs

import "github.com/ldproxy/xtralink/model"

// isSetupOrCleanup reports whether partialJobID is the Job's setup or
// cleanup step rather than one of its ordinary PartialJobs.
//
// Those two sit outside the sequence entirely: setup runs before any slot
// opens and cleanup after the last one closed, and onPartialJobDone
// already skips advancing the sequence for both. They must be skipped when
// a slot is handed out as well, or a sequenced Job would never run - setup
// would take slot 0 and leave every ordinary PartialJob waiting on a
// Current that nothing advances, and a cleanup pushed after the final
// advance set Current to -1 could never match its own slot.
func isSetupOrCleanup(job *model.Job, partialJobID string) bool {
	if job.Setup != nil && job.Setup.Id == partialJobID {
		return true
	}
	return job.Cleanup != nil && job.Cleanup.Id == partialJobID
}
