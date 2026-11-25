package jobs

import (
	"time"

	"github.com/ldproxy/xtralink/model"
)

func nowMillis() int64 {
	return time.Now().UnixMilli()
}

// IsCancelled reports whether the Job has been dismissed since it was handed
// to a processor. A long-running ProcessFunc that wants to be cancellable
// polls this and returns model.Success() when it flips - cancellation is
// cooperative, which is what makes it optional per processor: one that never
// asks simply runs to completion, and nothing else has to know.
//
// It re-reads the Job because the *model.Job a processor holds is a snapshot
// from the moment its PartialJob was taken. A read failure reports false: a
// processor should keep working rather than abandon a Job over a transient
// backend error.
func IsCancelled(backend Backend, jobID string) bool {
	job, err := backend.GetJob(jobID)
	return err == nil && job != nil && job.Status == model.StatusDISMISSED
}

// IsSettled reports whether job is done changing: finished, and its cleanup
// finished too. A finished Job still changes while its cleanup runs, since
// a failing cleanup turns it FAILED, and its follow-ups are only pushed once
// the cleanup is done. Removing it or dropping its listener any earlier
// loses that.
func IsSettled(job *model.Job) bool {
	if job.FinishedAt <= 0 {
		return false
	}
	return job.Cleanup == nil || job.Cleanup.FinishedAt > 0
}

// sameTake reports whether a and b are copies of the same take of a
// PartialJob. Take stamps Executor and StartedAt afresh every time, so a
// copy read before a re-queue and a later take never match.
func sameTake(a, b *model.PartialJob) bool {
	return a.Executor == b.Executor && a.StartedAt == b.StartedAt
}
