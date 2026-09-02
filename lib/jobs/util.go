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
