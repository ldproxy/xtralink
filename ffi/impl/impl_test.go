package impl

import (
	"testing"

	"github.com/google/uuid"

	"github.com/ldproxy/xtralink/lib/jobs"
)

const (
	testJobKind        = "delete-test"
	testPartialJobKind = "delete-test:worker"
)

// newTestJobQueue returns a JobQueue over MemoryBackend without starting the
// runner: these tests drive the backend directly, so there is no dispatch
// loop to wait on.
func newTestJobQueue() *JobQueue {
	q := NewJobQueue()
	q.backend = jobs.NewMemoryBackend()
	return q
}

// pushJob pushes a Job with a single open worker PartialJob, the shape
// finishJob completes.
func pushJob(t *testing.T, q *JobQueue) string {
	t.Helper()

	jobID := uuid.NewString()
	if err := q.backend.PushJob(jobs.NewJob(jobID, testJobKind, 1000, "", nil)); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	if err := q.backend.InitJob(jobID, 1, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	if err := q.backend.PushPartialJob(jobs.NewPartialJob(uuid.NewString(), testPartialJobKind, 1000, jobID), false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	return jobID
}

// finishJob takes and completes the worker PartialJob, which finalizes its
// Job - the same take/start/update/done sequence the Runner drives.
func finishJob(t *testing.T, q *JobQueue, jobID string) {
	t.Helper()

	partialJob, err := q.backend.Take(testPartialJobKind, "test")
	if err != nil || partialJob == nil {
		t.Fatalf("Take: %v, %+v", err, partialJob)
	}
	// A Job only counts as done once it has started, which is the Runner's
	// job for the first non-setup PartialJob it takes.
	if err := q.backend.StartJob(jobID); err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	if err := q.backend.UpdatePartialJob(partialJob.Id, 1); err != nil {
		t.Fatalf("UpdatePartialJob: %v", err)
	}
	if err := q.backend.Done(partialJob.Id); err != nil {
		t.Fatalf("Done: %v", err)
	}

	job, ok := q.Get(jobID)
	if !ok || job.FinishedAt <= 0 {
		t.Fatalf("expected the Job to be finished, got %+v (ok=%t)", job, ok)
	}
}

func TestJobQueue_DeleteRefusesUnfinishedJob(t *testing.T) {
	q := newTestJobQueue()
	jobID := pushJob(t, q)

	if q.Delete(jobID) {
		t.Error("expected Delete to refuse a Job that has not finished")
	}
	if _, ok := q.Get(jobID); !ok {
		t.Error("expected the Job to still be readable after a refused Delete")
	}
}

func TestJobQueue_DeleteRemovesFinishedJob(t *testing.T) {
	q := newTestJobQueue()
	jobID := pushJob(t, q)
	finishJob(t, q, jobID)

	if !q.Delete(jobID) {
		t.Fatal("expected Delete to retire a finished Job")
	}
	if _, ok := q.Get(jobID); ok {
		t.Error("expected the Job to be gone after Delete")
	}
}

func TestJobQueue_DeleteUnknownIdIsFalse(t *testing.T) {
	q := newTestJobQueue()

	if q.Delete(uuid.NewString()) {
		t.Error("expected Delete to report false for an unknown id")
	}
}
