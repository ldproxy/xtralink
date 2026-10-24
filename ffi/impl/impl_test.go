package impl

import (
	"testing"

	"github.com/google/uuid"

	"github.com/ldproxy/xtralink/lib/jobs"
	"github.com/ldproxy/xtralink/model"
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
	q.runner.Backend = q.backend
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

type finalizeProcessor struct {
	result model.JobResult
	calls  int
}

func (p *finalizeProcessor) Process(model.PartialJob, model.Job) (model.JobResult, error) {
	p.calls++
	return p.result, nil
}

func TestJobQueue_DeleteFinalizesJob(t *testing.T) {
	q := newTestJobQueue()
	jobID := pushJob(t, q)
	finishJob(t, q, jobID)
	finalize := &finalizeProcessor{result: model.Success()}
	if err := q.Register(jobs.FinalizeKind(testJobKind), 0, finalize); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if !q.Delete(jobID) {
		t.Fatal("expected Delete to retire a finished Job")
	}
	if finalize.calls != 1 {
		t.Errorf("expected finalize to run once on Delete, got %d", finalize.calls)
	}
}

func TestJobQueue_DeleteKeepsJobWhenFinalizeFails(t *testing.T) {
	q := newTestJobQueue()
	jobID := pushJob(t, q)
	finishJob(t, q, jobID)
	finalize := &finalizeProcessor{result: model.Error("storage unavailable")}
	if err := q.Register(jobs.FinalizeKind(testJobKind), 0, finalize); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if q.Delete(jobID) {
		t.Error("expected Delete to report false when finalize fails")
	}
	if _, ok := q.Get(jobID); !ok {
		t.Error("expected the Job to be kept so finalize can be retried")
	}
}

func TestJobQueue_DeleteUnknownIdIsFalse(t *testing.T) {
	q := newTestJobQueue()

	if q.Delete(uuid.NewString()) {
		t.Error("expected Delete to report false for an unknown id")
	}
}

func TestJobQueue_CancelDismissesUnfinishedJob(t *testing.T) {
	q := newTestJobQueue()
	jobID := pushJob(t, q)

	if !q.Cancel(jobID) {
		t.Fatal("expected Cancel to dismiss a Job that has not finished")
	}

	job, ok := q.Get(jobID)
	if !ok {
		t.Fatal("expected the dismissed Job to still be readable")
	}
	if job.Status != model.StatusDISMISSED {
		t.Errorf("Status = %q, want DISMISSED", job.Status)
	}
}

func TestJobQueue_CancelRefusesFinishedJob(t *testing.T) {
	q := newTestJobQueue()
	jobID := pushJob(t, q)
	finishJob(t, q, jobID)

	if q.Cancel(jobID) {
		t.Error("expected Cancel to refuse a Job that has already finished")
	}
}

func TestJobQueue_CancelUnknownIdIsFalse(t *testing.T) {
	q := newTestJobQueue()

	if q.Cancel(uuid.NewString()) {
		t.Error("expected Cancel to report false for an unknown id")
	}
}

func TestJobQueue_ListReturnsPushedJobs(t *testing.T) {
	q := newTestJobQueue()
	jobID := pushJob(t, q)

	list := q.List()
	if len(list.Jobs) != 1 || list.Jobs[0].Id != jobID {
		t.Errorf("List() = %+v, want the one pushed Job", list.Jobs)
	}
}

func TestJobQueue_TakenReturnsTakenPartialJobs(t *testing.T) {
	q := newTestJobQueue()
	pushJob(t, q)

	if taken := q.Taken(); len(taken.PartialJobs) != 0 {
		t.Fatalf("Taken() before Take = %+v, want empty", taken.PartialJobs)
	}

	partialJob, err := q.backend.Take(testPartialJobKind, "test")
	if err != nil || partialJob == nil {
		t.Fatalf("Take: %v, %+v", err, partialJob)
	}

	taken := q.Taken()
	if len(taken.PartialJobs) != 1 || taken.PartialJobs[0].Id != partialJob.Id {
		t.Errorf("Taken() = %+v, want the taken PartialJob", taken.PartialJobs)
	}
}

func TestJobQueue_FailedReturnsFailedPartialJobs(t *testing.T) {
	q := newTestJobQueue()
	pushJob(t, q)

	partialJob, err := q.backend.Take(testPartialJobKind, "test")
	if err != nil || partialJob == nil {
		t.Fatalf("Take: %v, %+v", err, partialJob)
	}
	if err := q.backend.Error(partialJob.Id, "boom", false); err != nil {
		t.Fatalf("Error: %v", err)
	}

	failed := q.Failed()
	if len(failed.PartialJobs) != 1 || failed.PartialJobs[0].Id != partialJob.Id {
		t.Errorf("Failed() = %+v, want the failed PartialJob", failed.PartialJobs)
	}
}

func TestJobQueue_ListsAreEmptyNotNil(t *testing.T) {
	q := newTestJobQueue()

	if q.List().Jobs == nil || q.Taken().PartialJobs == nil || q.Failed().PartialJobs == nil {
		t.Error("expected empty lists, not nil, so they serialize as []")
	}
}
