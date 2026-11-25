package impl

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

type recordingCancelListener struct {
	mu     sync.Mutex
	jobIDs []string
}

func (l *recordingCancelListener) OnCancel(jobID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.jobIDs = append(l.jobIDs, jobID)
}

func (l *recordingCancelListener) notified() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.jobIDs...)
}

type probeProcessor struct{ ran chan struct{} }

func (p probeProcessor) Process(model.PartialJob, model.Job) (model.JobResult, error) {
	close(p.ran)
	return model.Success(), nil
}

// listeningJobQueue runs the dispatch loop, which owns the backend
// subscription that delivers cancel notifications. Run subscribes before it
// dispatches anything, so a processed probe PartialJob proves the
// subscription is in place.
func listeningJobQueue(t *testing.T) (*JobQueue, *recordingCancelListener) {
	t.Helper()
	q := newTestJobQueue()
	listener := &recordingCancelListener{}
	q.OnCancel(listener)

	probe := probeProcessor{ran: make(chan struct{})}
	if err := q.Register("probe", 1000, probe); err != nil {
		t.Fatalf("Register: %v", err)
	}
	probeJobID := uuid.NewString()
	if err := q.backend.PushJob(jobs.NewJob(probeJobID, "probe", 1000, "", nil)); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	if err := q.backend.PushPartialJob(jobs.NewPartialJob(uuid.NewString(), "probe", 1000, probeJobID), false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		_ = q.runner.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
	})

	select {
	case <-probe.ran:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch loop never started")
	}
	return q, listener
}

func TestJobQueue_CancelNotifiesListener(t *testing.T) {
	q, listener := listeningJobQueue(t)
	jobID := pushJob(t, q)

	q.Cancel(jobID)

	deadline := time.Now().Add(2 * time.Second)
	for len(listener.notified()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := listener.notified(); len(got) != 1 || got[0] != jobID {
		t.Errorf("notified %v, want [%s]", got, jobID)
	}
}

func TestJobQueue_RefusedCancelDoesNotNotifyListener(t *testing.T) {
	q, listener := listeningJobQueue(t)
	jobID := pushJob(t, q)
	finishJob(t, q, jobID)

	q.Cancel(jobID)
	q.Cancel(uuid.NewString())

	time.Sleep(50 * time.Millisecond)
	if got := listener.notified(); len(got) != 0 {
		t.Errorf("notified %v, want none", got)
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

// WaitFor runs on a goroutine the FFI starts, so dereferencing a missing Job
// there would end the host process.
func TestJobQueue_WaitForUnknownIdIsZero(t *testing.T) {
	q := newTestJobQueue()

	if job := q.WaitFor(uuid.NewString()); job.Id != "" {
		t.Errorf("WaitFor unknown id = %+v, want zero Job", job)
	}
	if partialJob := q.WaitForPartial(uuid.NewString()); partialJob.Id != "" {
		t.Errorf("WaitForPartial unknown id = %+v, want zero PartialJob", partialJob)
	}
}

type slowProcessor struct{ finished *atomic.Bool }

func (p slowProcessor) Process(model.PartialJob, model.Job) (model.JobResult, error) {
	time.Sleep(100 * time.Millisecond)
	p.finished.Store(true)
	return model.Success(), nil
}

func TestJobQueue_StopWaitsForInFlightWork(t *testing.T) {
	q := NewJobQueue()
	var finished atomic.Bool
	if err := q.Register(testPartialJobKind, 1000, slowProcessor{&finished}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := q.Start(model.QueueConfiguration{Concurrency: 1, Executor: "test", Queue: model.QueueLOCAL}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	pushJob(t, q)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if taken, _ := q.backend.GetTaken(); len(taken) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	q.Stop()
	if !finished.Load() {
		t.Error("Stop returned while the partial job was still running")
	}
}

type recordingProcessor struct{ calls *atomic.Int32 }

func (p recordingProcessor) Process(model.PartialJob, model.Job) (model.JobResult, error) {
	p.calls.Add(1)
	return model.Success(), nil
}

func TestJobQueue_StandalonePartialJobReachesProcessor(t *testing.T) {
	q := NewJobQueue()
	var calls atomic.Int32
	if err := q.Register("standalone", 1000, recordingProcessor{&calls}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := q.Start(model.QueueConfiguration{Concurrency: 1, Executor: "test", Queue: model.QueueLOCAL}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer q.Stop()

	if err := q.backend.PushPartialJob(jobs.NewPartialJob(uuid.NewString(), "standalone", 1000, ""), false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if calls.Load() == 0 {
		failed, _ := q.backend.GetFailed()
		t.Errorf("processor never called, failed: %v", failed)
	}
}

func TestJobQueue_RestartKeepsTheMemoryQueue(t *testing.T) {
	q := NewJobQueue()
	cfg := model.QueueConfiguration{Concurrency: 1, Executor: "test", Queue: model.QueueLOCAL}
	if err := q.Start(cfg); err != nil {
		t.Fatalf("Start: %v", err)
	}
	jobID := pushJob(t, q)
	q.Stop()

	if err := q.Start(cfg); err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer q.Stop()
	if _, ok := q.Get(jobID); !ok {
		t.Error("job pushed before Stop is gone after Start")
	}
}
