package jobs

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ldproxy/xtralink/model"
)

// assertPushWakesRunner pushes work while the Runner is idle in a poll sleep
// far longer than the test allows, so only the push event can wake it.
func assertPushWakesRunner(t *testing.T, b Backend, cleanup func(id string)) {
	t.Helper()
	jobType := uniqueType("wake")

	var processed int32
	r := NewRunner(b, "test")
	r.PollInterval = time.Minute
	r.HousekeepingInterval = 0
	r.Register(&JobProcessor{Kind: jobType, Priority: 1000, Process: func(*model.PartialJob, *model.Job, Backend) model.JobResult {
		atomic.AddInt32(&processed, 1)
		return model.Success()
	}})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() { cancel(); <-done }()

	time.Sleep(100 * time.Millisecond) // let the Runner find nothing and go to sleep
	partialJob := NewPartialJob(uuid.NewString(), jobType, 1000, "")
	cleanup(partialJob.Id)
	if err := b.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&processed) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if atomic.LoadInt32(&processed) == 0 {
		t.Error("the push did not wake the Runner before its one-minute poll")
	}
}

func TestMemoryBackend_PushWakesRunner(t *testing.T) {
	assertPushWakesRunner(t, NewMemoryBackend(), func(string) {})
}

func TestRedisBackend_PushWakesRunner(t *testing.T) {
	b := requireRedis(t)
	assertPushWakesRunner(t, b, func(id string) { cleanupPartialJob(t, b, id) })
}

// Two RedisBackends stand in for two replicas sharing one queue: a Cancel
// on one has to reach a listener on the other.
func TestRedisBackend_CancelReachesOtherProcess(t *testing.T) {
	canceller := requireRedis(t)
	listener := requireRedis(t)

	cancelled := make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := listener.Listen(ctx, Notifications{OnCancel: func(jobID string) { cancelled <- jobID }}); err != nil {
		t.Fatalf("Listen: %v", err)
	}

	job := NewJob(uuid.NewString(), uniqueType("cancel-remote"), 1000, "", nil)
	cleanupJob(t, canceller, job.Id)
	if err := canceller.PushJob(job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	if ok, err := canceller.Cancel(job.Id); err != nil || !ok {
		t.Fatalf("Cancel = %v, %v", ok, err)
	}

	select {
	case got := <-cancelled:
		if got != job.Id {
			t.Errorf("notified %s, want %s", got, job.Id)
		}
	case <-time.After(2 * time.Second):
		t.Error("the other process was never notified of the cancel")
	}
}

func TestMemoryBackend_ListenStopsWithContext(t *testing.T) {
	b := NewMemoryBackend()
	var notified int32
	ctx, cancel := context.WithCancel(context.Background())
	if err := b.Listen(ctx, Notifications{OnWork: func() { atomic.AddInt32(&notified, 1) }}); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	cancel()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		remaining := len(b.subscribers)
		b.mu.Unlock()
		if remaining == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Error("subscriber still registered after its context was cancelled")
}

type recordingJobListener struct {
	mu   sync.Mutex
	jobs []model.Job
}

func (l *recordingJobListener) OnProgress(job model.Job) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.jobs = append(l.jobs, job)
}

func (l *recordingJobListener) last() (model.Job, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.jobs) == 0 {
		return model.Job{}, 0
	}
	return l.jobs[len(l.jobs)-1], len(l.jobs)
}

// The listener lives with the process that pushed the Job, while another
// process runs its PartialJob: every change made there has to reach it, and
// it has to be dropped once the Job is finished.
func TestRedisBackend_ListenerFollowsJobAdvancedElsewhere(t *testing.T) {
	pusher := requireRedis(t)
	worker := requireRedis(t)
	jobType := uniqueType("listen")

	job := NewJob(uuid.NewString(), jobType, 1000, "", nil)
	cleanupJob(t, pusher, job.Id)
	listener := &recordingJobListener{}
	if err := pusher.PushJobListen(job, listener); err != nil {
		t.Fatalf("PushJobListen: %v", err)
	}

	partialJob := NewPartialJob(uuid.NewString(), jobType+":worker", 1000, job.Id)
	cleanupPartialJob(t, worker, partialJob.Id)
	if err := worker.InitJob(job.Id, 1, nil); err != nil {
		t.Fatalf("InitJob: %v", err)
	}
	if err := worker.PushPartialJob(partialJob, false); err != nil {
		t.Fatalf("PushPartialJob: %v", err)
	}
	if _, err := worker.Take(partialJob.Kind, "worker"); err != nil {
		t.Fatalf("Take: %v", err)
	}
	if err := worker.StartJob(job.Id); err != nil {
		t.Fatalf("StartJob: %v", err)
	}
	if err := worker.UpdatePartialJob(partialJob.Id, 1); err != nil {
		t.Fatalf("UpdatePartialJob: %v", err)
	}
	if err := worker.Done(partialJob.Id); err != nil {
		t.Fatalf("Done: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got, _ := listener.last(); got.FinishedAt > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	got, calls := listener.last()
	if got.FinishedAt == 0 || got.Progress.Current != 1 {
		t.Fatalf("last delivered state after %d calls: finishedAt=%d current=%d, want the finished Job", calls, got.FinishedAt, got.Progress.Current)
	}
	if watched := pusher.watchedJobs(); len(watched) != 0 {
		t.Errorf("listener still registered after its Job finished: %v", watched)
	}
}
