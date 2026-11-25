package jobs

import (
	"context"
	"sync"
	"time"
)

// jobListenerResync is how often every watched Job is re-read regardless of
// events, so a listener still gets the final state of its Job when the
// event announcing it was published while the subscription was down.
const jobListenerResync = 30 * time.Second

// redisJobListeners holds the JobListeners PushJobListen registered in this
// process. A Job may be advanced by any process sharing the queue, so the
// listener cannot be called where the change happens, the way MemoryBackend
// does it: every process publishes the id of a Job it changed, and the one
// holding the listener re-reads the Job and calls it.
//
// The subscription only runs while there is something to watch, and a
// listener is dropped once its Job has finished, so nothing outlives the
// Jobs it reports on.
type redisJobListeners struct {
	mu        sync.Mutex
	listeners map[string]JobListener
	pending   map[string]bool
	signal    chan struct{}
	stop      context.CancelFunc
}

// watchJob registers listener for jobID, starting the subscription if it is
// the first one. It returns once the subscription is in place, so the
// caller can push the Job afterwards without missing its first change.
func (b *RedisBackend) watchJob(jobID string, listener JobListener) {
	w := &b.jobListeners
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.listeners == nil {
		w.listeners = map[string]JobListener{}
		w.pending = map[string]bool{}
	}
	w.listeners[jobID] = listener

	if w.stop == nil {
		// A fresh signal per subscription, so a delivery goroutine of an
		// earlier one that has not noticed its cancellation yet cannot take
		// this one's events and read with a cancelled context.
		w.signal = make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		w.stop = cancel
		b.subscribeJobChanges(ctx, w.signal)
	}
}

func (b *RedisBackend) unwatchJob(jobID string) {
	w := &b.jobListeners
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.listeners, jobID)
	delete(w.pending, jobID)
	if len(w.listeners) == 0 && w.stop != nil {
		w.stop()
		w.stop = nil
	}
}

func (b *RedisBackend) subscribeJobChanges(ctx context.Context, signal <-chan struct{}) {
	pubsub := b.client.Subscribe(ctx, b.channelJob)
	if _, err := pubsub.Receive(ctx); err != nil {
		// go-redis keeps retrying in the background; the resync covers
		// whatever is published until it succeeds.
		b.Logger.Warn().Err(err).Msg("subscribing to job changes failed")
	}
	messages := pubsub.Channel()

	go func() {
		defer pubsub.Close()
		resync := time.NewTicker(jobListenerResync)
		defer resync.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-messages:
				if !ok {
					return
				}
				b.markJobsChanged(msg.Payload)
			case <-resync.C:
				b.markJobsChanged(b.watchedJobs()...)
			}
		}
	}()

	go b.deliverJobChanges(ctx, signal)
}

func (b *RedisBackend) watchedJobs() []string {
	w := &b.jobListeners
	w.mu.Lock()
	defer w.mu.Unlock()

	ids := make([]string, 0, len(w.listeners))
	for id := range w.listeners {
		ids = append(ids, id)
	}
	return ids
}

// markJobsChanged queues the watched ones among jobIDs for delivery. Several
// changes before delivery collapse into one read of the latest state.
func (b *RedisBackend) markJobsChanged(jobIDs ...string) {
	w := &b.jobListeners
	w.mu.Lock()
	marked := false
	for _, id := range jobIDs {
		if w.listeners[id] != nil {
			w.pending[id] = true
			marked = true
		}
	}
	signal := w.signal
	w.mu.Unlock()

	if marked {
		select {
		case signal <- struct{}{}:
		default:
		}
	}
}

// deliverJobChanges runs apart from the message loop so reading the Jobs
// back never holds up the subscription.
func (b *RedisBackend) deliverJobChanges(ctx context.Context, signal <-chan struct{}) {
	w := &b.jobListeners
	for {
		select {
		case <-ctx.Done():
			return
		case <-signal:
		}

		w.mu.Lock()
		changed := make(map[string]JobListener, len(w.pending))
		for id := range w.pending {
			changed[id] = w.listeners[id]
		}
		clear(w.pending)
		w.mu.Unlock()

		for jobID, listener := range changed {
			b.deliverJobChange(ctx, jobID, listener)
		}
	}
}

func (b *RedisBackend) deliverJobChange(ctx context.Context, jobID string, listener JobListener) {
	job, err := b.getJob(ctx, jobID)
	if err != nil {
		b.Logger.Warn().Err(err).Str("job", jobID).Msg("reading changed job failed")
		return
	}
	if job == nil {
		b.unwatchJob(jobID)
		return
	}

	listener.OnProgress(*job)
	if IsSettled(job) {
		b.unwatchJob(jobID)
	}
}
