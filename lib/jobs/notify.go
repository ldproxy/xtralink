package jobs

import (
	"context"
	"sync"
)

// Notifications are the callbacks Backend.Listen delivers to. Either may be
// nil. They run on a goroutine owned by the subscription, never while the
// backend holds a lock, so they may call back into the backend.
type Notifications struct {
	// OnWork fires when a PartialJob may have become takeable: one was
	// pushed or re-queued, or a Sequence advanced and opened a gate. It
	// carries no payload because the receiver rescans its queues anyway,
	// and repeated events may be coalesced into one.
	OnWork func()
	// OnCancel fires once per Job that Backend.Cancel dismissed.
	OnCancel func(jobID string)
}

// subscriber hands events from a backend to one Listen caller without ever
// blocking the backend: events are recorded under the subscriber's own lock
// and delivered by its goroutine.
type subscriber struct {
	mu        sync.Mutex
	work      bool
	cancelled []string
	signal    chan struct{}
}

func newSubscriber() *subscriber {
	return &subscriber{signal: make(chan struct{}, 1)}
}

func (s *subscriber) notifyWork() {
	s.mu.Lock()
	s.work = true
	s.mu.Unlock()
	s.poke()
}

func (s *subscriber) notifyCancel(jobID string) {
	s.mu.Lock()
	s.cancelled = append(s.cancelled, jobID)
	s.mu.Unlock()
	s.poke()
}

func (s *subscriber) poke() {
	select {
	case s.signal <- struct{}{}:
	default:
	}
}

func (s *subscriber) deliver(ctx context.Context, n Notifications) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.signal:
		}

		s.mu.Lock()
		work, cancelled := s.work, s.cancelled
		s.work, s.cancelled = false, nil
		s.mu.Unlock()

		for _, jobID := range cancelled {
			if n.OnCancel != nil {
				n.OnCancel(jobID)
			}
		}
		if work && n.OnWork != nil {
			n.OnWork()
		}
	}
}
