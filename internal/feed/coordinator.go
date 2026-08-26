package feed

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrBackpressure = errors.New("feed vessel queue is congested")

type worker struct {
	queue     chan Pulse
	sink      Sink
	lifecycle Lifecycle
	cancel    context.CancelFunc
}

type Scheduler struct {
	mu       sync.RWMutex
	limit    int
	now      func() time.Time
	workers  map[string]*worker
	rejected map[string]int
	closed   bool
}

func NewScheduler(limit int, now func() time.Time) *Scheduler {
	if limit < 1 {
		limit = 1
	}
	if now == nil {
		now = time.Now
	}
	return &Scheduler{
		limit: limit, now: now, workers: map[string]*worker{},
		rejected: map[string]int{},
	}
}

func (s *Scheduler) Register(vesselID string, sink Sink, lifecycle Lifecycle) error {
	if vesselID == "" || sink == nil || lifecycle == nil {
		return errors.New("vessel, sink and lifecycle are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("scheduler is closed")
	}
	if _, exists := s.workers[vesselID]; exists {
		return fmt.Errorf("vessel %s already registered", vesselID)
	}
	ctx, cancel := context.WithCancel(context.Background())
	item := &worker{queue: make(chan Pulse, s.limit), sink: sink, lifecycle: lifecycle, cancel: cancel}
	s.workers[vesselID] = item
	go s.run(ctx, item)
	return nil
}

func (s *Scheduler) run(ctx context.Context, item *worker) {
	for {
		select {
		case <-ctx.Done():
			return
		case pulse := <-item.queue:
			// Re-validate at delivery: a pulse may have expired while waiting
			// behind a slow vessel. Dropping it here prevents a burst of stale
			// pulses from being flushed onto the link when it recovers.
			if s.stale(pulse, item.lifecycle) {
				s.reject(pulse.VesselID)
				continue
			}
			_ = item.sink.Apply(ctx, pulse)
		}
	}
}

// stale reports whether pulse must no longer be delivered because its batch is
// no longer active or its expiry has elapsed. It is checked at both enqueue
// and delivery so that validity is enforced at the moment of application.
func (s *Scheduler) stale(pulse Pulse, lifecycle Lifecycle) bool {
	return !lifecycle.Active(pulse.BatchID) || !pulse.ExpiresAt.After(s.now())
}

// reject records that a pulse for vesselID was dropped due to congestion
// (backpressure or post-queue expiry) so slow vessels stay observable.
func (s *Scheduler) reject(vesselID string) {
	s.mu.Lock()
	s.rejected[vesselID]++
	s.mu.Unlock()
}

func (s *Scheduler) Schedule(pulse Pulse) error {
	s.mu.RLock()
	item, exists := s.workers[pulse.VesselID]
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return errors.New("scheduler is closed")
	}
	if !exists {
		return fmt.Errorf("vessel %s is not registered", pulse.VesselID)
	}
	if !item.lifecycle.Active(pulse.BatchID) || !pulse.ExpiresAt.After(s.now()) {
		return errors.New("feed pulse is no longer valid for the active batch")
	}
	// Backpressure: when the per-vessel queue is full the sink is not keeping
	// up (a slow/stalled vessel). Rather than spawning an unbounded backlog of
	// blocked goroutines — which would let memory climb while the link is slow
	// and then flush every stale pulse at once when it recovers — we reject the
	// pulse so the caller surfaces backpressure immediately. The worker also
	// re-validates expiry at delivery, so anything that aged out while queued
	// is dropped instead of being applied.
	select {
	case item.queue <- pulse:
		return nil
	default:
		s.reject(pulse.VesselID)
		return fmt.Errorf("%w: vessel %s", ErrBackpressure, pulse.VesselID)
	}
}

func (s *Scheduler) Pending(vesselID string) int {
	s.mu.RLock()
	item := s.workers[vesselID]
	s.mu.RUnlock()
	if item == nil {
		return 0
	}
	return len(item.queue)
}

func (s *Scheduler) Rejected(vesselID string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rejected[vesselID]
}

func (s *Scheduler) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	workers := make([]*worker, 0, len(s.workers))
	for _, item := range s.workers {
		workers = append(workers, item)
	}
	s.mu.Unlock()
	for _, item := range workers {
		item.cancel()
	}
}
