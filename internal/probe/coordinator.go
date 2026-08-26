package probe

import (
	"fmt"
	"sync"
	"time"
)

type Calibration struct {
	ProbeID string    `json:"probe_id"`
	Offset  float64   `json:"offset"`
	Slope   float64   `json:"slope"`
	At      time.Time `json:"at"`
}

type listener struct {
	id     string
	stream chan Calibration
	done   <-chan struct{}
}

type Registry struct {
	mu           sync.RWMutex
	calibrations map[string]Calibration
	listeners    map[string]listener
}

func NewRegistry() *Registry {
	return &Registry{calibrations: map[string]Calibration{}, listeners: map[string]listener{}}
}

func (r *Registry) Subscribe(id string, buffer int) (<-chan Calibration, func(), error) {
	if id == "" {
		return nil, nil, fmt.Errorf("listener id is required")
	}
	if buffer < 1 {
		buffer = 1
	}
	done := make(chan struct{})
	stream := make(chan Calibration, buffer)
	r.mu.Lock()
	if _, exists := r.listeners[id]; exists {
		r.mu.Unlock()
		return nil, nil, fmt.Errorf("listener %s already registered", id)
	}
	r.listeners[id] = listener{id: id, stream: stream, done: done}
	r.mu.Unlock()
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			close(done)
			r.mu.Lock()
			delete(r.listeners, id)
			r.mu.Unlock()
		})
	}
	return stream, cancel, nil
}

func (r *Registry) Broadcast(calibration Calibration) int {
	// Snapshot the listeners under the lock, then release it before any
	// channel send. A blocking send held while the registry lock is held is the
	// classic head-of-line stall: one listener whose stream is full (its batch
	// consumer has exited or stalled) wedges the entire registry, hanging every
	// reader of r.mu (Get, ListenerCount, Subscribe, even the listener's own
	// cancel). By sending only after releasing the lock, no single listener can
	// block the rest or the registry.
	r.mu.Lock()
	r.calibrations[calibration.ProbeID] = calibration
	pending := make([]listener, 0, len(r.listeners))
	for id, item := range r.listeners {
		select {
		case <-item.done:
			// Listener signaled done; prune it so a stale entry never survives.
			delete(r.listeners, id)
		default:
			pending = append(pending, item)
		}
	}
	r.mu.Unlock()

	delivered := 0
	for _, item := range pending {
		// Non-blocking send: a listener whose stream is full (consumer gone or
		// slow) cannot stall the broadcast. Drop this delivery and evict the
		// listener so it stops absorbing future broadcasts and clogging Get/
		// ListenerCount — the "abandoned listener blocks global broadcast" path.
		select {
		case item.stream <- calibration:
			delivered++
		default:
			r.evict(item.id)
		}
	}
	return delivered
}

// evict removes a listener that can no longer keep up with broadcasts. It is
// safe to call concurrently with Broadcast/Subscribe since it re-acquires the
// lock, and it tolerates the listener already being gone.
func (r *Registry) evict(id string) {
	r.mu.Lock()
	delete(r.listeners, id)
	r.mu.Unlock()
}

func (r *Registry) Get(probeID string) (Calibration, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	value, ok := r.calibrations[probeID]
	return value, ok
}

func (r *Registry) ListenerCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.listeners)
}
