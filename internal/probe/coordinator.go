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
	r.mu.Lock()
	r.calibrations[calibration.ProbeID] = calibration
	listeners := make([]listener, 0, len(r.listeners))
	for id, item := range r.listeners {
		select {
		case <-item.done:
			delete(r.listeners, id)
		default:
			listeners = append(listeners, item)
		}
	}
	r.mu.Unlock()
	delivered := 0
	for _, item := range listeners {
		select {
		case <-item.done:
		case item.stream <- calibration:
			delivered++
		default:
		}
	}
	return delivered
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
