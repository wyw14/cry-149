package oxygen

import (
	"sync"
	"time"
)

type Sample struct {
	BatchID  string    `json:"batch_id"`
	VesselID string    `json:"vessel_id"`
	Value    float64   `json:"value"`
	At       time.Time `json:"at"`
}

type Window struct {
	mu       sync.RWMutex
	capacity int
	values   map[string][]Sample
}

func NewWindow(capacity int) *Window {
	if capacity < 1 {
		capacity = 1
	}
	return &Window{capacity: capacity, values: map[string][]Sample{}}
}

func (w *Window) Append(sample Sample) {
	w.mu.Lock()
	values := append(w.values[sample.BatchID], sample)
	if len(values) > w.capacity {
		values = append([]Sample(nil), values[len(values)-w.capacity:]...)
	}
	w.values[sample.BatchID] = values
	w.mu.Unlock()
}

func (w *Window) Values(batchID string) []Sample {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return append([]Sample(nil), w.values[batchID]...)
}

func (w *Window) Latest(batchID string) (Sample, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	values := w.values[batchID]
	if len(values) == 0 {
		return Sample{}, false
	}
	return values[len(values)-1], true
}
