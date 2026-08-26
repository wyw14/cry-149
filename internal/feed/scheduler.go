package feed

import (
	"context"
	"sync"
	"time"
)

type Pulse struct {
	ID        string    `json:"id"`
	BatchID   string    `json:"batch_id"`
	VesselID  string    `json:"vessel_id"`
	AmountML  float64   `json:"amount_ml"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

type Sink interface {
	Apply(context.Context, Pulse) error
}

type Lifecycle interface {
	Active(batchID string) bool
}

type MemorySink struct {
	mu     sync.Mutex
	pulses []Pulse
	delay  <-chan struct{}
}

func NewMemorySink(delay <-chan struct{}) *MemorySink {
	return &MemorySink{delay: delay, pulses: []Pulse{}}
}

func (s *MemorySink) Apply(ctx context.Context, pulse Pulse) error {
	if s.delay != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.delay:
		}
	}
	s.mu.Lock()
	s.pulses = append(s.pulses, pulse)
	s.mu.Unlock()
	return nil
}
