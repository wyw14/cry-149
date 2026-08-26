package feed

import (
	"fmt"
	"sync"
	"time"
)

type ControlMode string

const (
	ModeAutomatic ControlMode = "automatic"
	ModeManual    ControlMode = "manual"
	ModePaused    ControlMode = "paused"
)

type Ownership struct {
	BatchID   string      `json:"batch_id"`
	Mode      ControlMode `json:"mode"`
	Setpoint  float64     `json:"setpoint"`
	UpdatedAt time.Time   `json:"updated_at"`
}

type State struct {
	mu     sync.RWMutex
	owners map[string]Ownership
}

func NewState() *State {
	return &State{owners: map[string]Ownership{}}
}

func (s *State) Set(vesselID string, ownership Ownership) error {
	if vesselID == "" || ownership.BatchID == "" {
		return fmt.Errorf("vessel and batch are required")
	}
	if ownership.Mode != ModeAutomatic && ownership.Mode != ModeManual && ownership.Mode != ModePaused {
		return fmt.Errorf("invalid feed mode %q", ownership.Mode)
	}
	s.mu.Lock()
	s.owners[vesselID] = ownership
	s.mu.Unlock()
	return nil
}

func (s *State) Release(vesselID, batchID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.owners[vesselID]
	if !ok || value.BatchID != batchID {
		return false
	}
	delete(s.owners, vesselID)
	return true
}

func (s *State) Snapshot() map[string]Ownership {
	s.mu.RLock()
	defer s.mu.RUnlock()
	values := make(map[string]Ownership, len(s.owners))
	for key, value := range s.owners {
		values[key] = value
	}
	return values
}
