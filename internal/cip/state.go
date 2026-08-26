package cip

import (
	"fmt"
	"sync"
	"time"
)

type CycleState struct {
	PlanID    string    `json:"plan_id"`
	VesselID  string    `json:"vessel_id"`
	Step      string    `json:"step"`
	Status    string    `json:"status"`
	UpdatedAt time.Time `json:"updated_at"`
}

type State struct {
	mu     sync.RWMutex
	cycles map[string]CycleState
}

func NewState() *State {
	return &State{cycles: map[string]CycleState{}}
}

func (s *State) Begin(plan Plan, now time.Time) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, exists := s.cycles[plan.VesselID]; exists && current.Status == "running" {
		return fmt.Errorf("vessel %s already has cleaning plan %s", plan.VesselID, current.PlanID)
	}
	s.cycles[plan.VesselID] = CycleState{PlanID: plan.ID, VesselID: plan.VesselID, Status: "running", UpdatedAt: now}
	return nil
}

func (s *State) Step(vesselID, step string, now time.Time) {
	s.mu.Lock()
	value := s.cycles[vesselID]
	value.Step = step
	value.UpdatedAt = now
	s.cycles[vesselID] = value
	s.mu.Unlock()
}

func (s *State) Finish(vesselID, status string, now time.Time) {
	s.mu.Lock()
	value := s.cycles[vesselID]
	value.Status = status
	value.UpdatedAt = now
	s.cycles[vesselID] = value
	s.mu.Unlock()
}

func (s *State) Get(vesselID string) (CycleState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.cycles[vesselID]
	return value, ok
}
