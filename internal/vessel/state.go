package vessel

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/cry-149/internal/model"
)

var ErrOccupied = errors.New("vessel is already assigned")

type State struct {
	mu          sync.RWMutex
	ID          string
	BatchID     string
	Phase       model.Phase
	Temperature float64
	Pressure    float64
	UpdatedAt   time.Time
	route       Route
}

func NewState(id string, now time.Time) *State {
	return &State{ID: id, Phase: model.PhaseReleased, UpdatedAt: now, route: Route{}}
}

func (s *State) Bind(batchID string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.BatchID != "" && s.BatchID != batchID {
		return ErrOccupied
	}
	s.BatchID = batchID
	s.Phase = model.PhaseInoculated
	s.UpdatedAt = now
	return nil
}

func (s *State) Advance(batchID string, phase model.Phase, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if batchID == "" || batchID != s.BatchID {
		return fmt.Errorf("batch %s does not own vessel %s", batchID, s.ID)
	}
	if !model.ValidPhase(phase) {
		return fmt.Errorf("invalid vessel phase %q", phase)
	}
	s.Phase = phase
	s.UpdatedAt = now
	if phase == model.PhaseReleased {
		s.BatchID = ""
		s.route = Route{}
	}
	return nil
}

func (s *State) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Snapshot{
		ID: s.ID, BatchID: s.BatchID, Phase: s.Phase,
		Temperature: s.Temperature, Pressure: s.Pressure,
		UpdatedAt: s.UpdatedAt, Route: s.route.Clone(),
	}
}

func (s *State) SetRoute(route Route) {
	s.mu.Lock()
	s.route = route.Clone()
	s.mu.Unlock()
}

type Snapshot struct {
	ID          string      `json:"id"`
	BatchID     string      `json:"batch_id,omitempty"`
	Phase       model.Phase `json:"phase"`
	Temperature float64     `json:"temperature"`
	Pressure    float64     `json:"pressure"`
	UpdatedAt   time.Time   `json:"updated_at"`
	Route       Route       `json:"route"`
}
