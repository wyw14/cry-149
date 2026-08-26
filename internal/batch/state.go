package batch

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/wyw14/cry-149/internal/model"
)

type State struct {
	mu       sync.RWMutex
	batches  map[string]model.Batch
	byVessel map[string]string
}

func NewState() *State {
	return &State{batches: map[string]model.Batch{}, byVessel: map[string]string{}}
}

func (s *State) Create(vesselID, recipeID string, now time.Time) (model.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing := s.byVessel[vesselID]; existing != "" {
		return model.Batch{}, fmt.Errorf("vessel %s is assigned to batch %s", vesselID, existing)
	}
	created, err := model.NewBatch(vesselID, recipeID, now)
	if err != nil {
		return model.Batch{}, err
	}
	s.batches[created.ID] = created.Clone()
	s.byVessel[vesselID] = created.ID
	return created.Clone(), nil
}

func (s *State) Transition(id string, phase model.Phase, now time.Time) (model.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, exists := s.batches[id]
	if !exists {
		return model.Batch{}, fmt.Errorf("batch %s not found", id)
	}
	if err := value.Transition(phase, now); err != nil {
		return model.Batch{}, err
	}
	s.batches[id] = value.Clone()
	if phase == model.PhaseReleased {
		delete(s.byVessel, value.VesselID)
	}
	return value.Clone(), nil
}

func (s *State) Active(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.batches[id]
	return ok && value.Active()
}

func (s *State) List() []model.Batch {
	s.mu.RLock()
	values := make([]model.Batch, 0, len(s.batches))
	for _, value := range s.batches {
		values = append(values, value.Clone())
	}
	s.mu.RUnlock()
	sort.Slice(values, func(i, j int) bool { return values[i].StartedAt.Before(values[j].StartedAt) })
	return values
}

func (s *State) Restore(values map[string]model.Batch) error {
	copyValues := map[string]model.Batch{}
	byVessel := map[string]string{}
	for id, value := range values {
		if value.ID != id || value.VesselID == "" || !model.ValidPhase(value.Phase) {
			return fmt.Errorf("invalid recovered batch %s", id)
		}
		copyValues[id] = value.Clone()
		if value.Phase != model.PhaseReleased {
			byVessel[value.VesselID] = id
		}
	}
	s.mu.Lock()
	s.batches = copyValues
	s.byVessel = byVessel
	s.mu.Unlock()
	return nil
}
