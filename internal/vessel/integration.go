package vessel

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/wyw14/cry-149/internal/model"
)

type Fleet struct {
	mu      sync.RWMutex
	vessels map[string]*State
}

func NewFleet(ids []string, now time.Time) *Fleet {
	fleet := &Fleet{vessels: make(map[string]*State, len(ids))}
	for _, id := range ids {
		fleet.vessels[id] = NewState(id, now)
	}
	return fleet
}

func (f *Fleet) Get(id string) (*State, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	state, ok := f.vessels[id]
	if !ok {
		return nil, fmt.Errorf("vessel %s not found", id)
	}
	return state, nil
}

func (f *Fleet) Bind(batch model.Batch, now time.Time) error {
	state, err := f.Get(batch.VesselID)
	if err != nil {
		return err
	}
	return state.Bind(batch.ID, now)
}

func (f *Fleet) Advance(batch model.Batch, now time.Time) error {
	state, err := f.Get(batch.VesselID)
	if err != nil {
		return err
	}
	return state.Advance(batch.ID, batch.Phase, now)
}

func (f *Fleet) Snapshots() []Snapshot {
	f.mu.RLock()
	states := make([]*State, 0, len(f.vessels))
	for _, state := range f.vessels {
		states = append(states, state)
	}
	f.mu.RUnlock()
	values := make([]Snapshot, 0, len(states))
	for _, state := range states {
		values = append(values, state.Snapshot())
	}
	sort.Slice(values, func(i, j int) bool { return values[i].ID < values[j].ID })
	return values
}

func (f *Fleet) Equipment() []model.Equipment {
	states := f.Snapshots()
	values := make([]model.Equipment, 0, len(states))
	for _, state := range states {
		values = append(values, model.Equipment{
			ID: state.ID, Kind: "fermenter", State: string(state.Phase),
			BatchID: state.BatchID, UpdatedAt: state.UpdatedAt,
		})
	}
	return values
}
