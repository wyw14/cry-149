package harvest

import (
	"sync"
	"time"

	"github.com/wyw14/cry-149/internal/vessel"
)

type Transfer struct {
	ID        string       `json:"id"`
	BatchID   string       `json:"batch_id"`
	VesselID  string       `json:"vessel_id"`
	Route     vessel.Route `json:"route"`
	State     string       `json:"state"`
	UpdatedAt time.Time    `json:"updated_at"`
}

type State struct {
	mu        sync.RWMutex
	transfers map[string]Transfer
}

func NewState() *State {
	return &State{transfers: map[string]Transfer{}}
}

func (s *State) Set(transfer Transfer) {
	transfer.Route = transfer.Route.Clone()
	s.mu.Lock()
	s.transfers[transfer.ID] = transfer
	s.mu.Unlock()
}

func (s *State) Get(id string) (Transfer, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	transfer, ok := s.transfers[id]
	transfer.Route = transfer.Route.Clone()
	return transfer, ok
}

func (s *State) List() []Transfer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	values := make([]Transfer, 0, len(s.transfers))
	for _, transfer := range s.transfers {
		copyTransfer := transfer
		copyTransfer.Route = transfer.Route.Clone()
		values = append(values, copyTransfer)
	}
	return values
}
