package utility

import (
	"errors"
	"sync"
	"time"
)

type ActionRecord struct {
	Name      string    `json:"name"`
	State     string    `json:"state"`
	Owner     string    `json:"owner"`
	UpdatedAt time.Time `json:"updated_at"`
}

type State struct {
	mu      sync.RWMutex
	actions map[string]ActionRecord
	order   []string
}

func NewState() *State {
	return &State{actions: map[string]ActionRecord{}, order: []string{}}
}

func (s *State) Set(name, state, owner string, now time.Time) error {
	if name == "" || state == "" {
		return errors.New("utility action and state are required")
	}
	s.mu.Lock()
	s.actions[name] = ActionRecord{Name: name, State: state, Owner: owner, UpdatedAt: now}
	s.order = append(s.order, name+":"+state)
	s.mu.Unlock()
	return nil
}

func (s *State) Get(name string) (ActionRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.actions[name]
	return value, ok
}

func (s *State) Order() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]string(nil), s.order...)
}

func ReverseIndexes(length int) []int {
	indexes := make([]int, length)
	for index := 0; index < length; index++ {
		indexes[index] = length - index - 1
	}
	return indexes
}
