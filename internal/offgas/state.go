package offgas

import "sync"

type AnalyzerState struct {
	mu      sync.RWMutex
	latest  map[string]Record
	invalid map[string]int
}

func NewAnalyzerState() *AnalyzerState {
	return &AnalyzerState{latest: map[string]Record{}, invalid: map[string]int{}}
}

func (s *AnalyzerState) Accept(record Record) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous, exists := s.latest[record.VesselID]
	if exists && !record.At.After(previous.At) {
		s.invalid[record.VesselID]++
		return false
	}
	s.latest[record.VesselID] = record
	return true
}
