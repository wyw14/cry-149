package model

import "time"

type Snapshot struct {
	Revision   string               `json:"revision"`
	SavedAt    time.Time            `json:"saved_at"`
	Batches    map[string]Batch     `json:"batches"`
	Operations map[string]Operation `json:"operations"`
	Equipment  map[string]Equipment `json:"equipment"`
	Interlocks map[string]Interlock `json:"interlocks"`
	Incidents  map[string]Incident  `json:"incidents"`
}

func EmptySnapshot(now time.Time) Snapshot {
	return Snapshot{
		SavedAt: now,
		Batches: map[string]Batch{}, Operations: map[string]Operation{},
		Equipment: map[string]Equipment{}, Interlocks: map[string]Interlock{},
		Incidents: map[string]Incident{},
	}
}

func (s Snapshot) OperationList() []Operation {
	values := make([]Operation, 0, len(s.Operations))
	for _, value := range s.Operations {
		values = append(values, value)
	}
	return values
}

func (s Snapshot) InterlockList() []Interlock {
	values := make([]Interlock, 0, len(s.Interlocks))
	for _, value := range s.Interlocks {
		values = append(values, value)
	}
	return values
}

func (s Snapshot) IncidentList() []Incident {
	values := make([]Incident, 0, len(s.Incidents))
	for _, value := range s.Incidents {
		values = append(values, value)
	}
	return values
}
