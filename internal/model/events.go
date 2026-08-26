package model

import (
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID         string         `json:"id"`
	Kind       string         `json:"kind"`
	BatchID    string         `json:"batch_id,omitempty"`
	VesselID   string         `json:"vessel_id,omitempty"`
	At         time.Time      `json:"at"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

func NewEvent(kind, batchID, vesselID string, at time.Time, attributes map[string]any) Event {
	copyAttributes := make(map[string]any, len(attributes))
	for key, value := range attributes {
		copyAttributes[key] = value
	}
	return Event{
		ID: uuid.NewString(), Kind: kind, BatchID: batchID,
		VesselID: vesselID, At: at, Attributes: copyAttributes,
	}
}

type Operation struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	BatchID   string    `json:"batch_id,omitempty"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Detail    string    `json:"detail,omitempty"`
}

func NewOperation(kind, batchID string, now time.Time) Operation {
	return Operation{
		ID: uuid.NewString(), Kind: kind, BatchID: batchID,
		State: "accepted", CreatedAt: now, UpdatedAt: now,
	}
}

func (o Operation) WithState(state, detail string, now time.Time) Operation {
	o.State = state
	o.Detail = detail
	o.UpdatedAt = now
	return o
}

type Equipment struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	State     string    `json:"state"`
	BatchID   string    `json:"batch_id,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Interlock struct {
	ID       string `json:"id"`
	Resource string `json:"resource"`
	Owner    string `json:"owner"`
	Reason   string `json:"reason"`
	Active   bool   `json:"active"`
}

type Incident struct {
	ID        string    `json:"id"`
	Severity  string    `json:"severity"`
	Component string    `json:"component"`
	Message   string    `json:"message"`
	OpenedAt  time.Time `json:"opened_at"`
}

func NewIncident(severity, component, message string, now time.Time) Incident {
	return Incident{
		ID: uuid.NewString(), Severity: severity, Component: component,
		Message: message, OpenedAt: now,
	}
}
