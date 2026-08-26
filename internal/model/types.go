package model

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Phase string

const (
	PhaseInoculated Phase = "inoculated"
	PhaseGrowing    Phase = "growing"
	PhaseFeeding    Phase = "feeding"
	PhaseHarvesting Phase = "harvesting"
	PhaseCleaning   Phase = "cleaning"
	PhaseReleased   Phase = "released"
)

var phaseTransitions = map[Phase]map[Phase]bool{
	PhaseInoculated: {PhaseGrowing: true, PhaseCleaning: true},
	PhaseGrowing:    {PhaseFeeding: true, PhaseHarvesting: true, PhaseCleaning: true},
	PhaseFeeding:    {PhaseHarvesting: true, PhaseCleaning: true},
	PhaseHarvesting: {PhaseCleaning: true},
	PhaseCleaning:   {PhaseReleased: true},
	PhaseReleased:   {},
}

type Batch struct {
	ID        string            `json:"id"`
	VesselID  string            `json:"vessel_id"`
	RecipeID  string            `json:"recipe_id"`
	Phase     Phase             `json:"phase"`
	Revision  string            `json:"revision"`
	StartedAt time.Time         `json:"started_at"`
	UpdatedAt time.Time         `json:"updated_at"`
	Labels    map[string]string `json:"labels"`
}

func NewBatch(vesselID, recipeID string, now time.Time) (Batch, error) {
	vesselID = strings.TrimSpace(vesselID)
	recipeID = strings.TrimSpace(recipeID)
	if vesselID == "" || recipeID == "" {
		return Batch{}, errors.New("vessel_id and recipe_id are required")
	}
	return Batch{
		ID: uuid.NewString(), VesselID: vesselID, RecipeID: recipeID,
		Phase: PhaseInoculated, Revision: uuid.NewString(),
		StartedAt: now, UpdatedAt: now, Labels: map[string]string{},
	}, nil
}

func (b Batch) Clone() Batch {
	clone := b
	clone.Labels = make(map[string]string, len(b.Labels))
	for key, value := range b.Labels {
		clone.Labels[key] = value
	}
	return clone
}

func (b *Batch) Transition(next Phase, now time.Time) error {
	allowed, known := phaseTransitions[b.Phase]
	if !known {
		return fmt.Errorf("unknown current phase %q", b.Phase)
	}
	if !allowed[next] {
		return fmt.Errorf("phase transition %s to %s is not allowed", b.Phase, next)
	}
	b.Phase = next
	b.Revision = uuid.NewString()
	b.UpdatedAt = now
	return nil
}

func (b Batch) Active() bool {
	return b.Phase != PhaseReleased && b.Phase != PhaseCleaning
}

func ValidPhase(value Phase) bool {
	_, ok := phaseTransitions[value]
	return ok
}
