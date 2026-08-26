package recipe

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type StepKind string

const (
	StepHold    StepKind = "hold"
	StepFeed    StepKind = "feed"
	StepAerate  StepKind = "aerate"
	StepHarvest StepKind = "harvest"
)

type Step struct {
	ID        string        `json:"id"`
	Kind      StepKind      `json:"kind"`
	Duration  time.Duration `json:"duration"`
	Setpoint  float64       `json:"setpoint"`
	Condition string        `json:"condition,omitempty"`
}

type Plan struct {
	ID       string `json:"id"`
	Revision string `json:"revision"`
	Steps    []Step `json:"steps"`
}

func (p Plan) Clone() Plan {
	clone := p
	clone.Steps = append([]Step(nil), p.Steps...)
	return clone
}

func (p Plan) Validate() error {
	if p.ID == "" || p.Revision == "" {
		return errors.New("plan id and revision are required")
	}
	if len(p.Steps) == 0 {
		return errors.New("plan must have steps")
	}
	seen := map[string]bool{}
	for _, step := range p.Steps {
		if step.ID == "" || seen[step.ID] {
			return fmt.Errorf("invalid or duplicate step id %q", step.ID)
		}
		if step.Duration < 0 {
			return fmt.Errorf("step %s has negative duration", step.ID)
		}
		seen[step.ID] = true
	}
	return nil
}

type Registry struct {
	mu    sync.RWMutex
	plans map[string]Plan
}

func NewRegistry() *Registry {
	return &Registry{plans: map[string]Plan{}}
}

func (r *Registry) Put(plan Plan) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	r.plans[plan.ID] = plan.Clone()
	r.mu.Unlock()
	return nil
}

func (r *Registry) Get(id string) (Plan, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	plan, ok := r.plans[id]
	if !ok {
		return Plan{}, fmt.Errorf("recipe %s not found", id)
	}
	return plan.Clone(), nil
}

func DefaultPlan() Plan {
	return Plan{
		ID: "standard-fed-batch", Revision: "r1",
		Steps: []Step{
			{ID: "warm", Kind: StepHold, Duration: 20 * time.Minute, Setpoint: 31},
			{ID: "grow", Kind: StepAerate, Duration: 3 * time.Hour, Setpoint: 40},
			{ID: "feed", Kind: StepFeed, Duration: 6 * time.Hour, Setpoint: 18},
			{ID: "harvest", Kind: StepHarvest, Setpoint: 1},
		},
	}
}
