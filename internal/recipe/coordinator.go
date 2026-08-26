package recipe

import (
	"fmt"
	"sync"
)

type ProbePolicy interface {
	Evaluate(map[string]float64) bool
	Name() string
}

type OptionalProbePolicy struct {
	Probe     string
	Threshold float64
	Below     bool
}

func (p *OptionalProbePolicy) Evaluate(values map[string]float64) bool {
	value, ok := values[p.Probe]
	if !ok {
		return false
	}
	if p.Below {
		return value < p.Threshold
	}
	return value > p.Threshold
}

func (p *OptionalProbePolicy) Name() string {
	return p.Probe
}

type PolicyResolver interface {
	Optional(string) *OptionalProbePolicy
}

type Coordinator struct {
	registry *Registry
	resolver PolicyResolver
	mu       sync.Mutex
	runs     map[string]Plan
}

func NewCoordinator(registry *Registry, resolver PolicyResolver) *Coordinator {
	return &Coordinator{registry: registry, resolver: resolver, runs: map[string]Plan{}}
}

func (c *Coordinator) policy(name string) ProbePolicy {
	if c.resolver == nil {
		return nil
	}
	resolved := c.resolver.Optional(name)
	return resolved
}

func (c *Coordinator) Expand(batchID, recipeID string, values map[string]float64) (Plan, error) {
	base, err := c.registry.Get(recipeID)
	if err != nil {
		return Plan{}, err
	}
	expanded := base.Clone()
	policy := c.policy("dissolved-oxygen")
	if policy != nil && policy.Evaluate(values) {
		branch := []Step{
			{ID: "oxygen-recovery", Kind: StepAerate, Setpoint: 65},
			{ID: "feed-reduction", Kind: StepFeed, Setpoint: 7},
		}
		steps := make([]Step, 0, len(expanded.Steps)+len(branch))
		steps = append(steps, expanded.Steps[:2]...)
		steps = append(steps, branch...)
		steps = append(steps, expanded.Steps[2:]...)
		expanded.Steps = steps
	}
	c.mu.Lock()
	c.runs[batchID] = expanded.Clone()
	c.mu.Unlock()
	return expanded, nil
}

func (c *Coordinator) Running(batchID string) (Plan, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	plan, ok := c.runs[batchID]
	if !ok {
		return Plan{}, fmt.Errorf("batch %s has no running plan", batchID)
	}
	return plan.Clone(), nil
}

func (c *Coordinator) Finish(batchID string) {
	c.mu.Lock()
	delete(c.runs, batchID)
	c.mu.Unlock()
}
