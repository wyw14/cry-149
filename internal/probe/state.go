package probe

import (
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/cry-149/internal/recipe"
)

type Reading struct {
	ProbeID  string    `json:"probe_id"`
	BatchID  string    `json:"batch_id"`
	VesselID string    `json:"vessel_id"`
	Kind     string    `json:"kind"`
	Value    float64   `json:"value"`
	At       time.Time `json:"at"`
}

type Catalog struct {
	mu       sync.RWMutex
	policies map[string]*recipe.OptionalProbePolicy
	latest   map[string]Reading
}

func NewCatalog() *Catalog {
	return &Catalog{policies: map[string]*recipe.OptionalProbePolicy{}, latest: map[string]Reading{}}
}

func (c *Catalog) Configure(name string, threshold float64, below bool) {
	c.mu.Lock()
	c.policies[name] = &recipe.OptionalProbePolicy{Probe: name, Threshold: threshold, Below: below}
	c.mu.Unlock()
}

func (c *Catalog) Optional(name string) *recipe.OptionalProbePolicy {
	c.mu.RLock()
	defer c.mu.RUnlock()
	policy := c.policies[name]
	if policy == nil {
		return nil
	}
	copyPolicy := *policy
	return &copyPolicy
}

func (c *Catalog) Record(reading Reading) error {
	if reading.ProbeID == "" || reading.VesselID == "" {
		return fmt.Errorf("probe and vessel are required")
	}
	c.mu.Lock()
	previous, exists := c.latest[reading.ProbeID]
	if exists && reading.At.Before(previous.At) {
		c.mu.Unlock()
		return fmt.Errorf("probe reading is older than current state")
	}
	c.latest[reading.ProbeID] = reading
	c.mu.Unlock()
	return nil
}

func (c *Catalog) Latest(probeID string) (Reading, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	reading, ok := c.latest[probeID]
	return reading, ok
}
