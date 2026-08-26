package utility

import (
	"fmt"
	"sync"
	"time"
)

type RouteClaim struct {
	RouteID   string    `json:"route_id"`
	Owner     string    `json:"owner"`
	Segments  []string  `json:"segments"`
	CreatedAt time.Time `json:"created_at"`
}

type RouteClaims struct {
	mu     sync.RWMutex
	claims map[string]RouteClaim
}

func NewRouteClaims() *RouteClaims {
	return &RouteClaims{claims: map[string]RouteClaim{}}
}

func (c *RouteClaims) Reserve(claim RouteClaim) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, active := range c.claims {
		if active.Owner == claim.Owner {
			continue
		}
		if overlaps(active.Segments, claim.Segments) {
			return fmt.Errorf("route %s conflicts with %s", claim.RouteID, active.RouteID)
		}
	}
	claim.Segments = append([]string(nil), claim.Segments...)
	c.claims[claim.RouteID] = claim
	return nil
}

func (c *RouteClaims) Release(routeID, owner string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	claim, exists := c.claims[routeID]
	if !exists || claim.Owner != owner {
		return false
	}
	delete(c.claims, routeID)
	return true
}

func (c *RouteClaims) Snapshot() []RouteClaim {
	c.mu.RLock()
	defer c.mu.RUnlock()
	values := make([]RouteClaim, 0, len(c.claims))
	for _, claim := range c.claims {
		copyClaim := claim
		copyClaim.Segments = append([]string(nil), claim.Segments...)
		values = append(values, copyClaim)
	}
	return values
}

func overlaps(left, right []string) bool {
	seen := make(map[string]bool, len(left))
	for _, value := range left {
		seen[value] = true
	}
	for _, value := range right {
		if seen[value] {
			return true
		}
	}
	return false
}
