package utility_test

import (
	"testing"
	"time"

	"github.com/wyw14/cry-149/internal/utility"
)

type adjustableClock struct {
	wall time.Time
	mono time.Duration
}

func (c *adjustableClock) Now() time.Time           { return c.wall }
func (c *adjustableClock) Monotonic() time.Duration { return c.mono }
func (c *adjustableClock) Step(wall time.Duration, mono time.Duration) {
	c.wall = c.wall.Add(wall)
	c.mono += mono
}

func TestUtilityLeaseExpirySurvivesWallClockRollback(t *testing.T) {
	clock := &adjustableClock{wall: time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC)}
	manager := utility.NewLeaseManager(clock)
	if _, err := manager.Acquire("hot-water", "CIP-12", time.Minute); err != nil {
		t.Fatal(err)
	}
	clock.Step(-time.Hour, 2*time.Minute)
	if owner, active := manager.Owner("hot-water"); active {
		t.Fatalf("expired lease remained active for %s after wall clock rollback", owner)
	}
	lease, err := manager.Acquire("hot-water", "CIP-NEW", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Owner != "CIP-NEW" {
		t.Fatalf("new owner did not acquire circuit: %#v", lease)
	}
}
