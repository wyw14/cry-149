package utility

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

var ErrLeaseBusy = errors.New("utility circuit is leased")

type Clock interface {
	Now() time.Time
	Monotonic() time.Duration
}

type SystemClock struct {
	started time.Time
}

func NewSystemClock() *SystemClock {
	return &SystemClock{started: time.Now()}
}

func (c *SystemClock) Now() time.Time {
	return time.Now()
}

func (c *SystemClock) Monotonic() time.Duration {
	return time.Since(c.started)
}

type Lease struct {
	ID          string        `json:"id"`
	Resource    string        `json:"resource"`
	Owner       string        `json:"owner"`
	IssuedAt    time.Time     `json:"issued_at"`
	Duration    time.Duration `json:"duration"`
	expiresAt   time.Duration
	expiresWall time.Time
}

type LeaseManager struct {
	mu     sync.Mutex
	clock  Clock
	leases map[string]Lease
}

func NewLeaseManager(clock Clock) *LeaseManager {
	if clock == nil {
		clock = NewSystemClock()
	}
	return &LeaseManager{clock: clock, leases: map[string]Lease{}}
}

func (m *LeaseManager) Acquire(resource, owner string, duration time.Duration) (Lease, error) {
	if resource == "" || owner == "" || duration <= 0 {
		return Lease{}, errors.New("resource, owner and positive duration are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireLocked()
	if current, exists := m.leases[resource]; exists && current.Owner != owner {
		return Lease{}, fmt.Errorf("%w: %s owned by %s", ErrLeaseBusy, resource, current.Owner)
	}
	lease := Lease{
		ID: uuid.NewString(), Resource: resource, Owner: owner,
		IssuedAt: m.clock.Now(), Duration: duration,
		expiresAt:   m.clock.Monotonic() + duration,
		expiresWall: m.clock.Now().Add(duration),
	}
	m.leases[resource] = lease
	return lease, nil
}

func (m *LeaseManager) Renew(resource, owner string, duration time.Duration) (Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireLocked()
	lease, exists := m.leases[resource]
	if !exists || lease.Owner != owner {
		return Lease{}, fmt.Errorf("owner %s has no lease for %s", owner, resource)
	}
	lease.ID = uuid.NewString()
	lease.IssuedAt = m.clock.Now()
	lease.Duration = duration
	lease.expiresAt = m.clock.Monotonic() + duration
	lease.expiresWall = m.clock.Now().Add(duration)
	m.leases[resource] = lease
	return lease, nil
}

func (m *LeaseManager) Release(resource, owner string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	lease, exists := m.leases[resource]
	if !exists || lease.Owner != owner {
		return false
	}
	delete(m.leases, resource)
	return true
}

func (m *LeaseManager) Owner(resource string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireLocked()
	lease, exists := m.leases[resource]
	return lease.Owner, exists
}

func (m *LeaseManager) Active() []Lease {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireLocked()
	values := make([]Lease, 0, len(m.leases))
	for _, lease := range m.leases {
		values = append(values, lease)
	}
	return values
}

func (m *LeaseManager) expireLocked() {
	now := m.clock.Now()
	for resource, lease := range m.leases {
		if !now.Before(lease.expiresWall) {
			delete(m.leases, resource)
		}
	}
}
