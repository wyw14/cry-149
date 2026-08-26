package utility

import (
	"context"
	"fmt"
	"time"
)

type Coordinator struct {
	leases     *LeaseManager
	dispatcher *Dispatcher
}

func NewCoordinator(leases *LeaseManager, dispatcher *Dispatcher) *Coordinator {
	return &Coordinator{leases: leases, dispatcher: dispatcher}
}

func (c *Coordinator) Begin(ctx context.Context, resource, owner, action string, duration time.Duration) (Lease, error) {
	lease, err := c.leases.Acquire(resource, owner, duration)
	if err != nil {
		return Lease{}, err
	}
	if err := c.dispatcher.Dispatch(ctx, Command{Resource: resource, Action: action, Owner: owner}); err != nil {
		c.leases.Release(resource, owner)
		return Lease{}, fmt.Errorf("begin utility operation: %w", err)
	}
	return lease, nil
}

func (c *Coordinator) Continue(ctx context.Context, resource, owner, action string, duration time.Duration) (Lease, error) {
	lease, err := c.leases.Renew(resource, owner, duration)
	if err != nil {
		return Lease{}, err
	}
	if err := c.dispatcher.Dispatch(ctx, Command{Resource: resource, Action: action, Owner: owner}); err != nil {
		return Lease{}, fmt.Errorf("continue utility operation: %w", err)
	}
	return lease, nil
}

func (c *Coordinator) End(ctx context.Context, resource, owner, action string) error {
	if err := c.dispatcher.Dispatch(ctx, Command{Resource: resource, Action: action, Owner: owner}); err != nil {
		return err
	}
	if !c.leases.Release(resource, owner) {
		return fmt.Errorf("utility lease for %s is not owned by %s", resource, owner)
	}
	return nil
}

func (c *Coordinator) Owner(resource string) (string, bool) {
	return c.leases.Owner(resource)
}
