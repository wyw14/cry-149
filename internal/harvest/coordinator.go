package harvest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-149/internal/vessel"
)

type Availability func(attempt int) bool

type Coordinator struct {
	router  *Router
	state   *State
	backoff Backoff
	now     func() time.Time
}

func NewCoordinator(router *Router, state *State, backoff Backoff, now func() time.Time) *Coordinator {
	if now == nil {
		now = time.Now
	}
	return &Coordinator{router: router, state: state, backoff: backoff, now: now}
}

func (c *Coordinator) Transfer(ctx context.Context, batchID, vesselID string, route vessel.Route, available Availability) (Transfer, error) {
	if batchID == "" || vesselID == "" || available == nil {
		return Transfer{}, errors.New("batch, vessel and availability are required")
	}
	transfer := Transfer{
		ID: uuid.NewString(), BatchID: batchID, VesselID: vesselID,
		Route: route.Clone(), State: "waiting", UpdatedAt: c.now(),
	}
	c.state.Set(transfer)
	for attempt := 0; attempt < 3; attempt++ {
		if available(attempt) && c.router.reserve(transfer.ID, route) {
			transfer.State = "active"
			transfer.UpdatedAt = c.now()
			c.state.Set(transfer)
			return transfer, nil
		}
		if c.backoff == nil {
			continue
		}
		if err := c.backoff.Wait(ctx, attempt); err != nil {
			transfer.State = "cancelled"
			transfer.UpdatedAt = c.now()
			c.state.Set(transfer)
			c.router.release(transfer.ID)
			return Transfer{}, err
		}
	}
	transfer.State = "unavailable"
	transfer.UpdatedAt = c.now()
	c.state.Set(transfer)
	return Transfer{}, fmt.Errorf("no harvest route became available")
}

func (c *Coordinator) Complete(id string) bool {
	transfer, ok := c.state.Get(id)
	if !ok {
		return false
	}
	c.router.release(id)
	transfer.State = "complete"
	transfer.UpdatedAt = c.now()
	c.state.Set(transfer)
	return true
}

func (c *Coordinator) Preview(route vessel.Route, blocked map[string]bool) vessel.Route {
	preview := route.Clone()
	preview.MarkCleaning(blocked)
	return preview
}

func (c *Coordinator) Active() map[string]vessel.Route {
	return c.router.Active()
}
