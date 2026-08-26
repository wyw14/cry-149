package service

import (
	"sort"
	"time"

	"github.com/wyw14/cry-149/internal/model"
)

func (r *Runtime) Operations() []model.Operation {
	r.mu.RLock()
	values := make([]model.Operation, 0, len(r.operations))
	for _, value := range r.operations {
		values = append(values, value)
	}
	r.mu.RUnlock()
	sort.Slice(values, func(i, j int) bool { return values[i].CreatedAt.Before(values[j].CreatedAt) })
	return values
}

func (r *Runtime) Equipment() []model.Equipment {
	values := r.fleet.Equipment()
	for _, lease := range r.leases.Active() {
		values = append(values, model.Equipment{
			ID: lease.Resource, Kind: "utility", State: "leased",
			BatchID: lease.Owner, UpdatedAt: lease.IssuedAt,
		})
	}
	sort.Slice(values, func(i, j int) bool { return values[i].ID < values[j].ID })
	return values
}

func (r *Runtime) Interlocks() []model.Interlock {
	r.mu.RLock()
	values := make([]model.Interlock, 0, len(r.interlocks))
	for _, value := range r.interlocks {
		values = append(values, value)
	}
	r.mu.RUnlock()
	for _, lease := range r.leases.Active() {
		values = append(values, model.Interlock{
			ID: "lease:" + lease.ID, Resource: lease.Resource,
			Owner: lease.Owner, Reason: "active utility lease", Active: true,
		})
	}
	sort.Slice(values, func(i, j int) bool { return values[i].ID < values[j].ID })
	return values
}

func (r *Runtime) Incidents() []model.Incident {
	r.mu.RLock()
	values := make([]model.Incident, 0, len(r.incidents))
	for _, value := range r.incidents {
		values = append(values, value)
	}
	r.mu.RUnlock()
	sort.Slice(values, func(i, j int) bool { return values[i].OpenedAt.Before(values[j].OpenedAt) })
	return values
}

func (r *Runtime) Batches() []model.Batch {
	return r.batches.List()
}

func (r *Runtime) SetInterlock(resource, owner, reason string, active bool) model.Interlock {
	interlock := model.Interlock{
		ID: resource + ":" + owner, Resource: resource,
		Owner: owner, Reason: reason, Active: active,
	}
	r.mu.Lock()
	if active {
		r.interlocks[interlock.ID] = interlock
	} else {
		delete(r.interlocks, interlock.ID)
	}
	r.mu.Unlock()
	_, _ = r.journal.Record("interlock.changed", owner, resource, map[string]any{
		"active": active, "reason": reason, "at": time.Now(),
	})
	return interlock
}
