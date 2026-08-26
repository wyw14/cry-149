package service

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/wyw14/cry-149/internal/model"
)

func (r *Runtime) save() error {
	snapshot := r.batchRecovery.Snapshot(time.Now())
	r.mu.RLock()
	for key, value := range r.operations {
		snapshot.Operations[key] = value
	}
	for _, value := range r.fleet.Equipment() {
		snapshot.Equipment[value.ID] = value
	}
	for key, value := range r.interlocks {
		snapshot.Interlocks[key] = value
	}
	for key, value := range r.incidents {
		snapshot.Incidents[key] = value
	}
	r.mu.RUnlock()
	if err := r.snapshots.Save(snapshot); err != nil {
		return fmt.Errorf("save runtime state: %w", err)
	}
	return nil
}

func (r *Runtime) restore() error {
	snapshot, err := r.snapshots.Load()
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("restore runtime state: %w", err)
	}
	if err := r.batchRecovery.Restore(snapshot); err != nil {
		return fmt.Errorf("restore batches: %w", err)
	}
	r.mu.Lock()
	r.operations = map[string]model.Operation{}
	for _, value := range snapshot.OperationList() {
		r.operations[value.ID] = value
	}
	r.interlocks = map[string]model.Interlock{}
	for _, value := range snapshot.InterlockList() {
		r.interlocks[value.ID] = value
	}
	r.incidents = map[string]model.Incident{}
	for _, value := range snapshot.IncidentList() {
		r.incidents[value.ID] = value
	}
	r.mu.Unlock()
	return nil
}

func (r *Runtime) Checkpoint() error {
	r.mu.RLock()
	closed := r.closed
	r.mu.RUnlock()
	if closed {
		return fmt.Errorf("runtime is closed")
	}
	return r.save()
}
