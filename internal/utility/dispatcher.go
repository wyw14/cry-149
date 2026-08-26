package utility

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Command struct {
	Resource string
	Action   string
	Owner    string
}

type Driver interface {
	Execute(context.Context, Command) error
}

type Dispatcher struct {
	mu      sync.RWMutex
	drivers map[string]Driver
	state   *State
}

func NewDispatcher(state *State) *Dispatcher {
	return &Dispatcher{drivers: map[string]Driver{}, state: state}
}

func (d *Dispatcher) Register(resource string, driver Driver) error {
	if resource == "" || driver == nil {
		return fmt.Errorf("resource and driver are required")
	}
	d.mu.Lock()
	d.drivers[resource] = driver
	d.mu.Unlock()
	return nil
}

func (d *Dispatcher) Dispatch(ctx context.Context, command Command) error {
	d.mu.RLock()
	driver := d.drivers[command.Resource]
	d.mu.RUnlock()
	if driver == nil {
		return fmt.Errorf("utility driver for %s not found", command.Resource)
	}
	if err := driver.Execute(ctx, command); err != nil {
		return fmt.Errorf("execute %s on %s: %w", command.Action, command.Resource, err)
	}
	return d.state.Set(command.Resource, command.Action, command.Owner, time.Now())
}

type MemoryDriver struct {
	mu       sync.Mutex
	commands []Command
}

func NewMemoryDriver() *MemoryDriver {
	return &MemoryDriver{commands: []Command{}}
}

func (d *MemoryDriver) Execute(_ context.Context, command Command) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.commands = append(d.commands, command)
	return nil
}

func (d *MemoryDriver) Commands() []Command {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]Command(nil), d.commands...)
}
