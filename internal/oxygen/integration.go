package oxygen

import (
	"fmt"
	"sync"
	"time"
)

type ControllerBank struct {
	mu          sync.RWMutex
	controllers map[string]*Controller
	window      *Window
}

func NewControllerBank(window *Window) *ControllerBank {
	return &ControllerBank{controllers: map[string]*Controller{}, window: window}
}

func (b *ControllerBank) Register(batchID string, target float64, now time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.controllers[batchID]; exists {
		return fmt.Errorf("oxygen controller for batch %s already exists", batchID)
	}
	b.controllers[batchID] = NewController(target, now)
	return nil
}

func (b *ControllerBank) Observe(sample Sample) (Output, error) {
	b.window.Append(sample)
	b.mu.RLock()
	controller := b.controllers[sample.BatchID]
	b.mu.RUnlock()
	if controller == nil {
		return Output{}, fmt.Errorf("oxygen controller for batch %s not found", sample.BatchID)
	}
	return controller.Observe(sample.Value, sample.At), nil
}

func (b *ControllerBank) Mode(batchID string, mode Mode, output Output, now time.Time) (Output, error) {
	b.mu.RLock()
	controller := b.controllers[batchID]
	b.mu.RUnlock()
	if controller == nil {
		return Output{}, fmt.Errorf("oxygen controller for batch %s not found", batchID)
	}
	return controller.SetMode(mode, output, now), nil
}

func (b *ControllerBank) State(batchID string) (Mode, float64, Output, error) {
	b.mu.RLock()
	controller := b.controllers[batchID]
	b.mu.RUnlock()
	if controller == nil {
		return "", 0, Output{}, fmt.Errorf("oxygen controller for batch %s not found", batchID)
	}
	mode, integral, output := controller.State()
	return mode, integral, output, nil
}

func (b *ControllerBank) Remove(batchID string) {
	b.mu.Lock()
	delete(b.controllers, batchID)
	b.mu.Unlock()
}
