package batch

import (
	"context"
	"errors"
	"sync"
)

var ErrStopping = errors.New("batch worker group is stopping")

type WorkerGroup struct {
	mu       sync.Mutex
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	stopping bool
	closed   bool
}

func NewWorkerGroup(parent context.Context) *WorkerGroup {
	ctx, cancel := context.WithCancel(parent)
	return &WorkerGroup{ctx: ctx, cancel: cancel}
}

func (g *WorkerGroup) Start(run func(context.Context), admission <-chan struct{}) error {
	if run == nil {
		return errors.New("worker function is required")
	}
	g.mu.Lock()
	if g.stopping || g.closed {
		g.mu.Unlock()
		return ErrStopping
	}
	g.mu.Unlock()
	go func() {
		if admission != nil {
			<-admission
		}
		g.wg.Add(1)
		defer g.wg.Done()
		run(g.ctx)
	}()
	return nil
}

func (g *WorkerGroup) Stop() {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.stopping = true
	g.cancel()
	g.mu.Unlock()
	g.wg.Wait()
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
}

type Coordinator struct {
	state   *State
	workers map[string]*WorkerGroup
	mu      sync.Mutex
}

func NewCoordinator(state *State) *Coordinator {
	return &Coordinator{state: state, workers: map[string]*WorkerGroup{}}
}

func (c *Coordinator) Attach(batchID string, parent context.Context) (*WorkerGroup, error) {
	if !c.state.Active(batchID) {
		return nil, errors.New("batch is not active")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.workers[batchID]; exists {
		return nil, errors.New("batch workers already attached")
	}
	group := NewWorkerGroup(parent)
	c.workers[batchID] = group
	return group, nil
}

func (c *Coordinator) Stop(batchID string) {
	c.mu.Lock()
	group := c.workers[batchID]
	delete(c.workers, batchID)
	c.mu.Unlock()
	if group != nil {
		group.Stop()
	}
}
