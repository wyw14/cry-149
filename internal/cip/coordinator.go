package cip

import (
	"context"
	"fmt"
	"time"

	"github.com/wyw14/cry-149/internal/journal"
	"github.com/wyw14/cry-149/internal/utility"
)

type Coordinator struct {
	state    *State
	recorder *journal.Recorder
	now      func() time.Time
}

func NewCoordinator(state *State, recorder *journal.Recorder, now func() time.Time) *Coordinator {
	if now == nil {
		now = time.Now
	}
	return &Coordinator{state: state, recorder: recorder, now: now}
}

func (c *Coordinator) Run(ctx context.Context, plan Plan) error {
	if err := c.state.Begin(plan, c.now()); err != nil {
		return err
	}
	completed := make([]Step, 0, len(plan.Steps))
	for index, step := range plan.Steps {
		c.state.Step(plan.VesselID, step.Name, c.now())
		if err := step.Apply(ctx); err != nil {
			failure := journal.NewStepFailure(step.Name, index, err)
			rollback := c.compensate(ctx, completed)
			c.state.Finish(plan.VesselID, "failed", c.now())
			_, _ = c.recorder.Record("cip.failed", plan.ID, plan.VesselID, map[string]any{"step": step.Name})
			return journal.JoinFailures(append([]error{failure}, rollback...))
		}
		completed = append(completed, step)
		_, _ = c.recorder.Record("cip.step", plan.ID, plan.VesselID, map[string]any{"step": step.Name})
	}
	c.state.Finish(plan.VesselID, "complete", c.now())
	_, err := c.recorder.Record("cip.complete", plan.ID, plan.VesselID, nil)
	return err
}

func (c *Coordinator) compensate(ctx context.Context, completed []Step) []error {
	indexes := utility.ReverseIndexes(len(completed))
	failures := make([]error, 0)
	for _, index := range indexes {
		step := completed[index]
		if err := step.Compensate(ctx); err != nil {
			failures = append(failures, fmt.Errorf("compensate %s: %w", step.Name, err))
		}
	}
	return failures
}
