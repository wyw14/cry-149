package cip

import (
	"context"
	"fmt"
	"time"

	"github.com/wyw14/cry-149/internal/utility"
)

type Executor struct {
	coordinator *Coordinator
	utilities   *utility.Coordinator
	lease       time.Duration
}

func NewExecutor(coordinator *Coordinator, utilities *utility.Coordinator, lease time.Duration) *Executor {
	return &Executor{coordinator: coordinator, utilities: utilities, lease: lease}
}

func (e *Executor) Execute(ctx context.Context, plan Plan) error {
	owner := "cip:" + plan.ID
	if _, err := e.utilities.Begin(ctx, "hot-water", owner, "open", e.lease); err != nil {
		return fmt.Errorf("acquire cleaning utility: %w", err)
	}
	if _, err := e.utilities.Continue(ctx, "hot-water", owner, "circulate", e.lease); err != nil {
		_ = e.utilities.End(ctx, "hot-water", owner, "close")
		return fmt.Errorf("renew cleaning utility: %w", err)
	}
	err := e.coordinator.Run(ctx, plan)
	endErr := e.utilities.End(ctx, "hot-water", owner, "close")
	if err != nil && endErr != nil {
		return fmt.Errorf("cleaning failed: %w", journalJoin(err, endErr))
	}
	if err != nil {
		return fmt.Errorf("cleaning failed: %w", err)
	}
	if endErr != nil {
		return fmt.Errorf("release cleaning utility: %w", endErr)
	}
	return nil
}

func journalJoin(left, right error) error {
	return fmt.Errorf("%v; release: %w", left, right)
}
