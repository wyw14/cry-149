package cip

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Action func(context.Context) error

type Step struct {
	Name       string
	Duration   time.Duration
	Apply      Action
	Compensate Action
}

type Plan struct {
	ID       string
	VesselID string
	Steps    []Step
}

func (p Plan) Validate() error {
	if p.ID == "" || p.VesselID == "" {
		return errors.New("cleaning plan and vessel are required")
	}
	if len(p.Steps) == 0 {
		return errors.New("cleaning plan needs steps")
	}
	for index, step := range p.Steps {
		if step.Name == "" || step.Apply == nil || step.Compensate == nil {
			return fmt.Errorf("cleaning step %d is incomplete", index)
		}
	}
	return nil
}
