package journal

import (
	"errors"
	"fmt"
)

type StepFailure struct {
	Step  string
	Index int
	Err   error
}

func (e *StepFailure) Error() string {
	return fmt.Sprintf("cleaning step %d %s: %v", e.Index, e.Step, e.Err)
}

func (e *StepFailure) Unwrap() error {
	return e.Err
}

func NewStepFailure(step string, index int, err error) error {
	if err == nil {
		return nil
	}
	return &StepFailure{Step: step, Index: index, Err: err}
}

func FindStepFailure(err error) (*StepFailure, bool) {
	var failure *StepFailure
	if !errors.As(err, &failure) {
		return nil, false
	}
	return failure, true
}

func JoinFailures(failures []error) error {
	values := make([]error, 0, len(failures))
	for _, failure := range failures {
		if failure != nil {
			values = append(values, failure)
		}
	}
	return errors.Join(values...)
}
