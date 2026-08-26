package service

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-149/internal/cip"
	"github.com/wyw14/cry-149/internal/feed"
	"github.com/wyw14/cry-149/internal/journal"
	"github.com/wyw14/cry-149/internal/model"
	"github.com/wyw14/cry-149/internal/offgas"
	"github.com/wyw14/cry-149/internal/oxygen"
	"github.com/wyw14/cry-149/internal/probe"
	"github.com/wyw14/cry-149/internal/recipe"
	"github.com/wyw14/cry-149/internal/vessel"
)

func (r *Runtime) Inoculate(vesselID, recipeID string) (model.Operation, model.Batch, recipe.Plan, error) {
	now := time.Now()
	operation := model.NewOperation("batch.inoculate", "", now)
	created, plan, err := r.batchFlow.Inoculate(vesselID, recipeID, now)
	if err != nil {
		r.finishOperation(operation.WithState("failed", err.Error(), time.Now()))
		return operation, model.Batch{}, recipe.Plan{}, err
	}
	operation.BatchID = created.ID
	if err := r.attachCalibrationWorker(created.ID); err != nil {
		r.finishOperation(operation.WithState("failed", err.Error(), time.Now()))
		return operation, model.Batch{}, recipe.Plan{}, err
	}
	operation = operation.WithState("complete", "batch accepted and recipe expanded", time.Now())
	r.finishOperation(operation)
	return operation, created, plan, nil
}

func (r *Runtime) Advance(batchID string, phase model.Phase) (model.Operation, model.Batch, error) {
	operation := model.NewOperation("batch.advance", batchID, time.Now())
	updated, err := r.batchFlow.Advance(batchID, phase, time.Now())
	if err != nil {
		r.finishOperation(operation.WithState("failed", err.Error(), time.Now()))
		return operation, model.Batch{}, err
	}
	operation = operation.WithState("complete", string(phase), time.Now())
	if phase == model.PhaseReleased {
		r.batchWorkers.Stop(batchID)
	}
	r.finishOperation(operation)
	return operation, updated, nil
}

func (r *Runtime) ScheduleFeed(batchID, vesselID string, amount float64, validity time.Duration) (model.Operation, error) {
	operation := model.NewOperation("feed.schedule", batchID, time.Now())
	pulse := feed.Pulse{
		ID: uuid.NewString(), BatchID: batchID, VesselID: vesselID,
		AmountML: amount, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(validity),
	}
	if err := r.feed.Schedule(pulse); err != nil {
		r.finishOperation(operation.WithState("failed", err.Error(), time.Now()))
		return operation, err
	}
	operation = operation.WithState("complete", pulse.ID, time.Now())
	r.finishOperation(operation)
	return operation, nil
}

func (r *Runtime) ReceiveProbe(reading probe.Reading) (model.Operation, oxygen.Output, error) {
	operation := model.NewOperation("probe.receive", reading.BatchID, time.Now())
	output, err := r.probeReceiver.Receive(reading)
	if err != nil {
		r.finishOperation(operation.WithState("failed", err.Error(), time.Now()))
		return operation, oxygen.Output{}, err
	}
	operation = operation.WithState("complete", reading.ProbeID, time.Now())
	r.finishOperation(operation)
	return operation, output, nil
}

func (r *Runtime) ImportOffgas(records []offgas.Record) (model.Operation, int, error) {
	operation := model.NewOperation("offgas.import", "", time.Now())
	count, err := r.offgas.Import(bytes.NewReader(offgas.Encode(records)))
	if err != nil {
		r.finishOperation(operation.WithState("failed", err.Error(), time.Now()))
		return operation, count, err
	}
	operation = operation.WithState("complete", fmt.Sprintf("%d records", count), time.Now())
	r.finishOperation(operation)
	return operation, count, nil
}

func (r *Runtime) PreviewHarvest(route vessel.Route, blocked map[string]bool) vessel.Route {
	return r.harvest.Preview(route, blocked)
}

func (r *Runtime) RunCIP(ctx context.Context, plan cip.Plan) (model.Operation, error) {
	operation := model.NewOperation("cip.run", plan.ID, time.Now())
	if err := r.cip.Execute(ctx, plan); err != nil {
		r.finishOperation(operation.WithState("failed", err.Error(), time.Now()))
		message := err.Error()
		if failure, ok := journal.FindStepFailure(err); ok {
			message = fmt.Sprintf("step %d %s: %v", failure.Index, failure.Step, failure.Err)
		}
		r.openIncident("critical", "cip", message)
		return operation, fmt.Errorf("cleaning failed")
	}
	operation = operation.WithState("complete", plan.VesselID, time.Now())
	r.finishOperation(operation)
	return operation, nil
}

func (r *Runtime) attachCalibrationWorker(batchID string) error {
	group, err := r.batchWorkers.Attach(batchID, context.Background())
	if err != nil {
		return err
	}
	stream, cancel, err := r.probeRegistry.Subscribe("batch:"+batchID, 8)
	if err != nil {
		return err
	}
	return group.Start(func(ctx context.Context) {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case calibration := <-stream:
				_, _ = r.journal.Record("probe.calibrated", batchID, "", map[string]any{
					"probe_id": calibration.ProbeID, "offset": calibration.Offset, "slope": calibration.Slope,
				})
			}
		}
	}, nil)
}

func (r *Runtime) CalibrateProbe(calibration probe.Calibration) int {
	return r.probeRegistry.Broadcast(calibration)
}

func (r *Runtime) finishOperation(operation model.Operation) {
	r.mu.Lock()
	r.operations[operation.ID] = operation
	r.mu.Unlock()
}

func (r *Runtime) openIncident(severity, component, message string) model.Incident {
	incident := model.NewIncident(severity, component, message, time.Now())
	r.mu.Lock()
	r.incidents[incident.ID] = incident
	r.mu.Unlock()
	return incident
}
