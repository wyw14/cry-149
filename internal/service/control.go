package service

import (
	"context"
	"fmt"
	"time"

	"github.com/wyw14/cry-149/internal/harvest"
	"github.com/wyw14/cry-149/internal/model"
	"github.com/wyw14/cry-149/internal/offgas"
	"github.com/wyw14/cry-149/internal/oxygen"
	"github.com/wyw14/cry-149/internal/vessel"
)

func (r *Runtime) Events() []model.Event {
	return r.journal.History()
}

func (r *Runtime) ImportOffgasRecords(records []offgas.Record) (model.Operation, int, error) {
	return r.ImportOffgas(records)
}

func (r *Runtime) SetOxygenMode(batchID string, mode oxygen.Mode, output oxygen.Output) (oxygen.Output, error) {
	return r.oxygenBank.Mode(batchID, mode, output, time.Now())
}

func (r *Runtime) ControlStatus(batchID, probeID string) map[string]any {
	values := r.oxygenWindow.Values(batchID)
	latest, hasLatest := r.oxygenWindow.Latest(batchID)
	mode, integral, output, modeErr := r.oxygenBank.State(batchID)
	plan, planErr := r.recipeFlow.Running(batchID)
	reading, hasReading := r.probes.Latest(probeID)
	calibration, hasCalibration := r.probeRegistry.Get(probeID)
	return map[string]any{
		"batch_id": batchID, "probe_id": probeID,
		"oxygen_samples": values, "latest_oxygen": latest, "has_latest_oxygen": hasLatest,
		"mode": mode, "integral": integral, "output": output, "mode_error": errorText(modeErr),
		"plan": plan, "plan_error": errorText(planErr),
		"reading": reading, "has_reading": hasReading,
		"calibration": calibration, "has_calibration": hasCalibration,
		"feed_ownership": r.feedState.Snapshot(),
	}
}

func (r *Runtime) FeedStatus(vesselID string) map[string]int {
	return map[string]int{"pending": r.feed.Pending(vesselID), "rejected": r.feed.Rejected(vesselID)}
}

func (r *Runtime) PreviewHarvestRoute(route vessel.Route, blocked map[string]bool) (vessel.Route, bool) {
	preview := r.PreviewHarvest(route, blocked)
	return preview, preview.Available()
}

func (r *Runtime) StartHarvest(ctx context.Context, batchID, vesselID string, route vessel.Route) (harvest.Transfer, error) {
	transfer, err := r.harvest.Transfer(ctx, batchID, vesselID, route, func(int) bool { return true })
	if err != nil {
		return harvest.Transfer{}, err
	}
	state, err := r.fleet.Get(vesselID)
	if err != nil {
		r.harvest.Complete(transfer.ID)
		return harvest.Transfer{}, err
	}
	state.SetRoute(route)
	return transfer, nil
}

func (r *Runtime) CompleteHarvest(id string) bool {
	return r.harvest.Complete(id)
}

func (r *Runtime) HarvestStatus() map[string]any {
	return map[string]any{"transfers": r.harvestState.List(), "active_routes": r.harvest.Active()}
}

func (r *Runtime) CIPStatus(vesselID string) (any, bool) {
	value, ok := r.cipState.Get(vesselID)
	return value, ok
}

func (r *Runtime) UtilityStatus(resource string) map[string]any {
	owner, leased := r.utility.Owner(resource)
	state, hasState := r.utilityState.Get(resource)
	return map[string]any{
		"resource": resource, "owner": owner, "leased": leased,
		"state": state, "has_state": hasState,
		"action_order": r.utilityState.Order(), "commands": r.utilityDriver.Commands(),
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprint(err)
}
