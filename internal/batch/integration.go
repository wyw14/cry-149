package batch

import (
	"fmt"
	"time"

	"github.com/wyw14/cry-149/internal/feed"
	"github.com/wyw14/cry-149/internal/journal"
	"github.com/wyw14/cry-149/internal/model"
	"github.com/wyw14/cry-149/internal/oxygen"
	"github.com/wyw14/cry-149/internal/recipe"
	"github.com/wyw14/cry-149/internal/vessel"
)

type Integration struct {
	state     *State
	fleet     *vessel.Fleet
	recipes   *recipe.Coordinator
	oxygen    *oxygen.ControllerBank
	feedState *feed.State
	recorder  *journal.Recorder
}

func NewIntegration(state *State, fleet *vessel.Fleet, recipes *recipe.Coordinator, oxygenBank *oxygen.ControllerBank, feedState *feed.State, recorder *journal.Recorder) *Integration {
	return &Integration{
		state: state, fleet: fleet, recipes: recipes, oxygen: oxygenBank,
		feedState: feedState, recorder: recorder,
	}
}

func (i *Integration) Inoculate(vesselID, recipeID string, now time.Time) (model.Batch, recipe.Plan, error) {
	created, err := i.state.Create(vesselID, recipeID, now)
	if err != nil {
		return model.Batch{}, recipe.Plan{}, err
	}
	if err := i.fleet.Bind(created, now); err != nil {
		return model.Batch{}, recipe.Plan{}, fmt.Errorf("bind vessel: %w", err)
	}
	plan, err := i.recipes.Expand(created.ID, recipeID, map[string]float64{})
	if err != nil {
		return model.Batch{}, recipe.Plan{}, fmt.Errorf("expand recipe: %w", err)
	}
	if err := i.oxygen.Register(created.ID, 40, now); err != nil {
		return model.Batch{}, recipe.Plan{}, err
	}
	if err := i.feedState.Set(vesselID, feed.Ownership{
		BatchID: created.ID, Mode: feed.ModeAutomatic, UpdatedAt: now,
	}); err != nil {
		return model.Batch{}, recipe.Plan{}, err
	}
	_, err = i.recorder.Record("batch.inoculated", created.ID, vesselID, map[string]any{"recipe_id": recipeID})
	return created, plan.Clone(), err
}

func (i *Integration) Advance(batchID string, phase model.Phase, now time.Time) (model.Batch, error) {
	updated, err := i.state.Transition(batchID, phase, now)
	if err != nil {
		return model.Batch{}, err
	}
	if err := i.fleet.Advance(updated, now); err != nil {
		return model.Batch{}, err
	}
	if phase == model.PhaseReleased {
		i.oxygen.Remove(batchID)
		i.feedState.Release(updated.VesselID, batchID)
		i.recipes.Finish(batchID)
	}
	_, err = i.recorder.Record("batch.phase", batchID, updated.VesselID, map[string]any{"phase": phase})
	return updated, err
}

func (i *Integration) Active(batchID string) bool {
	return i.state.Active(batchID)
}
