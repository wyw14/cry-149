package harvest_test

import (
	"testing"
	"time"

	"github.com/wyw14/cry-149/internal/harvest"
	"github.com/wyw14/cry-149/internal/vessel"
)

func TestHarvestPreviewCannotMutateLiveRouteSegments(t *testing.T) {
	route, err := vessel.NewRoute("R-10", "B-10", []vessel.Segment{
		{ID: "S-1", From: "FV-101", To: "HX-1", Open: true},
		{ID: "S-2", From: "HX-1", To: "TK-1", Open: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := harvest.NewCoordinator(harvest.NewRouter(), harvest.NewState(), nil, time.Now)
	preview := coordinator.Preview(route, map[string]bool{"S-2": true})
	if !preview.Segments[1].RequiresCleaning {
		t.Fatal("preview did not apply simulated cleaning condition")
	}
	if route.Segments[1].RequiresCleaning {
		t.Fatal("preview changed caller route")
	}
	state := vessel.NewState("FV-101", time.Now())
	state.SetRoute(route)
	snapshot := state.Snapshot()
	snapshot.Route.Segments[0].RequiresCleaning = true
	again := state.Snapshot()
	if again.Route.Segments[0].RequiresCleaning {
		t.Fatal("snapshot changed live vessel route")
	}
}
