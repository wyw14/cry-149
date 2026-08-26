package service

import (
	"testing"
	"time"

	"github.com/wyw14/cry-149/internal/model"
	"github.com/wyw14/cry-149/internal/probe"
)

func TestBatchProbeFeedLifecycle(t *testing.T) {
	runtime, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	_, created, plan, err := runtime.Inoculate("FV-101", "standard-fed-batch")
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) == 0 || !runtime.batchFlow.Active(created.ID) {
		t.Fatal("batch did not enter active recipe flow")
	}
	_, output, err := runtime.ReceiveProbe(probe.Reading{
		ProbeID: "DO-101", BatchID: created.ID, VesselID: created.VesselID,
		Kind: "dissolved-oxygen", Value: 35, At: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if output.Agitation <= 0 {
		t.Fatal("oxygen controller did not produce an output")
	}
	if _, err := runtime.ScheduleFeed(created.ID, created.VesselID, 4.5, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runtime.Advance(created.ID, model.PhaseGrowing); err != nil {
		t.Fatal(err)
	}
}
