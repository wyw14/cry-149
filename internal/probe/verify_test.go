package probe_test

import (
	"testing"
	"time"

	"github.com/wyw14/cry-149/internal/probe"
)

func TestProbeCalibrationSkipsAbandonedListenerWithoutBlocking(t *testing.T) {
	registry := probe.NewRegistry()
	_, _, err := registry.Subscribe("ended-batch", 1)
	if err != nil {
		t.Fatal(err)
	}
	active, _, err := registry.Subscribe("active-batch", 2)
	if err != nil {
		t.Fatal(err)
	}
	first := probe.Calibration{ProbeID: "PH-101", Slope: 1, At: time.Now()}
	registry.Broadcast(first)
	select {
	case <-active:
	case <-time.After(time.Second):
		t.Fatal("active listener missed initial calibration")
	}
	done := make(chan int, 1)
	second := probe.Calibration{ProbeID: "PH-101", Offset: 0.2, Slope: 1, At: time.Now()}
	go func() { done <- registry.Broadcast(second) }()
	select {
	case delivered := <-done:
		if delivered != 1 {
			t.Fatalf("delivered to %d listeners, want one active listener", delivered)
		}
	case <-time.After(150 * time.Millisecond):
		t.Fatal("abandoned listener blocked calibration broadcast")
	}
	if current, ok := registry.Get("PH-101"); !ok || current.Offset != second.Offset {
		t.Fatalf("registry state unavailable after broadcast: %#v %v", current, ok)
	}
}
