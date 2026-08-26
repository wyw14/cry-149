package probe

import (
	"fmt"
	"time"

	"github.com/wyw14/cry-149/internal/journal"
	"github.com/wyw14/cry-149/internal/oxygen"
)

type Receiver struct {
	catalog  *Catalog
	oxygen   *oxygen.ControllerBank
	recorder *journal.Recorder
}

func NewReceiver(catalog *Catalog, bank *oxygen.ControllerBank, recorder *journal.Recorder) *Receiver {
	return &Receiver{catalog: catalog, oxygen: bank, recorder: recorder}
}

func (r *Receiver) Receive(reading Reading) (oxygen.Output, error) {
	if err := r.catalog.Record(reading); err != nil {
		return oxygen.Output{}, err
	}
	if _, err := r.recorder.Record("probe.reading", reading.BatchID, reading.VesselID, map[string]any{
		"probe_id": reading.ProbeID, "kind": reading.Kind, "value": reading.Value,
	}); err != nil {
		return oxygen.Output{}, err
	}
	if reading.Kind != "dissolved-oxygen" {
		return oxygen.Output{At: time.Now()}, nil
	}
	output, err := r.oxygen.Observe(oxygen.Sample{
		BatchID: reading.BatchID, VesselID: reading.VesselID,
		Value: reading.Value, At: reading.At,
	})
	if err != nil {
		return oxygen.Output{}, fmt.Errorf("apply oxygen reading: %w", err)
	}
	return output, nil
}
