package offgas

import (
	"errors"
	"fmt"
	"io"

	"github.com/wyw14/cry-149/internal/journal"
	"github.com/wyw14/cry-149/internal/oxygen"
)

type Coordinator struct {
	window   *oxygen.Window
	recorder *journal.Recorder
	state    *AnalyzerState
}

func NewCoordinator(window *oxygen.Window, recorder *journal.Recorder) *Coordinator {
	return &Coordinator{window: window, recorder: recorder, state: NewAnalyzerState()}
}

func (c *Coordinator) Import(input io.Reader) (int, error) {
	reader := NewReader(input)
	count := 0
	for {
		record, err := reader.Next()
		if err == nil {
			if !c.state.Accept(record) {
				continue
			}
			c.window.Append(oxygen.Sample{
				BatchID: record.BatchID, VesselID: record.VesselID,
				Value: record.Oxygen, At: record.At,
			})
			if _, recordErr := c.recorder.Record("offgas.sample", record.BatchID, record.VesselID, map[string]any{
				"oxygen": record.Oxygen, "carbon": record.Carbon,
			}); recordErr != nil {
				return count, recordErr
			}
			count++
			continue
		}
		if errors.Is(err, io.EOF) {
			return count, nil
		}
		return count, fmt.Errorf("import offgas at record %d: %w", count+1, err)
	}
}
