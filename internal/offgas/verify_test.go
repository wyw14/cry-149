package offgas_test

import (
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/wyw14/cry-149/internal/journal"
	"github.com/wyw14/cry-149/internal/offgas"
	"github.com/wyw14/cry-149/internal/oxygen"
)

type dataEOFReader struct {
	data []byte
	done bool
}

func (r *dataEOFReader) Read(target []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	return copy(target, r.data), io.EOF
}

func TestOffgasReaderConsumesFinalRecordWithEOF(t *testing.T) {
	store, err := journal.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	recorder := journal.NewRecorder(store)
	defer recorder.Close()
	window := oxygen.NewWindow(8)
	coordinator := offgas.NewCoordinator(window, recorder)
	record := offgas.Record{BatchID: "B-1", VesselID: "FV-101", Oxygen: 17.2, Carbon: 4.1, At: time.Now()}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	count, err := coordinator.Import(&dataEOFReader{data: data})
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("imported %d records", count)
	}
	values := window.Values(record.BatchID)
	if len(values) != 1 || values[0].Value != record.Oxygen {
		t.Fatalf("final point missing from control window: %#v", values)
	}
	history := recorder.History()
	if len(history) != 1 || history[0].Kind != "offgas.sample" || history[0].BatchID != record.BatchID {
		t.Fatalf("final point missing from journal: %#v", history)
	}
}
