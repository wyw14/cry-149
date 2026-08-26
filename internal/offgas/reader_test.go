package offgas

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"
)

func mustRecord(t *testing.T, batchID, vesselID string, at time.Time) Record {
	t.Helper()
	return Record{BatchID: batchID, VesselID: vesselID, Oxygen: 21.0, Carbon: 5.0, At: at}
}

// readAll drains the Reader the same way Coordinator.Import does.
func readAll(t *testing.T, reader *Reader) []Record {
	t.Helper()
	var records []Record
	for {
		record, err := reader.Next()
		if err == nil {
			records = append(records, record)
			continue
		}
		if errors.Is(err, io.EOF) {
			return records
		}
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReaderEmitsAllRecordsWithoutTrailingNewline(t *testing.T) {
	// bytes.Reader returns the final chunk together with io.EOF on the last
	// Read call, and Encode produces newline-terminated records — so the input
	// below ends with a trailing newline. A non-seekable reader that returns
	// the last record bytes and io.EOF in a single Read used to lose that tail.
	at := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	records := []Record{
		mustRecord(t, "B-1", "FV-101", at),
		mustRecord(t, "B-1", "FV-101", at.Add(time.Second)),
		mustRecord(t, "B-1", "FV-101", at.Add(2*time.Second)),
	}
	// Drop the trailing newline so the last record's bytes and EOF arrive
	// together on the final Read — exactly the reported data+EOF condition.
	data := bytes.TrimSuffix(Encode(records), []byte("\n"))

	got := readAll(t, NewReader(bytes.NewReader(data)))
	if len(got) != len(records) {
		t.Fatalf("expected %d records, got %d (%#v)", len(records), len(got), got)
	}
	for i := range records {
		if got[i].BatchID != records[i].BatchID || got[i].At != records[i].At {
			t.Fatalf("record %d mismatch: got %#v want %#v", i, got[i], records[i])
		}
	}
}

func TestReaderHandlesDataAndEOFInOneRead(t *testing.T) {
	// oneShotReader hands back every byte alongside io.EOF in a single Read,
	// matching the io.Reader contract and the bytes.Reader end-of-input case.
	at := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	records := []Record{
		mustRecord(t, "B-1", "FV-101", at),
		mustRecord(t, "B-1", "FV-101", at.Add(time.Second)),
	}
	reader := NewReader(&oneShotReader{payload: Encode(records)})

	got := readAll(t, reader)
	if len(got) != len(records) {
		t.Fatalf("expected %d records, got %d (%#v)", len(records), len(got), got)
	}
}

type oneShotReader struct {
	payload   []byte
	delivered bool
}

func (o *oneShotReader) Read(p []byte) (int, error) {
	if o.delivered {
		return 0, io.EOF
	}
	n := copy(p, o.payload)
	o.delivered = true
	return n, io.EOF
}
