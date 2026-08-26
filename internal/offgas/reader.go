package offgas

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

type Record struct {
	BatchID  string    `json:"batch_id"`
	VesselID string    `json:"vessel_id"`
	Oxygen   float64   `json:"oxygen"`
	Carbon   float64   `json:"carbon"`
	At       time.Time `json:"at"`
}

type Reader struct {
	input io.Reader
	buf   []byte
	done  bool
}

func NewReader(input io.Reader) *Reader {
	return &Reader{input: input, buf: make([]byte, 0, 4096)}
}

func (r *Reader) Next() (Record, error) {
	for {
		if index := bytes.IndexByte(r.buf, '\n'); index >= 0 {
			line := append([]byte(nil), r.buf[:index]...)
			r.buf = append(r.buf[:0], r.buf[index+1:]...)
			return decodeRecord(line)
		}
		if r.done {
			if len(bytes.TrimSpace(r.buf)) == 0 {
				return Record{}, io.EOF
			}
			line := append([]byte(nil), r.buf...)
			r.buf = r.buf[:0]
			return decodeRecord(line)
		}
		chunk := make([]byte, 1024)
		n, err := r.input.Read(chunk)
		if n > 0 {
			r.buf = append(r.buf, chunk[:n]...)
		}
		if err != nil {
			if err == io.EOF {
				r.done = true
				continue
			}
			return Record{}, fmt.Errorf("read offgas input: %w", err)
		}
		if n == 0 {
			return Record{}, io.ErrNoProgress
		}
	}
}

func decodeRecord(line []byte) (Record, error) {
	var record Record
	if err := json.Unmarshal(bytes.TrimSpace(line), &record); err != nil {
		return Record{}, fmt.Errorf("decode offgas record: %w", err)
	}
	if record.BatchID == "" || record.VesselID == "" {
		return Record{}, fmt.Errorf("offgas record needs batch_id and vessel_id")
	}
	return record, nil
}

func Encode(records []Record) []byte {
	var buffer bytes.Buffer
	writer := bufio.NewWriter(&buffer)
	encoder := json.NewEncoder(writer)
	for _, record := range records {
		_ = encoder.Encode(record)
	}
	_ = writer.Flush()
	return buffer.Bytes()
}
