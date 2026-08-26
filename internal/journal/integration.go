package journal

import (
	"fmt"
	"time"

	"github.com/wyw14/cry-149/internal/model"
)

type Recorder struct {
	store *Store
}

func NewRecorder(store *Store) *Recorder {
	return &Recorder{store: store}
}

func (r *Recorder) Record(kind, batchID, vesselID string, attributes map[string]any) (model.Event, error) {
	event := model.NewEvent(kind, batchID, vesselID, time.Now(), attributes)
	if err := r.store.Append(event); err != nil {
		return model.Event{}, fmt.Errorf("record %s: %w", kind, err)
	}
	return event, nil
}

func (r *Recorder) History() []model.Event {
	return r.store.Events()
}

func (r *Recorder) Close() error {
	return r.store.Close()
}
