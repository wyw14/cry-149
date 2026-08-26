package batch

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-149/internal/model"
)

type Recovery struct {
	state *State
}

func NewRecovery(state *State) *Recovery {
	return &Recovery{state: state}
}

func (r *Recovery) Snapshot(now time.Time) model.Snapshot {
	snapshot := model.EmptySnapshot(now)
	snapshot.Revision = uuid.NewString()
	for _, value := range r.state.List() {
		snapshot.Batches[value.ID] = value.Clone()
	}
	return snapshot
}

func (r *Recovery) Restore(snapshot model.Snapshot) error {
	if snapshot.Revision == "" {
		return fmt.Errorf("snapshot revision is required")
	}
	return r.state.Restore(snapshot.Batches)
}
