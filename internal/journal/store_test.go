package journal

import (
	"testing"
	"time"

	"github.com/wyw14/cry-149/internal/model"
)

func TestStorePersistsEventsAcrossOpen(t *testing.T) {
	directory := t.TempDir()
	store, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	event := model.NewEvent("batch.started", "B-1", "FV-101", time.Now(), map[string]any{"recipe": "r1"})
	if err := store.Append(event); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	events := reopened.Events()
	if len(events) != 1 || events[0].ID != event.ID {
		t.Fatalf("unexpected restored events: %#v", events)
	}
}
