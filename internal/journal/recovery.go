package journal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/wyw14/cry-149/internal/model"
)

type SnapshotStore struct {
	path string
}

func NewSnapshotStore(directory string) (*SnapshotStore, error) {
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("create snapshot directory: %w", err)
	}
	return &SnapshotStore{path: filepath.Join(directory, "runtime-snapshot.json")}, nil
}

func (s *SnapshotStore) Save(snapshot model.Snapshot) error {
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}
	temporary := s.path + ".tmp"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err := os.Rename(temporary, s.path); err != nil {
		return fmt.Errorf("replace snapshot: %w", err)
	}
	return nil
}

func (s *SnapshotStore) Load() (model.Snapshot, error) {
	data, err := os.ReadFile(s.path)
	if errorsIsNotExist(err) {
		return model.Snapshot{}, os.ErrNotExist
	}
	if err != nil {
		return model.Snapshot{}, fmt.Errorf("read snapshot: %w", err)
	}
	var snapshot model.Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return model.Snapshot{}, fmt.Errorf("decode snapshot: %w", err)
	}
	return snapshot, nil
}

func errorsIsNotExist(err error) bool {
	return err != nil && os.IsNotExist(err)
}
