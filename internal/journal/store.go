package journal

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/wyw14/cry-149/internal/model"
)

var ErrClosed = errors.New("journal is closed")

type Store struct {
	mu     sync.RWMutex
	events []model.Event
	file   *os.File
	closed bool
}

func Open(directory string) (*Store, error) {
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("create journal directory: %w", err)
	}
	path := filepath.Join(directory, "events.jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}
	store := &Store{file: file, events: []model.Event{}}
	if err := store.load(); err != nil {
		file.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) load() error {
	if _, err := s.file.Seek(0, 0); err != nil {
		return fmt.Errorf("seek journal: %w", err)
	}
	scanner := bufio.NewScanner(s.file)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		var event model.Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return fmt.Errorf("decode journal event: %w", err)
		}
		s.events = append(s.events, event)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read journal: %w", err)
	}
	_, err := s.file.Seek(0, 2)
	return err
}

func (s *Store) Append(event model.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	if _, err := s.file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("append event: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("sync event: %w", err)
	}
	s.events = append(s.events, event)
	return nil
}

func (s *Store) Events() []model.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]model.Event(nil), s.events...)
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.file.Close()
}
