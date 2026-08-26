package feed_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/cry-149/internal/feed"
)

type activeLifecycle struct{}

func (activeLifecycle) Active(string) bool { return true }

type gatedSink struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (s *gatedSink) Apply(ctx context.Context, _ feed.Pulse) error {
	s.once.Do(func() { close(s.started) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
		return nil
	}
}

func TestFeedSchedulerBoundsBacklogForSlowVessel(t *testing.T) {
	now := time.Now()
	slow := &gatedSink{started: make(chan struct{}), release: make(chan struct{})}
	scheduler := feed.NewScheduler(1, func() time.Time { return now })
	defer scheduler.Close()
	if err := scheduler.Register("FV-SLOW", slow, activeLifecycle{}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Register("FV-FAST", feed.NewMemorySink(nil), activeLifecycle{}); err != nil {
		t.Fatal(err)
	}
	pulse := func(id, vessel string) feed.Pulse {
		return feed.Pulse{ID: id, BatchID: "B-1", VesselID: vessel, AmountML: 1, CreatedAt: now, ExpiresAt: now.Add(time.Minute)}
	}
	if err := scheduler.Schedule(pulse("P-1", "FV-SLOW")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-slow.started:
	case <-time.After(time.Second):
		t.Fatal("slow vessel did not start consuming")
	}
	if err := scheduler.Schedule(pulse("P-2", "FV-SLOW")); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Schedule(pulse("P-3", "FV-SLOW")); !errors.Is(err, feed.ErrBackpressure) {
		t.Fatalf("expected backpressure, got %v", err)
	}
	if scheduler.Pending("FV-SLOW") > 1 || scheduler.Rejected("FV-SLOW") != 1 {
		t.Fatalf("unexpected slow backlog state pending=%d rejected=%d", scheduler.Pending("FV-SLOW"), scheduler.Rejected("FV-SLOW"))
	}
	if err := scheduler.Schedule(pulse("P-FAST", "FV-FAST")); err != nil {
		t.Fatalf("healthy vessel was affected by slow consumer: %v", err)
	}
	close(slow.release)
}
