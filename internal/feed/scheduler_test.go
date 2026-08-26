package feed

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// staticClock holds a mutable now value so tests can advance time and observe
// expiry without racing against the real clock.
type staticClock struct{ now time.Time }

func (c *staticClock) Now() time.Time { return c.now }

type alwaysActive struct{}

func (alwaysActive) Active(string) bool { return true }

// blockingSink blocks every Apply until the test releases it, simulating a
// fermentation vessel whose link has gone slow / stalled. Release is idempotent
// so it is safe to call from both the test body and a deferred cleanup.
type blockingSink struct {
	release chan struct{}
	once    sync.Once
	applied atomic.Int32
}

func newBlockingSink() (*blockingSink, func()) {
	sink := &blockingSink{release: make(chan struct{})}
	return sink, sink.Release
}

func (s *blockingSink) Release() {
	s.once.Do(func() { close(s.release) })
}

func (s *blockingSink) Apply(ctx context.Context, pulse Pulse) error {
	s.applied.Add(1)
	<-s.release
	return nil
}

// waitFor returns true once counter reaches want, polling up to timeout. The
// counter is passed by pointer because atomic.Int32 must not be copied.
func waitFor(counter *atomic.Int32, want int32, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if counter.Load() == want {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return counter.Load() == want
}

// waitForRejected polls the scheduler's rejected counter for vesselID until it
// reaches want or the timeout elapses, smoothing out the worker's drain timing.
func waitForRejected(s *Scheduler, vesselID string, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.Rejected(vesselID) == want {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return s.Rejected(vesselID) == want
}

// TestSchedulerRejectsWhenQueueFullAndCountsRejected verifies that a full
// per-vessel queue surfaces backpressure instead of spawning a blocked
// goroutine per pulse — the cause of unbounded memory growth on a slow vessel.
func TestSchedulerRejectsWhenQueueFullAndCountsRejected(t *testing.T) {
	clock := &staticClock{now: time.Unix(1_000, 0)}
	scheduler := NewScheduler(1, clock.Now) // queue capacity of one
	sink, release := newBlockingSink()
	if err := scheduler.Register("FV-101", sink, alwaysActive{}); err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	defer release()

	// Fill the single queue slot and let the worker dequeue + block on Apply.
	first := Pulse{ID: "p1", BatchID: "B-1", VesselID: "FV-101", ExpiresAt: clock.Now().Add(time.Hour)}
	if err := scheduler.Schedule(first); err != nil {
		t.Fatalf("first pulse should be queued: %v", err)
	}
	if !waitFor(&sink.applied, 1, time.Second) {
		t.Fatal("worker did not dequeue the first pulse")
	}
	// Now the queue is empty but the worker is stuck inside Apply. Refill the
	// slot, so the *next* Schedule finds a full queue and must reject.
	second := Pulse{ID: "p2", BatchID: "B-1", VesselID: "FV-101", ExpiresAt: clock.Now().Add(time.Hour)}
	if err := scheduler.Schedule(second); err != nil {
		t.Fatalf("second pulse should refill the queue: %v", err)
	}
	// The third pulse arrives at a full queue (slot held by p2, worker blocked).
	third := Pulse{ID: "p3", BatchID: "B-1", VesselID: "FV-101", ExpiresAt: clock.Now().Add(time.Hour)}
	err := scheduler.Schedule(third)
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("pulse over a full queue must return ErrBackpressure, got %v", err)
	}
	if got := scheduler.Rejected("FV-101"); got != 1 {
		t.Fatalf("rejected counter = %d, want 1", got)
	}
}

// TestSchedulerDropsExpiredPulseAtDelivery verifies that a pulse which ages out
// while queued behind a slow vessel is dropped at delivery time rather than
// flushed onto a recovered link as a stale feed.
func TestSchedulerDropsExpiredPulseAtDelivery(t *testing.T) {
	clock := &staticClock{now: time.Unix(2_000, 0)}
	scheduler := NewScheduler(8, clock.Now)
	sink, release := newBlockingSink()
	if err := scheduler.Register("FV-102", sink, alwaysActive{}); err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	defer release()

	// p1 is valid now; the worker dequeues it and blocks inside Apply.
	p1 := Pulse{
		ID: "p1", BatchID: "B-2", VesselID: "FV-102",
		ExpiresAt: clock.Now().Add(2 * time.Second),
	}
	if err := scheduler.Schedule(p1); err != nil {
		t.Fatalf("p1 should be queued: %v", err)
	}
	if !waitFor(&sink.applied, 1, time.Second) {
		t.Fatal("worker did not dequeue p1")
	}

	// While the worker is stalled, enqueue p2 with a short validity window.
	// It sits in the queue because the worker cannot drain past the blocked p1.
	p2 := Pulse{
		ID: "p2", BatchID: "B-2", VesselID: "FV-102",
		ExpiresAt: clock.Now().Add(2 * time.Second),
	}
	if err := scheduler.Schedule(p2); err != nil {
		t.Fatalf("p2 should be queued: %v", err)
	}
	if got := scheduler.Pending("FV-102"); got != 1 {
		t.Fatalf("p2 should be waiting in the queue, pending = %d", got)
	}

	// Advance the clock so p2 has expired *while queued*. Then release p1 so
	// the worker reaches p2 — the delivery-time check must drop it.
	clock.now = clock.now.Add(time.Minute)
	release()

	// p1's Apply already counted; p2 must be dropped before Apply, so the
	// counter stays at 1.
	if got := sink.applied.Load(); got != 1 {
		t.Fatalf("apply count = %d, want 1 (stale p2 must not be applied)", got)
	}
	if !waitForRejected(scheduler, "FV-102", 1, time.Second) {
		t.Fatalf("rejected counter = %d, want 1 (dropped p2 should be counted)", scheduler.Rejected("FV-102"))
	}
}

// TestSchedulerDoesNotLeakGoroutinesOnBackpressure guards against the old
// behaviour of spawning a blocked goroutine per rejected pulse. Under backpressure
// the goroutine count must stay flat no matter how many pulses are rejected.
func TestSchedulerDoesNotLeakGoroutinesOnBackpressure(t *testing.T) {
	clock := &staticClock{now: time.Unix(3_000, 0)}
	scheduler := NewScheduler(1, clock.Now)
	sink, release := newBlockingSink()
	if err := scheduler.Register("FV-103", sink, alwaysActive{}); err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	defer release()

	blocker := Pulse{ID: "blocker", BatchID: "B-3", VesselID: "FV-103", ExpiresAt: clock.Now().Add(time.Hour)}
	if err := scheduler.Schedule(blocker); err != nil {
		t.Fatal(err)
	}
	if !waitFor(&sink.applied, 1, time.Second) {
		t.Fatal("worker did not dequeue the blocker")
	}

	// Refill the single slot so every subsequent pulse hits a full queue.
	hold := Pulse{ID: "hold", BatchID: "B-3", VesselID: "FV-103", ExpiresAt: clock.Now().Add(time.Hour)}
	if err := scheduler.Schedule(hold); err != nil {
		t.Fatalf("hold pulse should refill the queue: %v", err)
	}

	// Let any transient goroutines settle, then snapshot before the flood.
	time.Sleep(20 * time.Millisecond)
	before := runtime.NumGoroutine()

	const flood = 64
	for i := 0; i < flood; i++ {
		_ = scheduler.Schedule(Pulse{ID: "p", BatchID: "B-3", VesselID: "FV-103", ExpiresAt: clock.Now().Add(time.Hour)})
	}

	// Give any (erroneously) spawned goroutines a chance to appear.
	time.Sleep(20 * time.Millisecond)
	after := runtime.NumGoroutine()
	// The old code would spawn one blocked goroutine per rejected pulse (~flood
	// new goroutines). With the fix the count stays flat; allow a small delta
	// for runtime background goroutine noise.
	if delta := after - before; delta > 8 || delta < -8 {
		t.Fatalf("goroutine count moved from %d to %d (delta %d) — backpressure leaked goroutines", before, after, delta)
	}
	// With capacity 1 already filled by `hold`, all `flood` pulses are rejected.
	if got := scheduler.Rejected("FV-103"); got != flood {
		t.Fatalf("rejected counter = %d, want %d", got, flood)
	}
}
