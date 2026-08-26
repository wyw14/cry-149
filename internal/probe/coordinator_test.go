package probe

import (
	"testing"
	"time"
)

// calibrationFor is a small helper to build a Calibration with a stable probe id.
func calibrationFor(probeID string) Calibration {
	return Calibration{ProbeID: probeID, Offset: 0.1, Slope: 1, At: time.Time{}}
}

// TestBroadcastDoesNotBlockOnFullStaleListener reproduces the production
// head-of-line stall: a batch calibration listener whose consumer has exited
// (or stalled) leaves its buffered stream full. The original Broadcast sent on
// the stream while holding the registry write lock, so that one full, abandoned
// listener wedged the entire registry — hanging Get (every vessel status
// endpoint), ListenerCount (healthz), and even the listener's own cancel.
//
// After the fix, Broadcast must (a) never block on a full listener and (b)
// evict the stale listener so subsequent broadcasts/reads stay responsive.
func TestBroadcastDoesNotBlockOnFullStaleListener(t *testing.T) {
	registry := NewRegistry()
	stream, cancel, err := registry.Subscribe("batch:stale", 2)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// Simulate a listener that is still "registered" (not cancelled) but whose
	// consumer is gone. Fill its buffer so the next send would have to block.
	calibration := calibrationFor("DO-101")
	for i := 0; i < 2; i++ {
		registry.Broadcast(calibration)
	}

	// Now the stream is full and nobody is draining it. A blocking Broadcast
	// would hang here forever while holding the lock. Use a timeout guard so a
	// regression fails the test loudly instead of stalling the runner.
	done := make(chan int, 1)
	go func() {
		done <- registry.Broadcast(calibrationFor("DO-102"))
	}()
	select {
	case delivered := <-done:
		// The full, undrained listener cannot receive, so the broadcast must
		// drop it and evict it rather than block. Delivered count is 0 since
		// the only listener was full.
		if delivered != 0 {
			t.Fatalf("expected 0 deliveries to a full stale listener, got %d", delivered)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Broadcast blocked on a full stale listener — registry lock was held during a blocking send")
	}

	// The stale listener must have been evicted so it stops shadowing future
	// broadcasts and clogging Get/ListenerCount.
	if count := registry.ListenerCount(); count != 0 {
		t.Fatalf("expected stale listener to be evicted, ListenerCount=%d", count)
	}
	// The evicted stream must still be drainable by its owner without panic.
	select {
	case <-stream:
	default:
	}
	// cancel must remain safe to call after eviction (idempotent teardown).
	cancel()
}

// TestBroadcastSkipsAlreadyCancelledListener ensures a listener that called
// cancel() (done closed) before the broadcast is pruned rather than delivered
// to, and that Broadcast stays non-blocking.
func TestBroadcastSkipsAlreadyCancelledListener(t *testing.T) {
	registry := NewRegistry()
	if _, cancel, err := registry.Subscribe("batch:done", 1); err != nil {
		t.Fatalf("subscribe: %v", err)
	} else {
		cancel()
	}
	// One live listener that drains immediately.
	liveStream, liveCancel, err := registry.Subscribe("batch:live", 1)
	if err != nil {
		t.Fatalf("subscribe live: %v", err)
	}
	defer liveCancel()

	done := make(chan int, 1)
	go func() { done <- registry.Broadcast(calibrationFor("DO-200")) }()
	select {
	case delivered := <-done:
		if delivered != 1 {
			t.Fatalf("expected 1 delivery to the live listener, got %d", delivered)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Broadcast blocked despite cancelled listener being present")
	}
	select {
	case <-liveStream:
	default:
		t.Fatal("live listener did not receive the calibration")
	}
	if count := registry.ListenerCount(); count != 1 {
		t.Fatalf("expected only the live listener to remain, count=%d", count)
	}
}

// TestBroadcastConcurrentReadsStayResponsive asserts the core invariant the bug
// violated: while a broadcast is in flight, readers (Get/ListenerCount) must not
// be blocked by a slow or full listener.
func TestBroadcastConcurrentReadsStayResponsive(t *testing.T) {
	registry := NewRegistry()
	// A full, undrained listener — the exact condition that used to hang the
	// registry. Leave it uncancelled to mimic an abandoned batch listener.
	if _, _, err := registry.Subscribe("batch:abandoned", 1); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	registry.Broadcast(calibrationFor("DO-301")) // fills the abandoned stream.

	reads := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			registry.Get("DO-301")
			registry.ListenerCount()
		}
		close(reads)
	}()
	select {
	case <-reads:
	case <-time.After(2 * time.Second):
		t.Fatal("Get/ListenerCount blocked behind a broadcast stuck on a full listener")
	}
}
