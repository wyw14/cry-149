package harvest

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wyw14/cry-149/internal/vessel"
)

// channelBackoff blocks Wait until either ctx is cancelled or release is
// closed, letting a test deterministically park a transfer in backoff.
type channelBackoff struct {
	release chan struct{}
}

func (b channelBackoff) Wait(ctx context.Context, attempt int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.release:
		return nil
	}
}

func makeRoute(id, from, to string) vessel.Route {
	r, err := vessel.NewRoute(id, id, []vessel.Segment{
		{ID: id + "-seg", From: from, To: to},
	})
	if err != nil {
		panic(err)
	}
	return r
}

// TestTransferDoesNotHoldRouteMutexDuringBackoff reproduces the bug where a
// transfer that backed off while holding the shared route mutex blocked an
// unrelated transfer on a non-overlapping pipeline. Vessel one's transfer
// enters backoff (downstream unavailable); vessel two's transfer on disjoint
// segments must proceed immediately rather than queue on the shared mutex.
func TestTransferDoesNotHoldRouteMutexDuringBackoff(t *testing.T) {
	router := NewRouter()
	state := NewState()
	release := make(chan struct{})
	coord := NewCoordinator(router, state, channelBackoff{release: release}, time.Now)

	routeOne := makeRoute("R1", "v1", "d1")
	routeTwo := makeRoute("R2", "v2", "d2")

	// Vessel one: downstream never available, so it parks in backoff.
	neverAvailable := func(attempt int) bool { return false }

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := coord.Transfer(context.Background(), "B1", "V1", routeOne, neverAvailable); err == nil {
			t.Error("vessel one transfer unexpectedly succeeded")
		}
	}()

	// Give vessel one time to fail its availability check and enter backoff.
	time.Sleep(20 * time.Millisecond)

	// Vessel two uses disjoint segments and is immediately available. Under the
	// old implementation it would block on router.mu until vessel one's
	// backoff completes; it must now succeed promptly.
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := coord.Transfer(context.Background(), "B2", "V2", routeTwo, func(int) bool { return true })
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("vessel two transfer failed: %v", err)
		}
		if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
			t.Fatalf("vessel two waited %v for an unrelated transfer's backoff", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("vessel two transfer was blocked by vessel one backoff")
	}

	close(release)
	wg.Wait()
}

// TestTransferDoesNotDeadlockOnBackoffCancellation ensures a transfer that is
// cancelled while waiting in backoff does not self-deadlock by re-locking the
// route mutex it still held under the old defer-unlock implementation, and that
// it returns promptly.
func TestTransferDoesNotDeadlockOnBackoffCancellation(t *testing.T) {
	router := NewRouter()
	state := NewState()
	release := make(chan struct{})
	coord := NewCoordinator(router, state, channelBackoff{release: release}, time.Now)

	routeOne := makeRoute("R1", "v1", "d1")
	neverAvailable := func(attempt int) bool { return false }

	var (
		started int32
		wg      sync.WaitGroup
	)
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		atomic.StoreInt32(&started, 1)
		_, _ = coord.Transfer(ctx, "B1", "V1", routeOne, neverAvailable)
	}()

	for atomic.LoadInt32(&started) == 0 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond) // let it enter backoff

	// Cancelling while parked in backoff must return, not deadlock.
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled transfer did not return (deadlock)")
	}

	// The router must remain usable: an overlapping transfer by a new owner
	// must succeed, proving no claim or lock was leaked.
	routeSame := makeRoute("R1", "v1", "d1")
	if _, err := coord.Transfer(context.Background(), "B2", "V2", routeSame, func(int) bool { return true }); err != nil {
		t.Fatalf("router left unusable after cancellation: %v", err)
	}
}
