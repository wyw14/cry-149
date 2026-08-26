package harvest_test

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-149/internal/harvest"
	"github.com/wyw14/cry-149/internal/vessel"
)

type observedBackoff struct {
	entered chan struct{}
	release chan struct{}
}

func (b *observedBackoff) Wait(ctx context.Context, _ int) error {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.release:
		return nil
	}
}

func TestHarvestRetryDoesNotHoldRouteLockDuringBackoff(t *testing.T) {
	backoff := &observedBackoff{entered: make(chan struct{}, 3), release: make(chan struct{})}
	coordinator := harvest.NewCoordinator(harvest.NewRouter(), harvest.NewState(), backoff, time.Now)
	routeA, err := vessel.NewRoute("R-A", "B-A", []vessel.Segment{{ID: "A-1", From: "FV-101", To: "TK-1"}})
	if err != nil {
		t.Fatal(err)
	}
	routeB, err := vessel.NewRoute("R-B", "B-B", []vessel.Segment{{ID: "B-1", From: "FV-102", To: "TK-2"}})
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() {
		_, runErr := coordinator.Transfer(context.Background(), "B-A", "FV-101", routeA, func(int) bool { return false })
		firstDone <- runErr
	}()
	select {
	case <-backoff.entered:
	case <-time.After(time.Second):
		t.Fatal("first transfer did not enter backoff")
	}
	secondDone := make(chan error, 1)
	go func() {
		_, runErr := coordinator.Transfer(context.Background(), "B-B", "FV-102", routeB, func(int) bool { return true })
		secondDone <- runErr
	}()
	select {
	case runErr := <-secondDone:
		if runErr != nil {
			t.Fatalf("independent route failed: %v", runErr)
		}
	case <-time.After(150 * time.Millisecond):
		close(backoff.release)
		<-firstDone
		t.Fatal("retry backoff held the shared route lock")
	}
	close(backoff.release)
	if runErr := <-firstDone; runErr == nil {
		t.Fatal("unavailable transfer unexpectedly succeeded")
	}
}
