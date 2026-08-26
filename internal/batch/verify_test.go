package batch_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wyw14/cry-149/internal/batch"
)

func TestBatchStopWaitsForDynamicallyStartedProbeWorkers(t *testing.T) {
	group := batch.NewWorkerGroup(context.Background())
	admission := make(chan struct{})
	var ran atomic.Bool
	if err := group.Start(func(context.Context) { ran.Store(true) }, admission); err != nil {
		t.Fatal(err)
	}
	stopped := make(chan struct{})
	go func() {
		group.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop did not settle admitted worker")
	}
	close(admission)
	time.Sleep(25 * time.Millisecond)
	if ran.Load() {
		t.Fatal("worker started after stop had completed")
	}
	if err := group.Start(func(context.Context) {}, nil); err != batch.ErrStopping {
		t.Fatalf("new worker accepted after stop: %v", err)
	}
}
