package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/wyw14/cry-149/internal/batch"
	"github.com/wyw14/cry-149/internal/cip"
	"github.com/wyw14/cry-149/internal/feed"
	"github.com/wyw14/cry-149/internal/harvest"
	"github.com/wyw14/cry-149/internal/journal"
	"github.com/wyw14/cry-149/internal/model"
	"github.com/wyw14/cry-149/internal/offgas"
	"github.com/wyw14/cry-149/internal/oxygen"
	"github.com/wyw14/cry-149/internal/probe"
	"github.com/wyw14/cry-149/internal/recipe"
	"github.com/wyw14/cry-149/internal/utility"
	"github.com/wyw14/cry-149/internal/vessel"
)

type Runtime struct {
	mu            sync.RWMutex
	dataDir       string
	startedAt     time.Time
	journal       *journal.Recorder
	snapshots     *journal.SnapshotStore
	batches       *batch.State
	batchRecovery *batch.Recovery
	batchFlow     *batch.Integration
	batchWorkers  *batch.Coordinator
	fleet         *vessel.Fleet
	recipeFlow    *recipe.Coordinator
	feed          *feed.Scheduler
	feedState     *feed.State
	oxygenWindow  *oxygen.Window
	oxygenBank    *oxygen.ControllerBank
	probes        *probe.Catalog
	probeRegistry *probe.Registry
	probeReceiver *probe.Receiver
	offgas        *offgas.Coordinator
	harvest       *harvest.Coordinator
	harvestState  *harvest.State
	utility       *utility.Coordinator
	utilityState  *utility.State
	utilityDriver *utility.MemoryDriver
	leases        *utility.LeaseManager
	cip           *cip.Executor
	cipState      *cip.State
	operations    map[string]model.Operation
	interlocks    map[string]model.Interlock
	incidents     map[string]model.Incident
	closed        bool
}

func New(dataDir string) (*Runtime, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	store, err := journal.Open(filepath.Join(dataDir, "journal"))
	if err != nil {
		return nil, err
	}
	recorder := journal.NewRecorder(store)
	snapshotStore, err := journal.NewSnapshotStore(filepath.Join(dataDir, "snapshots"))
	if err != nil {
		recorder.Close()
		return nil, err
	}
	now := time.Now()
	defaultPlan := recipe.DefaultPlan()
	recipeRecovery := recipe.NewRecovery(map[string]recipe.Plan{defaultPlan.ID: defaultPlan})
	recipeData, err := recipeRecovery.Encode()
	if err != nil {
		recorder.Close()
		return nil, err
	}
	recipeRecovery, err = recipe.DecodeRecovery(recipeData)
	if err != nil {
		recorder.Close()
		return nil, err
	}
	recipeRegistry := recipe.NewRegistry()
	if err := recipeRecovery.Restore(recipeRegistry); err != nil {
		recorder.Close()
		return nil, err
	}
	probeCatalog := probe.NewCatalog()
	probeCatalog.Configure("dissolved-oxygen", 30, true)
	recipeFlow := recipe.NewCoordinator(recipeRegistry, probeCatalog)
	fleet := vessel.NewFleet([]string{"FV-101", "FV-102", "FV-103", "FV-104"}, now)
	batchState := batch.NewState()
	batchRecovery := batch.NewRecovery(batchState)
	feedState := feed.NewState()
	oxygenWindow := oxygen.NewWindow(128)
	oxygenBank := oxygen.NewControllerBank(oxygenWindow)
	batchFlow := batch.NewIntegration(batchState, fleet, recipeFlow, oxygenBank, feedState, recorder)
	feedScheduler := feed.NewScheduler(32, time.Now)
	for _, vesselID := range []string{"FV-101", "FV-102", "FV-103", "FV-104"} {
		if err := feedScheduler.Register(vesselID, feed.NewMemorySink(nil), batchFlow); err != nil {
			recorder.Close()
			return nil, err
		}
	}
	utilityState := utility.NewState()
	dispatcher := utility.NewDispatcher(utilityState)
	driver := utility.NewMemoryDriver()
	if err := dispatcher.Register("hot-water", driver); err != nil {
		recorder.Close()
		return nil, err
	}
	leases := utility.NewLeaseManager(utility.NewSystemClock())
	utilityFlow := utility.NewCoordinator(leases, dispatcher)
	cipState := cip.NewState()
	cipCoordinator := cip.NewCoordinator(cipState, recorder, time.Now)
	cipExecutor := cip.NewExecutor(cipCoordinator, utilityFlow, 2*time.Minute)
	harvestState := harvest.NewState()
	harvestFlow := harvest.NewCoordinator(harvest.NewRouter(), harvestState, timedBackoff{duration: 25 * time.Millisecond}, time.Now)
	runtime := &Runtime{
		dataDir: dataDir, startedAt: now, journal: recorder, snapshots: snapshotStore,
		batches: batchState, batchRecovery: batchRecovery, batchFlow: batchFlow,
		batchWorkers: batch.NewCoordinator(batchState), fleet: fleet, recipeFlow: recipeFlow,
		feed: feedScheduler, feedState: feedState, oxygenWindow: oxygenWindow,
		oxygenBank: oxygenBank, probes: probeCatalog, probeRegistry: probe.NewRegistry(),
		probeReceiver: probe.NewReceiver(probeCatalog, oxygenBank, recorder),
		offgas:        offgas.NewCoordinator(oxygenWindow, recorder), harvest: harvestFlow,
		harvestState: harvestState, utility: utilityFlow, utilityState: utilityState,
		utilityDriver: driver, leases: leases, cip: cipExecutor, cipState: cipState,
		operations: map[string]model.Operation{}, interlocks: map[string]model.Interlock{},
		incidents: map[string]model.Incident{},
	}
	if err := runtime.restore(); err != nil {
		runtime.Close()
		return nil, err
	}
	return runtime, nil
}

type timedBackoff struct {
	duration time.Duration
}

func (b timedBackoff) Wait(ctx context.Context, attempt int) error {
	timer := time.NewTimer(b.duration * time.Duration(attempt+1))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *Runtime) Health() map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return map[string]any{
		"status": "ok", "started_at": r.startedAt,
		"uptime_seconds":        int(time.Since(r.startedAt).Seconds()),
		"closed":                r.closed,
		"calibration_listeners": r.probeRegistry.ListenerCount(),
	}
}

func (r *Runtime) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()
	r.feed.Close()
	for _, current := range r.batches.List() {
		r.batchWorkers.Stop(current.ID)
	}
	if err := r.save(); err != nil {
		_ = r.journal.Close()
		return err
	}
	return r.journal.Close()
}
