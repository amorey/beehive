package beehive

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const defaultResyncInterval = 30 * time.Second

type beehiveState uint8

const (
	beehiveNew     beehiveState = iota // registered, not yet started
	beehiveRunning                     // Start succeeded, Stop not yet called
	beehiveStopped                     // Stop was called; instance is permanently unusable
)

// Beehive is the control plane: it owns the durable store and the set of
// registered controllers, and drives their reconcile loops between Start and
// Stop.
type Beehive struct {
	store          Store
	resyncInterval time.Duration
	concurrency    int // default worker count for all controllers; 0/1 = single-threaded
	// startupReconcile is the default startup strategy copied into each reconciler.
	startupReconcile StartupReconcileStrategy

	mu          sync.Mutex
	reconcilers map[GroupKind]*reconciler
	// order preserves registration order so Start brings controllers up — and
	// rolls them back — deterministically, rather than in random map order.
	order  []*reconciler
	state  beehiveState
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Start brings the control plane up: it starts every registered controller and
// launches its reconcile loop. It is an error to start twice or after Stop.
// Beehive is a one-shot object: once stopped, create a new instance.
//
// Start does not take a context: controller startup is assumed to be fast and
// non-blocking. Use Stop to tear the control plane down.
func (bh *Beehive) Start() error {
	bh.mu.Lock()
	defer bh.mu.Unlock()
	switch bh.state {
	case beehiveStopped:
		return fmt.Errorf("beehive: already stopped; create a new Beehive to restart")
	case beehiveRunning:
		return fmt.Errorf("beehive: already started")
	}

	// runCtx lives for the lifetime of the control plane and drives the
	// reconcile loops. It is cancelled by Stop.
	runCtx, cancel := context.WithCancel(context.Background())
	bh.cancel = cancel

	// Start controllers first so they can stand up background workers before
	// any reconcile is dispatched. Iterate in registration order so startup and
	// rollback are deterministic.
	started := make([]*reconciler, 0, len(bh.order))
	for _, r := range bh.order {
		if err := r.adapter.start(); err != nil {
			// Roll back the controllers we already started, then abort.
			// The reconcile loops were never launched, so there's nothing to
			// drain; we just stop each controller. Cleanup runs unbounded.
			for _, s := range started {
				_ = s.adapter.stop(context.Background())
			}
			cancel()
			return fmt.Errorf("beehive: start controller %s/%s: %w", r.gk.Group, r.gk.Kind, err)
		}
		started = append(started, r)
	}

	// A per-kind dependency waker requeues dependents on each change. Driving it
	// off change-events (which the store suppresses for no-ops) rather than every
	// reconcile means a steady state stops waking and cycles settle.
	//
	// Subscribe and start consuming every waker BEFORE launching any reconcile
	// loop: a controller's startup reconcile can modify a target the instant it
	// runs, and that Modified event must not be published before the relevant
	// waker is listening — otherwise dependents go unwoken under configurations
	// that rely on dependency events (e.g. a dependent with StartupReconcileNone
	// and resync disabled). A subscribe failure is non-fatal: that controller
	// still resyncs on its own timer.
	for _, r := range started {
		w, err := bh.store.WatchEvents(runCtx, r.gk)
		if err != nil {
			continue
		}
		bh.wg.Go(func() {
			bh.runDependencyWaker(runCtx, w)
		})
	}

	// Now launch the reconcile loops (ranging `started`, not bh.reconcilers,
	// keeps the "only started controllers run" invariant structural).
	for _, r := range started {
		bh.wg.Go(func() {
			r.run(runCtx)
		})
	}

	bh.state = beehiveRunning
	return nil
}

// runDependencyWaker requeues dependents when a target is modified, until ctx is
// cancelled or the stream ends. The watcher is established by Start (events-only,
// no snapshot: the reconciler's own startup pass already covers existing objects).
// The ctx.Done() arm is needed because a watcher's channel may never close on its
// own.
func (bh *Beehive) runDependencyWaker(ctx context.Context, w Watcher) {
	defer w.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-w.Events():
			if !ok {
				return
			}
			// Only modifications can change a target's observable state. Skipping
			// Added avoids replaying the whole snapshot at startup (where the
			// reconciler's own startup pass already covers every object), and a
			// brand-new object has no dependents yet anyway.
			if ev.Type == WatchEventModified {
				bh.wakeDependents(ctx, ev.Object.ID)
			}
		}
	}
}

// wakeDependents requeues every object that depends_on targetID, each in its own
// kind's reconciler. Over-eager wakes are harmless: unregistered kinds are
// ignored and the work queue coalesces duplicates.
func (bh *Beehive) wakeDependents(ctx context.Context, targetID ObjectID) {
	deps, err := bh.store.ListReferrers(ctx, targetID, RelationDependsOn)
	if err != nil {
		return
	}
	for _, d := range deps {
		bh.enqueueIfRegistered(GroupKind{Group: d.Group, Kind: d.Kind}, d.ID)
	}
}

// Stop tears the control plane down: it cancels the reconcile loops, waits for
// them to drain (bounded by ctx), then stops every controller.
func (bh *Beehive) Stop(ctx context.Context) {
	bh.mu.Lock()
	if bh.state != beehiveRunning {
		bh.mu.Unlock()
		return
	}
	// Transition and cancel under the lock, then release it before waiting on wg.
	// The dependency wakers (counted in wg) acquire bh.mu via enqueueIfRegistered;
	// holding it across wg.Wait would deadlock a waker mid-event against Stop when
	// ctx is unbounded. order is frozen after Start, so it's safe to read unlocked.
	bh.state = beehiveStopped
	bh.cancel()
	bh.mu.Unlock()

	// Wait for reconcile loops to exit, but don't block past ctx.
	done := make(chan struct{})
	go func() {
		bh.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}

	// Watch subscriptions are owned by the store, not the control plane, so Stop
	// does not terminate them: an active watcher ends when its context is
	// cancelled or the store is closed.
	for _, r := range bh.order {
		_ = r.adapter.stop(ctx)
	}
}

// New creates a control plane backed by store s. Register controllers on the
// returned Beehive before calling Start.
func New(s Store, opts ...Option) (*Beehive, error) {
	bh := &Beehive{
		store:          s,
		resyncInterval: defaultResyncInterval,
		reconcilers:    make(map[GroupKind]*reconciler),
	}
	for _, o := range opts {
		if err := o(bh); err != nil {
			return nil, err
		}
	}
	return bh, nil
}

// Register installs controller c for the resource kind gk. It must be called
// before Start, and only once per kind.
func Register[Spec, Status any](bh *Beehive, gk GroupKind, c Controller[Spec, Status], opts ...Option) error {
	bh.mu.Lock()
	defer bh.mu.Unlock()
	if bh.state != beehiveNew {
		return fmt.Errorf("beehive: cannot register %s/%s after Start", gk.Group, gk.Kind)
	}
	if _, exists := bh.reconcilers[gk]; exists {
		return fmt.Errorf("beehive: controller already registered for %s/%s", gk.Group, gk.Kind)
	}

	r := &reconciler{
		gk:               gk,
		store:            bh.store,
		work:             newWorkQueue(),
		resyncInterval:   bh.resyncInterval,
		maxRetryInterval: defaultMaxRetryInterval,
		concurrency:      bh.concurrency,
		startupReconcile: bh.startupReconcile,
		backoffFor:       make(map[ObjectID]time.Duration),
	}
	r.adapter = &typedController[Spec, Status]{gk: gk, bh: bh, inner: c}

	// Per-controller option overrides (e.g. WithResyncInterval, WithMaxRetryInterval).
	for _, o := range opts {
		if err := o(r); err != nil {
			return err
		}
	}

	bh.reconcilers[gk] = r
	bh.order = append(bh.order, r)
	return nil
}

// isRegistered reports whether a controller is registered for gk. The client
// watch surface uses it to reject watches on kinds with no controller, a
// contract the store can't enforce since it doesn't track registrations.
func (bh *Beehive) isRegistered(gk GroupKind) bool {
	bh.mu.Lock()
	defer bh.mu.Unlock()
	_, ok := bh.reconcilers[gk]
	return ok
}

// enqueueIfRegistered wakes the reconciler for (gk, id) if one exists.
// It is a no-op when gk has no registered controller (e.g. a client-only kind).
func (bh *Beehive) enqueueIfRegistered(gk GroupKind, id ObjectID) {
	bh.mu.Lock()
	r, ok := bh.reconcilers[gk]
	bh.mu.Unlock()
	if ok {
		r.enqueue(id)
	}
}
