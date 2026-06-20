package beehive

import (
	"context"
	"fmt"
	"log/slog"
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
	// logger and logLevel are the user-supplied logging config (nil logger =
	// disabled). They stay raw until Start resolves them via resolveLogger; each
	// reconciler inherits them as its own default (see Register).
	logger   *slog.Logger
	logLevel slog.Leveler

	mu          sync.Mutex
	reconcilers map[GroupKind]*reconciler
	// order preserves registration order so Start brings controllers up — and
	// rolls them back — deterministically, rather than in random map order.
	order  []*reconciler
	state  beehiveState
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// log returns a non-nil logger. Start resolves bh.logger, but Stop (and tests
// that drive state directly) may run before that, so guard against nil.
func (bh *Beehive) log() *slog.Logger {
	if bh.logger == nil {
		return discardLogger
	}
	return bh.logger
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

	// Resolve the control plane's own logger once: nil becomes the discard logger
	// so the goroutines below (GC sweeper, dependency wakers) log unconditionally.
	bh.logger = resolveLogger(bh.logger, bh.logLevel)

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
			bh.logger.Error("controller start failed; rolled back",
				"group", r.gk.Group, "kind", r.gk.Kind, "err", err)
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
			bh.logger.Warn("dependency waker subscription failed; relying on resync",
				"group", r.gk.Group, "kind", r.gk.Kind, "err", err)
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

	// The global GC sweeper collects deletion-pending objects of client-only
	// kinds, which no per-controller backstop reaches. Counted in wg so Stop
	// drains it.
	bh.wg.Go(func() {
		bh.runGCSweeper(runCtx)
	})

	bh.state = beehiveRunning
	bh.logger.Info("control plane started", "controllers", len(started))
	return nil
}

// runGCSweeper is the global garbage-collection backstop. The per-controller
// reconcile loop runs collect for its own kind; this sweeps every kind, so a
// deletion-pending object of a client-only kind (no registered controller) is
// still collected — otherwise it would strand and RESTRICT-block its owner's
// delete forever. It sweeps once at startup and then on the resync cadence; a
// disabled resync leaves only the startup pass, matching the per-controller
// backstop.
func (bh *Beehive) runGCSweeper(ctx context.Context) {
	bh.sweepDeletionPending(ctx)
	if bh.resyncInterval <= 0 {
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(bh.resyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bh.sweepDeletionPending(ctx)
		}
	}
}

// sweepDeletionPending collects every deletion-pending object. collect is a
// no-op while an object still holds finalizers or live referrers, and idempotent
// if another path already collected it, so re-sweeping the registered kinds that
// their own controllers handle is harmless.
func (bh *Beehive) sweepDeletionPending(ctx context.Context) {
	ids, err := bh.store.ListAllDeletionPendingIDs(ctx)
	if err != nil {
		bh.logger.Warn("gc sweep: listing deletion-pending objects failed; retry next sweep", "err", err)
		return
	}
	for _, id := range ids {
		// Best-effort: a benign ErrNotFound (already collected) or a transient
		// error is retried on the next sweep.
		_, _ = bh.collect(ctx, id)
	}
}

// runDependencyWaker requeues dependents when a target changes, until ctx is
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
			// Wake on any present-state change. We must handle Added, not just
			// Modified: the conflating store hub coalesces a create-then-modify into
			// a single Added, so skipping it would drop the modify's wake. A
			// brand-new object usually has no dependents, so the extra lookup is a
			// cheap no-op — the over-wake is harmless. Deleted carries nothing to
			// requeue (a gone object has no dependents).
			if ev.Type == WatchEventAdded || ev.Type == WatchEventModified {
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
	bh.log().Info("control plane stopping")
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
	bh.log().Info("control plane stopped")
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
		// Inherit the control plane's logging config as the default; the options
		// below may override it for this controller.
		logger:   bh.logger,
		logLevel: bh.logLevel,
	}
	adapter := &typedController[Spec, Status]{gk: gk, bh: bh, inner: c}
	r.adapter = adapter

	// Per-controller option overrides (e.g. WithResyncInterval, WithMaxRetryInterval).
	for _, o := range opts {
		if err := o(r); err != nil {
			return err
		}
	}

	// Resolve once now that overrides are applied, and tag every record with the
	// kind so per-object logs need only add the id. The adapter shares the same
	// resolved logger for its reconcile-scoped messages.
	r.logger = resolveLogger(r.logger, r.logLevel).With("group", gk.Group, "kind", gk.Kind)
	adapter.logger = r.logger

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
