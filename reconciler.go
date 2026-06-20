package beehive

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	defaultMaxRetryInterval  = 30 * time.Second
	defaultBaseRetryInterval = 1 * time.Second
)

// controllerAdapter is the non-generic view of a registered controller. The
// generic Register wraps the user's Controller[Spec, Status] in a concrete
// adapter that closes over Spec/Status, so everything below this line —
// reconciler, work queue, Store — stays free of type parameters and deals in
// raw JSON.
type controllerAdapter interface {
	start() error
	stop(ctx context.Context) error
	reconcile(ctx context.Context, id ObjectID) (Result, error)
}

// typedController adapts a generic Controller[Spec, Status] to the non-generic
// controllerAdapter interface.
type typedController[Spec, Status any] struct {
	gk    GroupKind
	bh    *Beehive
	inner Controller[Spec, Status]
}

func (t *typedController[Spec, Status]) start() error {
	client := &controllerClientImpl[Status]{bh: t.bh, gk: t.gk}
	return t.inner.Start(client)
}

func (t *typedController[Spec, Status]) stop(ctx context.Context) error {
	return t.inner.Stop(ctx)
}

// reconcile runs the controller in a single transaction: load, reconcile, and
// any controller-client writes (UpdateStatus, …) all commit together, or all
// roll back if Reconcile returns an error. The store's Within publishes the
// watch events those writes emit only after the transaction commits, so
// watchers never observe state that was rolled back.
func (t *typedController[Spec, Status]) reconcile(ctx context.Context, id ObjectID) (Result, error) {
	var result Result
	var deleting, missing bool
	// Controller-client calls that free a ref target register it here; we requeue
	// them once the transaction commits (see DeleteDependency).
	wakes := &pendingWakes{}
	ctx = withPendingWakes(ctx, wakes)
	err := t.bh.store.Within(ctx, func(ctx context.Context) error {
		raw, err := t.bh.store.GetObject(ctx, id)
		if errors.Is(err, ErrNotFound) {
			// The queued object is already gone (collected by a prior pass, a
			// cascade, or the backstop between enqueue and now). Nothing to
			// reconcile — a no-op success, not a retryable error. Scoped to this
			// load so a controller's own ErrNotFound (e.g. a dependency on a missing
			// target) still surfaces and retries.
			missing = true
			return nil
		}
		if err != nil {
			return err
		}
		// Read from the already-loaded row as a fast path: it lets a non-finalizing
		// reconcile (the common case) skip collect's separate transaction entirely,
		// while still running GC on the pass where the controller clears its last
		// finalizer.
		deleting = raw.DeletionRequestedAt != nil
		obj, err := rawToTyped[Spec, Status](raw)
		if err != nil {
			return err
		}
		var reconcileErr error
		result, reconcileErr = t.inner.Reconcile(ctx, obj)
		return reconcileErr
	})
	if missing {
		return Result{}, nil
	}
	if err != nil {
		return result, err
	}
	// Committed: requeue any targets the controller freed via DeleteDependency, so
	// a now-unreferenced deletion-pending target is re-examined without waiting on
	// the resync backstop. Post-commit, so a rolled-back DeleteRef wakes nothing.
	for _, tgt := range wakes.targets {
		t.bh.enqueueIfRegistered(GroupKind{Group: tgt.Group, Kind: tgt.Kind}, tgt.ID)
	}
	// GC runs in its own transaction after the controller's writes commit, so a
	// finalizer the controller just cleared is visible and a blocked physical
	// delete doesn't roll back the controller's work.
	if deleting {
		gone, err := t.bh.collect(ctx, id)
		if err != nil {
			return result, err
		}
		// The row is gone: drop any RequeueAfter the controller asked for, or the
		// worker would reschedule a dead id straight into ErrNotFound.
		if gone {
			return Result{}, nil
		}
	}
	return result, nil
}

// reconciler drives the reconcile loop for a single registered controller.
// It owns the work queue, exponential backoff, and periodic resync timer.
type reconciler struct {
	gk                GroupKind
	adapter           controllerAdapter
	store             Store
	work              *workQueue
	resyncInterval    time.Duration
	maxRetryInterval  time.Duration
	baseRetryInterval time.Duration // zero falls back to defaultBaseRetryInterval
	concurrency       int           // number of concurrent worker goroutines; 0/1 = single-threaded
	// startupReconcile selects which objects get an initial reconcile when run starts.
	startupReconcile StartupReconcileStrategy

	backoffMu  sync.Mutex
	backoffFor map[ObjectID]time.Duration
}

// enqueue adds id to the work queue if one is configured.
func (r *reconciler) enqueue(id ObjectID) {
	if r.work != nil {
		r.work.add(id)
	}
}

// enqueueUnsettled asks the store for IDs of objects that haven't converged yet
// and enqueues them. Objects currently being reconciled are skipped to prevent
// duplicate or concurrent reconciles for the same ID.
func (r *reconciler) enqueueUnsettled(ctx context.Context) {
	if r.store == nil {
		return
	}
	r.enqueueFrom(ctx, r.store.ListUnsettledIDs)
}

// enqueueDeletionPending enqueues objects that are finalizing (deletion
// requested, row not yet removed). It is the GC backstop: a delete bumps no
// generation, so the unsettled resync never wakes it, and once an owner is
// RESTRICT-blocked on a child nothing else re-checks it until the child is gone.
func (r *reconciler) enqueueDeletionPending(ctx context.Context) {
	if r.store == nil {
		return
	}
	r.enqueueFrom(ctx, r.store.ListDeletionPendingIDs)
}

// enqueueAll enqueues every object of the kind, including ones whose spec is
// already settled. Used once at startup so controllers can re-confirm
// process-scoped state (e.g. liveness conditions, which a prior process's writes
// leave reading as "verifying") that the unsettled-only resync would never wake.
func (r *reconciler) enqueueAll(ctx context.Context) {
	if r.store == nil {
		return
	}
	r.enqueueFrom(ctx, r.store.ListIDs)
}

// enqueueFrom enqueues the IDs returned by list. The work queue coalesces an ID
// that is already queued and defers one that is mid-reconcile (re-queuing it via
// done), so this never triggers a duplicate or concurrent reconcile.
func (r *reconciler) enqueueFrom(ctx context.Context, list func(context.Context, GroupKind) ([]ObjectID, error)) {
	ids, err := list(ctx, r.gk)
	if err != nil {
		return
	}
	for _, id := range ids {
		r.enqueue(id)
	}
}

// nextBackoff returns the next retry delay for id and doubles it for next time,
// capped at maxRetryInterval.
func (r *reconciler) nextBackoff(id ObjectID) time.Duration {
	r.backoffMu.Lock()
	defer r.backoffMu.Unlock()
	cur := r.backoffFor[id]
	if cur == 0 {
		cur = r.baseRetryInterval
		if cur == 0 {
			cur = defaultBaseRetryInterval
		}
	} else {
		cur *= 2
	}
	if cur > r.maxRetryInterval {
		cur = r.maxRetryInterval
	}
	r.backoffFor[id] = cur
	return cur
}

// clearBackoff resets the retry delay for id after a successful reconcile.
func (r *reconciler) clearBackoff(id ObjectID) {
	r.backoffMu.Lock()
	defer r.backoffMu.Unlock()
	delete(r.backoffFor, id)
}

// run is the per-controller reconcile loop. It exits when ctx is cancelled.
//
// A resyncInterval <= 0 disables the periodic resync entirely: the loop then
// reconciles only in response to events (once the work queue lands), never on a
// timer.
func (r *reconciler) run(ctx context.Context) {
	// Reconcile objects once at startup per the configured strategy. The default
	// (StartupReconcileAll) re-applies persisted specs that a previous run left
	// settled and gives controllers a chance to re-confirm process-scoped state
	// (e.g. liveness conditions, which read as "verifying" until rewritten in this
	// process). Resync ticks stay unsettled-only; staleness is purely a startup
	// concern.
	switch r.startupReconcile {
	case StartupReconcileNone:
		// No startup pass; the periodic resync and live events are the only drivers.
	case StartupReconcileUnsettled:
		r.enqueueUnsettled(ctx)
	default: // StartupReconcileAll
		r.enqueueAll(ctx)
	}
	// Always resume in-progress deletions at startup, independent of the spec
	// strategy above: a process that crashed mid-delete must still drive those
	// rows to removal. Deletion progress is orthogonal to spec convergence; the
	// work queue's set semantics coalesce any overlap with the pass above.
	r.enqueueDeletionPending(ctx)

	n := max(r.concurrency, 1)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			r.runWorker(ctx)
		})
	}
	// Drain the workers, then cancel any retry/RequeueAfter timers they left
	// pending so a torn-down reconciler doesn't leak timers that wake a dead queue.
	defer func() {
		wg.Wait()
		if r.work != nil {
			r.work.stop()
		}
	}()

	// time.NewTicker panics on a non-positive interval, so guard it: a disabled
	// resync means no ticker channel to select on.
	var resync <-chan time.Time
	if r.resyncInterval > 0 {
		ticker := time.NewTicker(r.resyncInterval)
		defer ticker.Stop()
		resync = ticker.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-resync:
			r.enqueueUnsettled(ctx)
			r.enqueueDeletionPending(ctx)
		}
	}
}

// runWorker is the per-goroutine reconcile loop. Multiple instances may run
// concurrently when concurrency > 1. It exits when ctx is cancelled.
func (r *reconciler) runWorker(ctx context.Context) {
	// A nil channel blocks forever in a select, which is the correct no-op
	// when no work queue is configured.
	var workReady <-chan struct{}
	if r.work != nil {
		workReady = r.work.ready
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-workReady:
			if id, ok := r.work.get(); ok {
				result, err := r.adapter.reconcile(ctx, id)
				// done releases the processing hold so a re-add (live event or
				// resync) that arrived mid-reconcile becomes dispatchable. The
				// queue guarantees no second worker had the id in the meantime.
				r.work.done(id)
				if err != nil {
					r.work.addAfter(id, r.nextBackoff(id))
				} else {
					r.clearBackoff(id)
					if result.RequeueAfter > 0 {
						r.work.addAfter(id, result.RequeueAfter)
					}
				}
			}
		}
	}
}
