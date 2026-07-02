// Copyright 2026 Andres Morey
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package beehive

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/amorey/beehive/internal/conflate"
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
	reconcile(ctx context.Context, id ObjectID) (Result, error)
}

// typedController adapts a generic Controller[Spec, Status] to the non-generic
// controllerAdapter interface.
type typedController[Spec, Status any] struct {
	gk     GroupKind
	bh     *Beehive
	inner  Controller[Spec, Status]
	client ControllerClient[Status] // built once at Register, passed into each Reconcile
	logger *slog.Logger             // kind-tagged; set by Register (never nil after that)
}

// log returns a non-nil logger, guarding the rare path where a typedController
// is built outside Register (e.g. in tests) and logger was never assigned.
func (t *typedController[Spec, Status]) log() *slog.Logger {
	if t.logger == nil {
		return discardLogger
	}
	return t.logger
}

// reconcile loads the object and runs the controller. There is no enclosing
// transaction: each ControllerClient write commits on its own (autocommit), so a
// write that lands before Reconcile returns an error stays committed and the level
// loop re-derives from it on retry. A controller that needs several writes to be
// atomic wraps them in ControllerClient.Within. GC runs in its own transaction
// afterward (see collect).
func (t *typedController[Spec, Status]) reconcile(ctx context.Context, id ObjectID) (Result, error) {
	log := t.log().With("id", id)
	// Controller-client calls that free a ref target register it here; we requeue
	// them after Reconcile returns (see DeleteDependency).
	wakes := &pendingWakes{}
	ctx = withPendingWakes(ctx, wakes)

	raw, err := t.bh.store.GetObject(ctx, id)
	if errors.Is(err, ErrNotFound) {
		// The queued object is already gone (collected by a prior pass, a cascade,
		// or the backstop between enqueue and now). Nothing to reconcile — a no-op
		// success, not a retryable error.
		log.DebugContext(ctx, "object gone before reconcile; skipping")
		return Result{}, nil
	}
	if err != nil {
		return Result{}, err
	}
	// The already-loaded row's deletion flag is a fast path: it lets a
	// non-finalizing reconcile (the common case) skip collect's separate
	// transaction entirely, while still running GC on the pass where the controller
	// clears its last finalizer.
	deleting := raw.DeletionRequestedAt != nil
	obj, err := rawToTyped[Spec, Status](raw, t.bh.migratorFor(t.gk))
	if err != nil {
		return Result{}, err
	}

	log.DebugContext(ctx, "reconciling", "generation", obj.Generation, "deleting", deleting)
	result, reconcileErr := t.inner.Reconcile(ctx, t.client, obj)
	if reconcileErr != nil {
		// Warn, not Error: a failed reconcile is expected churn the retry loop
		// absorbs. We don't return yet — the controller's committed writes still need
		// their GC follow-up below (see func doc), or a freed object could strand.
		log.WarnContext(ctx, "reconcile failed; will retry", "err", reconcileErr)
	}
	// Advance any targets the controller freed via DeleteDependency, so a
	// now-unreferenced deletion-pending target is re-examined without waiting on the
	// resync backstop. advanceGC (not enqueueIfRegistered) routes the wake by kind: a
	// registered kind enqueues, while a client-only kind with resync disabled — whose
	// freed target would otherwise strand, RESTRICT-blocking nothing else re-checks
	// it — collects synchronously.
	for _, tgt := range wakes.targets {
		t.bh.advanceGC(ctx, GroupKind{Group: tgt.Group, Kind: tgt.Kind}, tgt.ID)
	}
	// GC runs in its own transaction over the controller's committed writes, so a
	// finalizer the controller just cleared is visible.
	if deleting {
		gone, gcErr := t.bh.collect(ctx, id)
		if gcErr != nil {
			log.ErrorContext(ctx, "garbage collection failed; will retry", "err", gcErr)
			// Either error makes the worker retry; prefer the reconcile error.
			return result, cmp.Or(reconcileErr, gcErr)
		}
		// The row is gone: like the ErrNotFound skip above, there's nothing left to
		// reconcile, so drop any RequeueAfter and the reconcile error rather than
		// rescheduling a dead id straight into ErrNotFound.
		if gone {
			log.DebugContext(ctx, "object collected")
			return Result{}, nil
		}
	}
	return result, reconcileErr
}

// reconciler drives the reconcile loop for a single registered controller.
// It owns the work queue, exponential backoff, and periodic resync timer.
type reconciler struct {
	gk                GroupKind
	adapter           controllerAdapter
	store             Store
	work              *workQueue
	// scheduleHub fans each object's next-requeue changes out to WatchSchedule
	// subscribers, keyed by ObjectID with latest-value-per-id coalescing. The work
	// queue feeds it through onSchedule; Close (on teardown) ends live streams.
	scheduleHub *conflate.Hub[ObjectID, Schedule]
	resyncInterval    time.Duration
	maxRetryInterval  time.Duration
	baseRetryInterval time.Duration // zero falls back to defaultBaseRetryInterval
	concurrency       int           // number of concurrent worker goroutines; 0/1 = single-threaded
	// startupReconcile selects which objects get an initial reconcile when run starts.
	startupReconcile StartupReconcileStrategy
	// migrator is the per-kind schema-version converter set by WithMigrator at
	// Register; Register copies it into bh.migrators so the client path shares it.
	// nil when the kind opted out.
	migrator Migrator
	// logger is kind-tagged and resolved (never nil) once Register runs; logLevel
	// is the raw per-controller override consumed during that resolution.
	logger   *slog.Logger
	logLevel slog.Leveler

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

// requeue makes id immediately dispatchable, optionally resetting its retry
// backoff ladder first. It is the engine behind Client.Requeue — a latency hint,
// not a synchronous run, so a worker picks the id up on its own schedule. The
// resetBackoff intent lives here, in the layer every client surface shares, so
// the "WithResetBackoff clears the ladder before dispatch" invariant is enforced once.
// Backoff is otherwise cleared only by a successful reconcile, never by a plain
// requeue.
func (r *reconciler) requeue(id ObjectID, resetBackoff bool) {
	if resetBackoff {
		r.clearBackoff(id)
	}
	r.requeueNow(id)
}

// requeueNow makes id immediately dispatchable, cancelling any pending delayed
// requeue. It is the reconciler-layer counterpart of workQueue.requeueNow and the
// pure immediate-dispatch step: it deliberately does not touch the backoff ladder
// (see requeue, which layers the optional reset on top).
func (r *reconciler) requeueNow(id ObjectID) {
	if r.work != nil {
		// Drop any stale backoff timer and make the id dispatchable now, atomically.
		r.work.requeueNow(id)
	}
}

// nextRequeueAt reports when the loop has scheduled id to be requeued (a pending
// backoff/RequeueAfter delay, or now if already queued). ok is false when no
// requeue is scheduled; it reports only per-id timers, so it excludes the periodic
// resync and any event-driven wake — the actual next reconcile may be sooner.
func (r *reconciler) nextRequeueAt(id ObjectID) (time.Time, bool) {
	if r.work == nil {
		return time.Time{}, false
	}
	return r.work.nextRequeueAt(id)
}

// mergeSchedule is the schedule hub's coalescing policy: latest value wins and the
// slot is never annihilated. Unlike the object watch, "unscheduled" (the zero
// Schedule) is a real gauge value a subscriber must observe, so it is kept, not
// dropped — a slow reader converges to the id's current schedule.
func mergeSchedule(_, next Schedule) (Schedule, bool) { return next, true }

// publishSchedule feeds one work-queue schedule change into the hub. It is the
// onSchedule callback, so it runs under the queue lock: it maps the queue's native
// (time, scheduled) to the public Schedule (unscheduled folds to the zero time),
// then Sends — which never blocks, and a closed hub drops it. The scheduled bool is
// redundant with a zero time here, so it is ignored.
func (r *reconciler) publishSchedule(id ObjectID, at time.Time, _ bool) {
	_ = r.scheduleHub.Sender().Send(id, Schedule{NextRequeueAt: at})
}

// watchSchedule returns a channel that delivers id's current schedule on subscribe
// and every reschedule thereafter, until ctx is cancelled or the hub closes. The
// receiver is registered atomically with the snapshot read (subscribeSchedule), so
// no change between the two is lost. The queue's native (time, scheduled) is mapped
// to a Schedule here — the reconciler owns that domain type, not the queue.
func (r *reconciler) watchSchedule(ctx context.Context, id ObjectID) <-chan Schedule {
	var rx *conflate.Receiver[ObjectID, Schedule]
	at := r.work.subscribeSchedule(id, func() {
		rx = r.scheduleHub.ReceiverFunc(func(k ObjectID) bool { return k == id })
	})
	snapshot := Schedule{NextRequeueAt: at}

	out := make(chan Schedule)
	go func() {
		defer close(out)
		defer rx.Close()
		send := func(s Schedule) bool {
			select {
			case out <- s:
				return true
			case <-ctx.Done():
				return false
			}
		}
		if !send(snapshot) {
			return
		}
		for {
			s, err := rx.RecvContext(ctx)
			if err != nil {
				return // ctx cancelled or hub closed
			}
			if !send(s) {
				return
			}
		}
	}()
	return out
}

// run is the per-controller reconcile loop. It exits when ctx is cancelled.
//
// A resyncInterval <= 0 disables the periodic resync entirely: the loop then
// reconciles only in response to events (once the work queue lands), never on a
// timer.
func (r *reconciler) run(ctx context.Context) {
	// A reconciler built outside Register (e.g. in tests) may have no logger;
	// fall back to discard so the log sites below stay nil-safe.
	if r.logger == nil {
		r.logger = discardLogger
	}
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
	r.logger.Info("reconciler started", "workers", n, "resyncInterval", r.resyncInterval)
	// Drain the workers, then cancel any retry/RequeueAfter timers they left
	// pending so a torn-down reconciler doesn't leak timers that wake a dead queue,
	// and close the schedule hub so live WatchSchedule streams end instead of hanging
	// on a subscriber context that outlives the control plane.
	defer func() {
		wg.Wait()
		if r.work != nil {
			r.work.stop()
		}
		if r.scheduleHub != nil {
			r.scheduleHub.Close()
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
			r.logger.Info("reconciler stopped")
			return
		case <-resync:
			r.logger.Debug("resync tick")
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
					// The reconcile failure itself is already logged (with the
					// error) in typedController.reconcile; here we only add the
					// computed backoff delay at Debug.
					delay := r.nextBackoff(id)
					r.work.addAfter(id, delay)
					r.logger.Debug("requeued after failure", "id", id, "backoff", delay)
				} else {
					r.clearBackoff(id)
					if result.RequeueAfter > 0 {
						r.work.addAfter(id, result.RequeueAfter)
						r.logger.Debug("requeued", "id", id, "after", result.RequeueAfter)
					}
				}
			}
		}
	}
}
