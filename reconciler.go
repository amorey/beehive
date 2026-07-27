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
	"sync/atomic"
	"time"

	"github.com/amorey/gobus/conflate"
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
	// them after Reconcile returns (see DependenciesDelete).
	wakes := &pendingWakes{}
	ctx = withPendingWakes(ctx, wakes)

	raw, err := t.bh.store.ObjectsGet(ctx, id)
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
		// Quarantine, as List and adaptObjectStream do (see rawToTyped's callers): a row
		// whose bytes don't decode can't be reconciled, and the bytes won't change
		// until someone rewrites the spec — which re-enqueues it. Returning the error
		// would instead retry the identical row forever under backoff, and resync
		// (enqueue-by-id, no decode) re-adds it every tick regardless. Treat it as a
		// no-op success so the worker drops it. GC still runs: collect needs only the
		// id, so a finalizer-free deletion-pending row is still collected here; a
		// finalizer-bearing one can't be cleared without a decode the controller can
		// never do, so it correctly waits for a fixed build.
		//
		// This re-WARNs each time the catchup tick re-enqueues the unsettled poison row
		// (it never settles, so ObjectsListUnsettledIDs keeps returning it): a bad row is an
		// ongoing operational fault, and a recurring warning at that coarse cadence keeps
		// it visible rather than logging once and going silent.
		//
		// Returning here also leaves any owed reconcile_owed count standing, which is
		// deliberate, not an oversight of the early return: the wake is owed because a
		// dependency moved, and this pass did not service it — the controller never
		// saw the object. Draining it would be exactly the silent discard the
		// quarantine is written to avoid, and would leave the dependent stale with no
		// record that a reconcile was owed. The cost is that the pending-wake backstop
		// re-enqueues this row (and re-warns) until the bytes decode again, which is
		// the same recurring-visibility trade as above, and it self-clears the moment a
		// fixed build lets the pass run to the decrement below.
		log.WarnContext(ctx, "skipping undecodable object; cannot reconcile", "err", err)
		if deleting {
			if _, gcErr := t.bh.gcCollect(ctx, id); gcErr != nil {
				log.ErrorContext(ctx, "garbage collection failed; will retry", "err", gcErr)
				return Result{}, gcErr
			}
		}
		return Result{}, nil
	}

	log.DebugContext(ctx, "reconciling", "generation", obj.Generation, "deleting", deleting)
	result, reconcileErr := t.inner.Reconcile(ctx, t.client, obj)
	if reconcileErr != nil {
		// Warn, not Error: a failed reconcile is expected churn the retry loop
		// absorbs. We don't return yet — the controller's committed writes still need
		// their GC follow-up below (see func doc), or a freed object could strand.
		log.WarnContext(ctx, "reconcile failed; will retry", "err", reconcileErr)
	}
	// A successful pass read the target's current state, which addresses every wake
	// outstanding when it loaded the object — so subtract that whole count, not one.
	// Subtracting one would leave a residual with nothing to re-enqueue it: the work
	// queue coalesces, so the backstop's single enqueue is already spent, and with the
	// catchup tick disabled the leftover would sit until the next process start.
	// Increments that land *during* the pass are above the observed count, so they
	// survive the subtraction and keep the object owed — and each brought its own
	// in-memory requeue, so nothing has to schedule the follow-up here. Skip the
	// write when nothing was owed. A failed subtraction is not fatal: the count
	// stays up and the backstop retries it, whereas requeueing on the error would
	// spin against a store that keeps failing.
	if reconcileErr == nil && raw.ReconcileOwed != 0 {
		if err := t.bh.store.ReconcileOwedDecrement(ctx, id, raw.ReconcileOwed); err != nil {
			log.WarnContext(ctx, "failed to decrement pending-wake count; backstop will retry", "err", err)
		}
	}
	// Advance any targets the controller freed via DependenciesDelete, so a
	// now-unreferenced deletion-pending target is re-examined without waiting on the GC
	// sweep. gcAdvance (not enqueueIfRegistered) rather than a plain wake because the
	// follow-up a deletion owes is a collect, not a reconcile — it routes by the
	// target's own kind, and a client-only target falls to the sweeper's next tick.
	for _, tgt := range wakes.targets {
		t.bh.gcAdvance(ctx, tgt.GroupKind(), tgt.ID)
	}
	// GC runs in its own transaction over the controller's committed writes, so a
	// finalizer the controller just cleared is visible.
	if deleting {
		gone, gcErr := t.bh.gcCollect(ctx, id)
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
	gk      GroupKind
	adapter controllerAdapter
	store   Store
	work    *workQueue
	// scheduleHub fans each object's next-requeue changes out to SchedulesWatch
	// subscribers, keyed by ObjectID with latest-value-per-id coalescing. The work
	// queue feeds it through onSchedule; Close (on teardown) ends live streams.
	scheduleHub       *conflate.Hub[ObjectID, Schedule]
	catchupInterval   time.Duration
	resyncInterval    time.Duration
	maxRetryInterval  time.Duration
	baseRetryInterval time.Duration // zero falls back to defaultBaseRetryInterval
	concurrency       int           // number of concurrent worker goroutines; 0/1 = single-threaded
	// startupResync selects whether run also re-dispatches settled objects at
	// startup; owed work is drained regardless.
	startupResync bool
	// resyncOnce / resyncAlways let an outside signal escalate this reconciler's
	// catchup tick into a full pass. They are deliberately domain-agnostic — "one
	// full pass" and "full passes from now on", never the waker vocabulary that
	// sets them today — the same split the work queue keeps behind onSchedule.
	// Their zero value is the un-escalated state, so a reconciler built outside
	// Register needs no wiring.
	resyncOnce   atomic.Bool
	resyncAlways atomic.Bool
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

	// afterScheduleWatch, when set, runs after a scheduleWatch goroutine exits.
	// Tests use it to await teardown without reading the channel — a read would
	// let a parked send succeed and mask the ctx.Done/close arm under test.
	afterScheduleWatch func()
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
	r.enqueueFrom(ctx, "unsettled", r.store.ObjectsListUnsettledIDs)
}

// enqueuePendingWake enqueues objects owed a durable dependency wake (see
// reconcile_owed). Like a pending deletion it is recorded, known-owed work: a
// wake bumps no generation, so the unsettled listing never sees it, and its
// in-memory requeue does not outlive the process — a crash between the token's
// commit and the dispatch leaves a stranded dependent nothing else re-checks.
// Run unconditionally at startup (like deletion-pending, not gated by the spec
// strategy): a wake owed is a specific known-owed reconcile, orthogonal to spec
// convergence, so declining the startup resync must not suppress it.
func (r *reconciler) enqueuePendingWake(ctx context.Context) {
	if r.store == nil {
		return
	}
	r.enqueueFrom(ctx, "pending-wake", r.store.ReconcileOwedListIDs)
}

// hasPeriodicPass reports whether this reconciler has a periodic driver left for
// an escalation (resyncNextTick / resyncEveryTick) to ride. Kept next to the
// knobs it reads so the interval semantics stay inside the reconciler.
func (r *reconciler) hasPeriodicPass() bool {
	return r.catchupInterval > 0 || r.resyncInterval > 0
}

// resyncNextTick makes this reconciler's next catchup tick a full pass, once. For
// a signal that dropped a single wake: it must not degrade the reconciler
// permanently.
func (r *reconciler) resyncNextTick() { r.resyncOnce.Store(true) }

// resyncEveryTick makes every later catchup tick a full pass. For a signal that
// will keep dropping wakes for the life of the process, where a single pass would
// repair the moment of failure and strand everything after it. Never cleared:
// nothing that sets it has a recovery path.
func (r *reconciler) resyncEveryTick() { r.resyncAlways.Store(true) }

// tickResyncs reports whether this catchup tick runs a full pass instead,
// consuming the one-shot if that is what decided it.
//
// The standing reason is checked first — via ||'s short-circuit — so the one-shot
// survives a tick it did not decide: that tick runs a full pass regardless, and
// burning the flag there would discard a repair still owed if the standing reason
// later goes away.
func (r *reconciler) tickResyncs() bool {
	return r.resyncAlways.Load() || r.resyncOnce.Swap(false)
}

// enqueueCatchup drains the work the store has recorded as owed: objects whose
// spec has not converged, and objects owed a durable dependency wake. Both are
// derived from a column, so they are cheap to ask for and return nothing in a
// converged system — which is what lets this run on a frequent cadence where a
// full pass could not.
//
// The two listings stay separate rather than being unioned in SQL so that a
// failure in one still lets the other through, and so enqueueFrom's log names
// which backstop lost its pass (see its doc).
func (r *reconciler) enqueueCatchup(ctx context.Context) {
	r.enqueueUnsettled(ctx)
	r.enqueuePendingWake(ctx)
}

// enqueueAll enqueues every object of the kind, including ones whose spec is
// already settled. Used once at startup so controllers can re-confirm
// process-scoped state (e.g. liveness conditions, which a prior process's writes
// leave reading as "verifying") that the unsettled-only resync would never wake.
// It reports whether the listing succeeded, so a caller that spent a one-shot
// escalation on this pass can tell that the pass never happened.
func (r *reconciler) enqueueAll(ctx context.Context) bool {
	if r.store == nil {
		// Nothing to list, so nothing was lost — reporting failure here would re-arm
		// an escalation that can never run.
		return true
	}
	return r.enqueueFrom(ctx, "all", r.store.ObjectsListIDs)
}

// log returns a non-nil logger, guarding reconcilers built outside Register (e.g.
// the minimal ones in tests): run assigns discardLogger, but the enqueue helpers
// are reachable without it.
func (r *reconciler) log() *slog.Logger {
	if r.logger == nil {
		return discardLogger
	}
	return r.logger
}

// enqueueFrom enqueues the IDs returned by list. The work queue coalesces an ID
// that is already queued and defers one that is mid-reconcile (re-queuing it via
// done), so this never triggers a duplicate or concurrent reconcile.
//
// A failed list is logged, not retried: source names which backstop lost its pass,
// because what that costs differs sharply. The two catchup listings (unsettled,
// pending-wake) retry on the next catchup tick — unless catchup is off, where the
// startup pass was the only one, and a lost pending-wake listing defers every
// recorded owed wake to the next process start, the one path whose whole point is
// not losing them. The full pass ("all") rides the resync tick, which is off by
// default, so a failure there usually has no second chance in this process at all —
// hence the return value, which lets a caller that spent a one-shot escalation on
// the pass re-arm it. Silence made all of that indistinguishable from "nothing was
// owed".
//
// It reports whether the listing succeeded (an empty result is still a success:
// nothing was owed).
func (r *reconciler) enqueueFrom(ctx context.Context, source string, list func(context.Context, GroupKind) ([]ObjectID, error)) bool {
	ids, err := list(ctx, r.gk)
	if err != nil {
		r.log().WarnContext(ctx, "failed to list objects to enqueue; this pass is skipped",
			"source", source, "group", r.gk.Group, "kind", r.gk.Kind, "err", err)
		return false
	}
	for _, id := range ids {
		r.enqueue(id)
	}
	return true
}

// backoffNext returns the next retry delay for id and doubles it for next time,
// capped at maxRetryInterval.
func (r *reconciler) backoffNext(id ObjectID) time.Duration {
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

// backoffClear resets the retry delay for id after a successful reconcile.
func (r *reconciler) backoffClear(id ObjectID) {
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
		r.backoffClear(id)
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

// scheduleMerge is the schedule hub's coalescing policy: latest value wins and the
// slot is never annihilated. Unlike the object watch, "unscheduled" (the zero
// Schedule) is a real gauge value a subscriber must observe, so it is kept, not
// dropped — a slow reader converges to the id's current schedule.
func scheduleMerge(_, next Schedule) (Schedule, bool) { return next, true }

// schedulePublish feeds one work-queue schedule change into the hub. It is the
// onSchedule callback, so it runs under the queue lock: it maps the queue's native
// (time, scheduled) to the public Schedule (unscheduled folds to the zero time),
// then Sends — which never blocks, and a closed hub drops it. The scheduled bool is
// redundant with a zero time here, so it is ignored.
func (r *reconciler) schedulePublish(id ObjectID, at time.Time, _ bool) {
	_ = r.scheduleHub.Sender().Send(id, Schedule{NextRequeueAt: at})
}

// scheduleWatch returns a channel that delivers id's current schedule on subscribe
// and every reschedule thereafter, until ctx is cancelled or the hub closes. The
// receiver is registered atomically with the snapshot read (scheduleSubscribe), so
// no change between the two is lost. The queue's native (time, scheduled) is mapped
// to a Schedule here — the reconciler owns that domain type, not the queue.
func (r *reconciler) scheduleWatch(ctx context.Context, id ObjectID) <-chan Schedule {
	var rx *conflate.Receiver[ObjectID, Schedule]
	at := r.work.scheduleSubscribe(id, func() {
		rx = r.scheduleHub.Receiver(r.scheduleHub.WithKeyFilter(func(k ObjectID) bool { return k == id }))
	})
	snapshot := Schedule{NextRequeueAt: at}

	out := make(chan Schedule)
	go func() {
		if r.afterScheduleWatch != nil {
			defer r.afterScheduleWatch()
		}
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
			sev, err := rx.RecvContext(ctx)
			if err != nil {
				return // ctx cancelled or hub closed
			}
			if !send(sev.Value) {
				return
			}
		}
	}()
	return out
}

// run is the per-controller reconcile loop. It exits when ctx is cancelled.
//
// It runs two independent tickers, and each is disabled by a non-positive interval
// (tickerChan yields a nil channel, which never fires). resyncInterval <= 0 — the
// default — disables the *full* pass only; the catchup tick still drives periodic
// passes over the work the store records as owed. Only with both off does the loop
// reconcile purely in response to events and its own startup passes, which is why
// that combination is the one the log calls out.
func (r *reconciler) run(ctx context.Context) {
	// A reconciler built outside Register (e.g. in tests) may have no logger;
	// fall back to discard so the log sites below stay nil-safe.
	if r.logger == nil {
		r.logger = discardLogger
	}
	// Drain the work the store records as owed, always: an object whose spec never
	// converged (crashed mid-reconcile, or created and never settled) and one owed a
	// durable dependency wake are both *already* owed a pass, so declining them is a
	// correctness hole rather than a saving. In-progress deletions are the GC
	// sweeper's, whose own unconditional startup pass routes them — a registered kind
	// enqueued so its controller can clear finalizers, a client-only kind collected
	// directly (see advanceDeletion). Doing that here too meant a per-kind
	// listing that only duplicated what the sweeper had to do cross-kind.
	r.enqueueCatchup(ctx)
	// The startup resync is the part that is a choice: a full pass re-confirms
	// process-scoped state a restart invalidated (liveness conditions read as
	// "verifying" until this process rewrites them), which no owed-work listing can
	// see. enqueueAll is a superset of the catchup set, so the overlap above is
	// coalesced by the work queue rather than reconciled twice.
	if r.startupResync {
		r.enqueueAll(ctx)
	}

	n := max(r.concurrency, 1)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			r.runWorker(ctx)
		})
	}
	r.logger.Info("reconciler started", "workers", n,
		"catchupInterval", r.catchupInterval, "resyncInterval", r.resyncInterval)
	// Say so when a periodic driver is off. Each is a supported choice, so this is
	// not an error — but reached by accident (an unset config field, a duration that
	// failed to parse) the failure mode is silence: work quietly stops being
	// re-derived and nothing reports it. Info, because the caller retains recourse.
	//
	// Resync-off is deliberately not logged: it is the default, so narrating it
	// would put a line in every process's startup for the ordinary case.
	if r.catchupInterval <= 0 {
		r.logger.InfoContext(ctx, "catchup disabled: work the store records as owed (unconverged specs, owed dependency wakes) is drained once at startup and not re-derived after; drive it with Store.ObjectsListUnsettledIDs + Client.Requeue",
			"group", r.gk.Group, "kind", r.gk.Kind)
	}
	// Drain the workers, then cancel any retry/RequeueAfter timers they left
	// pending so a torn-down reconciler doesn't leak timers that wake a dead queue,
	// and close the schedule hub so live SchedulesWatch streams end instead of hanging
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

	resync, stopResync := tickerChan(r.resyncInterval)
	defer stopResync()
	// The catchup tick is the cheap, frequent one: it drains only what the store
	// records as owed, so it can run often without scaling with the object count.
	catchup, stopCatchup := tickerChan(r.catchupInterval)
	defer stopCatchup()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("reconciler stopped")
			return
		case <-catchup:
			// An escalation turns this tick into a full pass: a dropped or dead
			// dependency wake leaves a *settled* dependent stale, which no owed-work
			// listing can see. The escalation rides the catchup ticker rather than the
			// resync one because resync is off by default — a repair that depended on
			// an opt-in knob would not run where it is needed most.
			//
			// Re-arm when the full pass could not list: tickResyncs already
			// consumed the one-shot, so leaving it spent would discard a repair
			// that never ran — and with resync off by default nothing else would
			// reach the settled dependents it was owed to. Harmless when the
			// standing reason decided the tick instead: it leaves the one-shot
			// armed anyway.
			if !r.tick(ctx, "catchup", r.tickResyncs()) {
				r.resyncNextTick()
			}
		case <-resync:
			// The full pass: every object, converged or not.
			r.tick(ctx, "resync", true)
		}
	}
}

// tickerChan returns the tick channel for a periodic driver, plus its stop func.
// A non-positive interval means the driver is disabled: time.NewTicker would
// panic, and a nil channel is the right no-op — it blocks forever in a select.
func tickerChan(d time.Duration) (<-chan time.Time, func()) {
	if d <= 0 {
		return nil, func() {}
	}
	t := time.NewTicker(d)
	return t.C, t.Stop
}

// tick runs one periodic driver's pass. full means every object of the kind,
// converged or not; otherwise only the work the store records as owed. A full
// pass subsumes the catchup set, so it stands in for it rather than running both
// — which is why both drivers share this one body.
//
// It reports whether a full pass completed its listing. A non-full pass always
// reports true: its listings retry on their own next tick and no one-shot was
// spent on them, so a failure there must not be mistaken for a lost escalation.
func (r *reconciler) tick(ctx context.Context, driver string, full bool) bool {
	r.logger.Debug("periodic tick", "driver", driver, "fullPass", full)
	if full {
		return r.enqueueAll(ctx)
	}
	r.enqueueCatchup(ctx)
	return true
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
					delay := r.backoffNext(id)
					r.work.addAfter(id, delay)
					r.logger.Debug("requeued after failure", "id", id, "backoff", delay)
				} else {
					r.backoffClear(id)
					if result.RequeueAfter > 0 {
						r.work.addAfter(id, result.RequeueAfter)
						r.logger.Debug("requeued", "id", id, "after", result.RequeueAfter)
					}
				}
			}
		}
	}
}
