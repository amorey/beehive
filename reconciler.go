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

	"github.com/amorey/beehive/internal/driver"
	"github.com/amorey/beehive/internal/logging"
)

const (
	defaultMaxRetryInterval  = 30 * time.Second
	defaultBaseRetryInterval = 1 * time.Second
)

// controllerAdapter is the non-generic view of a registered controller.
// Register wraps the user's Controller[Spec, Status] in a concrete adapter, so
// everything below — reconciler, work queue, Store — stays free of type
// parameters.
type controllerAdapter interface {
	// gone reports that id's row no longer exists, so the worker drops what is
	// queued for it rather than dispatching an ErrNotFound. The result is
	// normalized, and on a branch with no controller behind it carries only a
	// scheduling decision — the handshake is written in here, not by the worker.
	reconcile(ctx context.Context, id ObjectID) (result ReconcileResult, gone bool)
}

// typedController adapts a generic Controller[Spec, Status] to the non-generic
// controllerAdapter interface.
type typedController[Spec, Status any] struct {
	gk     GroupKind
	bh     *Beehive
	inner  Controller[Spec, Status]
	logger *slog.Logger // kind-tagged; set by Register
}

// stampObserved records raw's generation as observed, skipping the store call
// when the loaded row already carries it. That gate stands in for the store's
// clamp only while observed_generation is monotonic — keep
// SetObservedGeneration its sole writer.
func (t *typedController[Spec, Status]) stampObserved(ctx context.Context, raw *RawObject) error {
	if raw.ObservedGeneration != nil && *raw.ObservedGeneration >= raw.Generation {
		return nil
	}
	settled, err := t.bh.store.Objects().SetObservedGeneration(ctx, t.gk, raw.ID, raw.Generation)
	if err != nil || !settled {
		return err
	}
	// A logged write: the wake is not optional. See controllerClientImpl.wakeAfter.
	t.bh.signalKindWritten(ctx, t.gk)
	return nil
}

// log guards the rare path where a typedController is built outside Register.
func (t *typedController[Spec, Status]) log() *slog.Logger {
	if t.logger == nil {
		return logging.Discard
	}
	return t.logger
}

// reconcile loads the object and runs the controller. There is no enclosing
// transaction: each ControllerClient write commits on its own; a controller
// that needs atomicity uses ControllerClient.Within. GC runs afterwards, in its
// own transaction.
func (t *typedController[Spec, Status]) reconcile(ctx context.Context, id ObjectID) (ReconcileResult, bool) {
	log := t.log().With("id", id)

	load, err := t.bh.store.Objects().GetForReconcile(ctx, id)
	if errors.Is(err, ErrNotFound) {
		// Already collected between enqueue and now: a no-op success.
		log.DebugContext(ctx, "object gone before reconcile; skipping")
		return Settled(), true
	}
	if err != nil {
		return Fail(err), false
	}
	raw := &load.Object
	deleting := raw.DeletionRequestedAt != nil
	migrator := t.bh.migratorFor(t.gk)
	obj, err := rawToTyped[Spec, Status](raw, migrator)
	if err != nil {
		// Quarantine, as List and the watch polls do: returning the error would
		// retry the identical bytes forever under backoff. Treated as a no-op
		// success; the owed pass re-enqueues (and re-warns) until a rewrite or a
		// fixed build makes the row decode. Any owed reconcile_owed count is
		// deliberately left standing — this pass never serviced the wake.
		log.WarnContext(ctx, "skipping undecodable object; cannot reconcile", "err", err)
		if deleting {
			// Collect needs only the id, so a finalizer-free deletion-pending row
			// is still collected.
			gone, gcErr := t.bh.gcCollect(ctx, id)
			if gcErr != nil {
				log.ErrorContext(ctx, "garbage collection failed; will retry", "err", gcErr)
				return Fail(gcErr), false
			}
			return Settled(), gone
		}
		return Settled(), false
	}

	log.DebugContext(ctx, "reconciling", "generation", obj.Generation, "deleting", deleting)
	// Ended below, before beehive's own writes: nothing the controller captured
	// may write past that point.
	pass := newPassClient[Status](t.bh, t.gk, obj.ID)
	// The status as loaded: what a status write must differ from to be worth a
	// transaction.
	pass.baseline = newStatusBaseline(raw, migratorStatusVersion(migrator))
	// Normalized before any gate below reads it.
	result := t.inner.Reconcile(ctx, pass, obj).normalize()
	pass.end()
	if !result.succeeded() {
		// Warn, not Error: the retry loop absorbs failed reconciles. Don't return
		// yet — committed writes still need their GC follow-up below.
		log.WarnContext(ctx, "reconcile failed; will retry", "err", result.err)
	}
	// Subtract the whole count observed at load, not 1: this pass read current
	// state, which answers every wake outstanding then. Increments landing during
	// the pass sit above the observed count and survive. A failed subtraction is
	// left to the backstop rather than retried under backoff.
	if result.succeeded() && raw.ReconcileOwed != 0 {
		if err := t.bh.store.ReconcileOwed().Decrement(ctx, t.gk, id, raw.ReconcileOwed); err != nil {
			log.WarnContext(ctx, "failed to decrement the reconcile-owed count; backstop will retry", "err", err)
		}
	}
	// Record the dependency watermark from the cursor taken at load, never from
	// now: every read the controller made happened after the load, so a target
	// that moved during the pass stays counted as owed. HasDependencies only
	// skips the call; the store's own gate keeps the write safe against a
	// mid-pass collect.
	//
	// A lost write leaves the watermark low, which only over-reports staleness:
	// any target change this pass did not observe is above the stale pass's
	// cursor, so the next sweep finds it. Costs a redundant pass, never a strand.
	// See docs/adr/2026-08-03-stale-dependents-cursor.md.
	if result.succeeded() && load.HasDependencies {
		// A cancelled write is shutdown, not a lost pass.
		if err := t.bh.store.Dependencies().WatermarkSet(ctx, id, load.Cursor); err != nil && ctx.Err() == nil {
			log.WarnContext(ctx, "failed to record the dependency watermark; the next target change re-derives it", "err", err)
		}
	}
	// After the watermark and before the GC block; both orderings are
	// load-bearing. A failure only leaves the object unsettled; a cancelled write
	// and a row collected mid-pass are not faults.
	// See docs/adr/2026-08-18-beehive-owns-the-generation-handshake.md.
	if result.settles() {
		if err := t.stampObserved(ctx, raw); err != nil &&
			ctx.Err() == nil && !errors.Is(err, ErrNotFound) {
			log.WarnContext(ctx, "failed to record the observed generation; the object stays unsettled", "err", err)
		}
	}
	// GC runs in its own transaction over the controller's committed writes, so
	// a finalizer the controller just cleared is visible.
	if deleting {
		gone, gcErr := t.bh.gcCollect(ctx, id)
		if gcErr != nil {
			log.ErrorContext(ctx, "garbage collection failed; will retry", "err", gcErr)
			return Fail(cmp.Or(result.err, gcErr)), false
		}
		// Collected: drop the requeue delay and the failure rather than
		// rescheduling a dead id straight into ErrNotFound.
		if gone {
			log.DebugContext(ctx, "object collected")
			return Settled(), true
		}
	}
	return result, false
}

// reconciler drives the reconcile loop for a single registered controller.
// It owns the work queue, exponential backoff, and the periodic pass timers.
type reconciler struct {
	gk               GroupKind
	adapter          controllerAdapter
	store            Store
	work             *workQueue
	owedPassInterval time.Duration
	fullPassInterval time.Duration
	// individualPassInterval re-arms each object's own next pass; 0 disables it.
	individualPassInterval time.Duration
	// individualPassRand sources that schedule's jitter; nil means no jitter,
	// which is what a reconciler built outside Register wants.
	individualPassRand func() float64
	maxRetryInterval   time.Duration
	baseRetryInterval  time.Duration // zero falls back to defaultBaseRetryInterval
	concurrency        int           // worker goroutines; 0/1 = single-threaded
	// startupFullPass selects whether run also re-dispatches settled objects at
	// startup; owed work is drained regardless.
	startupFullPass bool
	// migrator is set by WithMigrator at Register, which copies it into
	// bh.migrators so the client path shares it. nil when the kind opted out.
	migrator Migrator
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

// enqueueUnsettled enqueues objects whose spec hasn't converged.
func (r *reconciler) enqueueUnsettled(ctx context.Context) {
	if r.store == nil {
		return
	}
	r.enqueueFrom(ctx, "unsettled", r.store.Objects().ListUnsettledIDs)
}

// enqueueReconcileOwed enqueues objects owed a durable dependency wake. Run
// unconditionally at startup: a wake bumps no generation, so the unsettled
// listing never sees it, and a crash between the token's commit and dispatch
// would otherwise strand the dependent.
func (r *reconciler) enqueueReconcileOwed(ctx context.Context) {
	if r.store == nil {
		return
	}
	r.enqueueFrom(ctx, "reconcile-owed", r.store.ReconcileOwed().ListIDs)
}

// enqueueOwedPass drains what the store records as owed: unconverged specs and
// owed dependency wakes. Both are column-derived and return nothing in a
// converged system, which is what lets this run on a frequent cadence. The two
// listings stay separate so a failure in one still lets the other through.
func (r *reconciler) enqueueOwedPass(ctx context.Context) {
	r.enqueueUnsettled(ctx)
	r.enqueueReconcileOwed(ctx)
}

// enqueueAll enqueues every object of the kind, settled or not, so controllers
// can re-confirm process-scoped state the owed pass would never wake.
func (r *reconciler) enqueueAll(ctx context.Context) {
	if r.store == nil {
		return
	}
	r.enqueueFrom(ctx, "all", r.store.Objects().ListIDs)
}

// scheduleNext arms the pass after this one. Every branch schedules something,
// or the cadence chain breaks. Never called for a collected object.
func (r *reconciler) scheduleNext(id ObjectID, result ReconcileResult) {
	switch {
	case !result.succeeded():
		if errors.Is(result.err, ErrInvalidResult) {
			r.log().Error("controller returned an unusable result", "id", id, "err", result.err)
		}
		delay := r.backoffNext(id)
		r.work.addAfter(id, delay, alarmBackoff)
		r.log().Debug("requeued after failure", "id", id, "backoff", delay)
	case result.requeueSet && result.requeueAfter > 0:
		r.backoffClear(id)
		r.work.addAfter(id, result.requeueAfter, alarmRequeueAfter)
		r.log().Debug("requeued", "id", id, "after", result.requeueAfter)
	case result.requeueSet:
		// RequeueAfter(0): the queue's per-object floor paces it.
		r.backoffClear(id)
		r.work.add(id)
		r.log().Debug("requeued", "id", id, "after", 0)
	case result.unsettled():
		r.backoffClear(id)
		after := r.unsettledRequeue()
		r.work.addAfter(id, after, alarmRequeueAfter)
		r.log().Debug("requeued unsettled", "id", id, "after", after)
	default:
		r.backoffClear(id)
		if d := r.individualPassInterval; d > 0 {
			after := d + r.spread(d, individualPassJitterFrac)
			r.work.addAfter(id, after, alarmRequeueAfter)
			r.log().Debug("requeued for the individual pass", "id", id, "after", after)
		}
	}
}

// spread returns a random part of frac of d, for staggering an arming. Zero
// without a source, which makes every schedule exact.
func (r *reconciler) spread(d time.Duration, frac float64) time.Duration {
	if r.individualPassRand == nil {
		return 0
	}
	return time.Duration(r.individualPassRand() * frac * float64(d))
}

// admitAll arms every object of the kind, spreading the armings across one
// interval. A per-object alarm is armed by a pass, so an object no pass reaches
// — settled by a previous process, owing nothing — needs this to enter the
// cadence at all. Runs once per process, not per tick.
func (r *reconciler) admitAll(ctx context.Context, window time.Duration) error {
	if r.store == nil {
		return nil
	}
	return r.enqueueSpread(ctx, r.store.Objects().ListIDs, window)
}

// admit runs the admission scan, retrying until it succeeds or ctx ends. With
// no periodic tick behind it, one failed listing would otherwise leave the kind
// polling nothing for the life of the process.
func (r *reconciler) admit(ctx context.Context, window time.Duration) {
	backoff := driver.Backoff{Base: admitRetryBase, Max: admitRetryMax}
	for {
		err := r.admitAll(ctx, window)
		if err == nil {
			return
		}
		r.log().WarnContext(ctx, "failed to admit objects to the individual pass; retrying",
			"group", r.gk.Group, "kind", r.gk.Kind, "err", err)
		if !backoff.Wait(ctx) {
			return
		}
	}
}

// log guards reconcilers built outside Register (e.g. minimal ones in tests).
func (r *reconciler) log() *slog.Logger {
	if r.logger == nil {
		return logging.Discard
	}
	return r.logger
}

// enqueueFrom enqueues the IDs returned by list. The work queue coalesces
// duplicates and defers ids mid-reconcile. A failed list is logged with its
// source — which backstop lost its pass matters — and retried on that pass's
// own next tick.
func (r *reconciler) enqueueFrom(ctx context.Context, source string, list func(context.Context, GroupKind) ([]ObjectID, error)) {
	if err := r.enqueueSpread(ctx, list, 0); err != nil {
		r.log().WarnContext(ctx, "failed to list objects to enqueue; this pass is skipped",
			"source", source, "group", r.gk.Group, "kind", r.gk.Kind, "err", err)
	}
}

// enqueueSpread enqueues the IDs returned by list, each after a random part of
// window — zero enqueues them all at once. It hands the listing error back
// rather than logging it, since a caller with no tick behind it must retry.
func (r *reconciler) enqueueSpread(ctx context.Context, list func(context.Context, GroupKind) ([]ObjectID, error), window time.Duration) error {
	if r.work == nil {
		return nil
	}
	ids, err := list(ctx, r.gk)
	if err != nil {
		return err
	}
	for _, id := range ids {
		// alarmAdmit yields to a schedule already pending; a zero window
		// enqueues outright, which is the startup pass and arms nothing.
		r.work.addAfter(id, r.spread(window, 1), alarmAdmit)
	}
	return nil
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

// unsettledRequeue is when a bare Unsettled comes back: the owed pass's cadence,
// since this alarm is that pass extended to the objects whose generation its
// listing cannot see. The default stands in when the pass is disabled.
func (r *reconciler) unsettledRequeue() time.Duration {
	if r.owedPassInterval > 0 {
		return r.owedPassInterval
	}
	return defaultUnsettledRequeue
}

// requeue makes id immediately dispatchable, optionally resetting its backoff
// ladder first. It is the engine behind Client.Requeue — a latency hint, not a
// synchronous run. Backoff is otherwise cleared only by a successful reconcile.
func (r *reconciler) requeue(id ObjectID, resetBackoff bool) {
	if resetBackoff {
		r.backoffClear(id)
	}
	r.requeueNow(id)
}

// requeueNow makes id immediately dispatchable, cancelling any pending delayed
// requeue. It deliberately does not touch the backoff ladder.
func (r *reconciler) requeueNow(id ObjectID) {
	if r.work != nil {
		r.work.requeueNow(id)
	}
}

// scheduleAt reports when the loop has scheduled id to be requeued. The zero
// Schedule means nothing is scheduled; only per-id timers count, so the actual
// next reconcile (via a periodic pass) may be sooner.
func (r *reconciler) scheduleAt(id ObjectID) Schedule {
	if r.work == nil {
		return Schedule{}
	}
	return r.work.scheduleAt(id)
}

// run is the per-controller reconcile loop. It exits when ctx is cancelled.
// Each ticker is disabled by a non-positive interval; fullPassInterval <= 0 —
// the default — disables only the full pass, never the owed pass convergence
// rests on.
func (r *reconciler) run(ctx context.Context) {
	if r.logger == nil {
		r.logger = logging.Discard
	}
	// Always drain what the store records as owed; skipping it would be a
	// correctness hole. Deletions in progress belong to the GC sweeper.
	r.enqueueOwedPass(ctx)
	// The startup full pass is a choice: it re-confirms process-scoped state a
	// restart invalidated, which no owed-work listing can see. A kind that
	// enables it may depend on it — see
	// docs/adr/2026-08-07-the-startup-pass-may-be-depended-on.md. The work queue
	// collapses the overlap with the owed pass.
	// One startup listing, whatever asked for it: the startup pass wants every
	// object dispatched at once, the individual pass wants them spread over one
	// interval, and a kind that asked for both takes the sooner window.
	n := max(r.concurrency, 1)
	var wg sync.WaitGroup
	switch {
	case r.individualPassInterval > 0:
		window := r.individualPassInterval
		if r.startupFullPass {
			window = 0
		}
		wg.Go(func() { r.admit(ctx, window) })
	case r.startupFullPass:
		r.enqueueAll(ctx)
	}
	for range n {
		wg.Go(func() {
			r.runWorker(ctx)
		})
	}
	r.logger.Info("reconciler started", "workers", n,
		"owedPassInterval", r.owedPassInterval, "fullPassInterval", r.fullPassInterval)
	// An owed pass disabled by accident fails silently — work stops being
	// re-derived — so say so. (The full passes are off by default and logged by
	// neither their presence nor absence.)
	if r.owedPassInterval <= 0 {
		r.logger.InfoContext(ctx, "owed pass disabled: work the store records as owed (unconverged specs, owed dependency wakes) is drained once at startup and not re-derived after; drive it with Store.ObjectsListUnsettledIDs + Client.Requeue",
			"group", r.gk.Group, "kind", r.gk.Kind)
	}
	// Drain the workers, then stop pending timers so a torn-down reconciler
	// doesn't wake a dead queue. stop publishes each id's final schedule, and
	// closing the hub's sender then ends live WatchSchedule streams after they
	// read it. Never Hub.Close here: that is hard tear-down with no drain, and a
	// receiver could lose the final value on a timing race. A racing publish
	// gets ErrClosed, which workQueue.publish expects.
	defer func() {
		wg.Wait()
		if r.work != nil {
			r.work.stop()
			r.work.closeHub()
		}
	}()

	fullPass, stopFullPass := driver.TickerChan(r.fullPassInterval)
	defer stopFullPass()
	owedPass, stopOwedPass := driver.TickerChan(r.owedPassInterval)
	defer stopOwedPass()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("reconciler stopped")
			return
		case <-owedPass:
			r.tick(ctx, "owed-pass", false)
		case <-fullPass:
			r.tick(ctx, "full-pass", true)
		}
	}
}

// tick runs one periodic pass. A full pass subsumes the owed set, so it stands
// in for it rather than running both.
func (r *reconciler) tick(ctx context.Context, driver string, full bool) {
	r.logger.Debug("periodic tick", "driver", driver, "fullPass", full)
	if full {
		r.enqueueAll(ctx)
		return
	}
	r.enqueueOwedPass(ctx)
}

// runWorker is the per-goroutine reconcile loop; concurrency > 1 runs several.
func (r *reconciler) runWorker(ctx context.Context) {
	// A nil channel blocks forever, the correct no-op when no queue is configured.
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
				result, gone := r.adapter.reconcile(ctx, id)
				// done releases the processing hold so a re-add that arrived
				// mid-reconcile becomes dispatchable; a collected row has nothing
				// left to dispatch, so drop what is queued for it instead — and
				// schedule nothing, or the next alarm resurrects the id.
				if gone {
					r.work.forget(id)
					continue
				}
				r.work.done(id)
				r.scheduleNext(id, result)
			}
		}
	}
}
