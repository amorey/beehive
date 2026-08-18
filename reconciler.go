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
	// normalized: a failure is a kindFail, never a separate error.
	reconcile(ctx context.Context, id ObjectID) (result ReconcileResult, gone bool)
}

// typedController adapts a generic Controller[Spec, Status] to the non-generic
// controllerAdapter interface.
type typedController[Spec, Status any] struct {
	gk    GroupKind
	bh    *Beehive
	inner Controller[Spec, Status]
	// client is built once at Register and is the same value Register returns.
	// Each pass wraps it in a scopedControllerClient rather than handing it out.
	client *controllerClientImpl[Status]
	logger *slog.Logger // kind-tagged; set by Register
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
		return Settled(0), true
	}
	if err != nil {
		return Fail(err), false
	}
	raw := &load.Object
	deleting := raw.DeletionRequestedAt != nil
	obj, err := rawToTyped[Spec, Status](raw, t.bh.migratorFor(t.gk))
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
			return Settled(0), gone
		}
		return Settled(0), false
	}

	log.DebugContext(ctx, "reconciling", "generation", obj.Generation, "deleting", deleting)
	// The controller gets a client scoped to this pass, ended below: nothing it
	// captures may write after beehive starts concluding the pass.
	pass := &scopedControllerClient[Status]{inner: t.client}
	// normalize runs before anything reads the result: every gate below names
	// the kinds it admits, and an un-normalized zero would satisfy none of them
	// while also not being a failure.
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
	// Last of the three, and after the watermark: a crash between them must
	// leave an unsettled object with a low watermark, which only over-reports
	// staleness, never a settled object whose watermark never landed. Before the
	// GC block, or a collect in this pass leaves the stamp writing to a row that
	// is gone.
	if result.settles() {
		t.bh.stampObserved(ctx, log, t.gk, id, raw)
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
			return Settled(0), true
		}
	}
	return result, false
}

// stampObserved records the generation the pass was handed, which raw carries —
// never a fresh read, or a spec change landing mid-pass would be marked observed
// by a pass that never saw it.
//
// Gated on the generation already in hand: a converged object costs no store
// call at all, which keeps the steady state off the store's single connection.
// The gate is equivalent to the store's own clamp only because
// observed_generation is monotonic, which holds because advanceObserved is its
// sole writer — do not reintroduce an unclamped handshake write.
//
// A failure here is not a failed reconcile: it leaves the object unsettled, and
// the unsettled listing re-derives it.
func (bh *Beehive) stampObserved(ctx context.Context, log *slog.Logger, gk GroupKind, id ObjectID, raw *RawObject) {
	if raw.ObservedGeneration != nil && *raw.ObservedGeneration >= raw.Generation {
		return
	}
	settled, err := bh.store.Objects().SetObservedGeneration(ctx, gk, id, raw.Generation)
	switch {
	case err == nil:
		if settled {
			bh.signalKindWritten(ctx, gk)
		}
	// A cancelled write is shutdown, and a collect from another kind's cascade
	// can take the row between the load and here: both are normal, not faults.
	case ctx.Err() != nil, errors.Is(err, ErrNotFound):
	default:
		log.WarnContext(ctx, "failed to record the observed generation; the object stays unsettled", "err", err)
	}
}

// reconciler drives the reconcile loop for a single registered controller.
// It owns the work queue, exponential backoff, and the periodic pass timers.
type reconciler struct {
	gk                GroupKind
	adapter           controllerAdapter
	store             Store
	work              *workQueue
	owedPassInterval  time.Duration
	fullPassInterval  time.Duration
	maxRetryInterval  time.Duration
	baseRetryInterval time.Duration // zero falls back to defaultBaseRetryInterval
	concurrency       int           // worker goroutines; 0/1 = single-threaded
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
	ids, err := list(ctx, r.gk)
	if err != nil {
		r.log().WarnContext(ctx, "failed to list objects to enqueue; this pass is skipped",
			"source", source, "group", r.gk.Group, "kind", r.gk.Kind, "err", err)
		return
	}
	for _, id := range ids {
		r.enqueue(id)
	}
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
	if r.startupFullPass {
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
				// left to dispatch, so drop what is queued for it instead.
				if gone {
					r.work.forget(id)
				} else {
					r.work.done(id)
				}
				switch {
				case !result.succeeded():
					if errors.Is(result.err, ErrInvalidResult) {
						r.logger.Error("controller returned an unusable result", "id", id, "err", result.err)
					}
					delay := r.backoffNext(id)
					r.work.addAfter(id, delay, alarmBackoff)
					r.logger.Debug("requeued after failure", "id", id, "backoff", delay)
				case result.requeueAfter > 0:
					r.backoffClear(id)
					r.work.addAfter(id, result.requeueAfter, alarmRequeueAfter)
					r.logger.Debug("requeued", "id", id, "after", result.requeueAfter)
				case result.kind == kindUnsettled:
					// Unsettled with no delay: re-dispatch as soon as the queue's
					// per-object floor allows, or nothing would schedule it.
					r.backoffClear(id)
					r.work.add(id)
					r.logger.Debug("requeued unsettled", "id", id)
				default:
					r.backoffClear(id)
				}
			}
		}
	}
}
