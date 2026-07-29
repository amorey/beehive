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
// transaction: each ControllerClient write commits on its own, so a write that lands
// before Reconcile returns an error stays committed, and the next pass works from it.
// A controller that needs several writes to land together wraps them in
// ControllerClient.Within. GC runs afterwards, in its own transaction (see
// gcCollect).
func (t *typedController[Spec, Status]) reconcile(ctx context.Context, id ObjectID) (Result, error) {
	log := t.log().With("id", id)

	load, err := t.bh.store.ObjectsGetForReconcile(ctx, id)
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
	raw := &load.Object
	// The already-loaded row's deletion flag is a fast path: it lets a
	// non-finalizing reconcile (the common case) skip collect's separate
	// transaction entirely, while still running GC on the pass where the controller
	// clears its last finalizer.
	deleting := raw.DeletionRequestedAt != nil
	obj, err := rawToTyped[Spec, Status](raw, t.bh.migratorFor(t.gk))
	if err != nil {
		// Quarantine, as List and the watch polls do (see rawToTyped's callers): a row
		// whose bytes don't decode can't be reconciled, and the bytes won't change
		// until someone rewrites the spec — which re-enqueues it. Returning the error
		// would instead retry the identical row forever under backoff, and the full pass
		// (enqueue-by-id, no decode) re-adds it every tick regardless. Treat it as a
		// no-op success so the worker drops it. GC still runs: collect needs only the
		// id, so a finalizer-free deletion-pending row is still collected here; a
		// finalizer-bearing one can't be cleared without a decode the controller can
		// never do, so it correctly waits for a fixed build.
		//
		// This re-WARNs each time the owed-pass tick re-enqueues the unsettled poison row
		// (it never settles, so ObjectsListUnsettledIDs keeps returning it): a bad row is an
		// ongoing operational fault, and a recurring warning at that coarse cadence keeps
		// it visible rather than logging once and going silent.
		//
		// Returning here also leaves any owed reconcile_owed count standing, which is
		// deliberate, not an oversight of the early return: the wake is owed because a
		// dependency moved, and this pass did not service it — the controller never
		// saw the object. Draining it would be exactly the silent discard the
		// quarantine is written to avoid, and would leave the dependent stale with no
		// record that a reconcile was owed. The cost is that the reconcile-owed backstop
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
	// owed-pass tick disabled the leftover would sit until the next process start.
	// Increments that land *during* the pass are above the observed count, so they
	// survive the subtraction and keep the object owed — and each brought its own
	// in-memory requeue, so nothing has to schedule the follow-up here. Skip the
	// write when nothing was owed. A failed subtraction is not fatal: the count
	// stays up and the backstop retries it, whereas requeueing on the error would
	// spin against a store that keeps failing.
	if reconcileErr == nil && raw.ReconcileOwed != 0 {
		if err := t.bh.store.ReconcileOwedDecrement(ctx, id, raw.ReconcileOwed); err != nil {
			log.WarnContext(ctx, "failed to decrement the reconcile-owed count; backstop will retry", "err", err)
		}
	}
	// The dependency watermark: what this pass observed of the rest of the store, so
	// the stale-dependents pass can re-derive whether a dependency has moved since.
	// It is an independent write with an independent gate — the decrement above fires
	// when something was owed, this fires when the object has dependencies — and the
	// cursor comes from the *load*, never from here: every read the controller made
	// happened after it, so a target that moved during the pass stays counted as owed
	// and is reconciled once more. Sampling it now would instead land above a change
	// the pass never saw, and leave the dependent stale with nothing left to find it.
	//
	// HasDependencies only skips the call; the store's own gate still runs when it is
	// true, which is what keeps the write safe against the gcCollect below removing
	// the row mid-pass. A false flag that went stale during the pass is safe too: the
	// object then has no watermark row, an absent row already means stale, and the
	// error is in the over-reconcile direction this design errs in throughout.
	//
	// A failed write is logged and swallowed, as the decrement above is: it leaves the
	// object stale, so the next stale pass re-derives it, where returning the error
	// would push a self-healing bookkeeping write into the backoff ladder and retry a
	// whole reconcile over it.
	if reconcileErr == nil && load.HasDependencies {
		if err := t.bh.store.DependencyWatermarksSet(ctx, id, load.Cursor); err != nil {
			log.WarnContext(ctx, "failed to record the dependency watermark; the stale-dependents pass will re-derive it", "err", err)
		}
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
	concurrency       int           // number of concurrent worker goroutines; 0/1 = single-threaded
	// startupFullPass selects whether run also re-dispatches settled objects at
	// startup; owed work is drained regardless.
	startupFullPass bool
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
	r.enqueueFrom(ctx, "unsettled", r.store.ObjectsListUnsettledIDs)
}

// enqueueReconcileOwed enqueues objects owed a durable dependency wake (see
// reconcile_owed). Like a pending deletion it is recorded, known-owed work: a
// wake bumps no generation, so the unsettled listing never sees it, and its
// in-memory requeue does not outlive the process — a crash between the token's
// commit and the dispatch leaves a stranded dependent nothing else re-checks.
// Run unconditionally at startup (like deletion-pending, not gated by the spec
// strategy): a wake owed is a specific known-owed reconcile, orthogonal to spec
// convergence, so declining the startup full pass must not suppress it.
func (r *reconciler) enqueueReconcileOwed(ctx context.Context) {
	if r.store == nil {
		return
	}
	r.enqueueFrom(ctx, "reconcile-owed", r.store.ReconcileOwedListIDs)
}

// enqueueOwedPass drains the work the store has recorded as owed: objects whose
// spec has not converged, and objects owed a durable dependency wake. Both are
// derived from a column, so they are cheap to ask for and return nothing in a
// converged system — which is what lets this run on a frequent cadence where a
// full pass could not.
//
// The two listings stay separate rather than being unioned in SQL so that a
// failure in one still lets the other through, and so enqueueFrom's log names
// which backstop lost its pass (see its doc).
func (r *reconciler) enqueueOwedPass(ctx context.Context) {
	r.enqueueUnsettled(ctx)
	r.enqueueReconcileOwed(ctx)
}

// enqueueAll enqueues every object of the kind, including ones whose spec is
// already settled. Used once at startup so controllers can re-confirm
// process-scoped state (e.g. liveness conditions, which a prior process's writes
// leave reading as "verifying") that the owed pass would never wake.
func (r *reconciler) enqueueAll(ctx context.Context) {
	if r.store == nil {
		return
	}
	r.enqueueFrom(ctx, "all", r.store.ObjectsListIDs)
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
// because what that costs differs sharply. The two owed-pass listings (unsettled,
// reconcile-owed) retry on the next owed-pass tick — unless that pass is off,
// where the
// startup pass was the only one, and a lost reconcile-owed listing defers every
// recorded owed wake to the next process start, the one path whose whole point is
// not losing them. The full pass ("all") rides the full-pass tick, which is off by
// default, so a failure there usually has no second chance in this process at all.
// Silence made all of that indistinguishable from "nothing was owed".
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
// drivers — the owed pass, the full pass and the dependency waker all reconcile
// without one —
// so the actual next reconcile may be sooner.
func (r *reconciler) nextRequeueAt(id ObjectID) (time.Time, bool) {
	if r.work == nil {
		return time.Time{}, false
	}
	return r.work.nextRequeueAt(id)
}

// run is the per-controller reconcile loop. It exits when ctx is cancelled.
//
// It runs two independent tickers, and each is disabled by a non-positive interval
// (tickerChan yields a nil channel, which never fires). fullPassInterval <= 0 — the
// default — disables the *full* pass only; the owed-pass tick still drives periodic
// passes over the work the store records as owed. That is the pass convergence rests
// on, which is why it is the one whose absence the log calls out.
func (r *reconciler) run(ctx context.Context) {
	// A reconciler built outside Register (e.g. in tests) may have no logger;
	// fall back to discard so the log sites below stay nil-safe.
	if r.logger == nil {
		r.logger = discardLogger
	}
	// Always drain the work the store records as owed. An object whose spec never
	// converged — a crash mid-reconcile, or a create that never settled — and one owed
	// a durable dependency wake are both already owed a pass, so skipping them would
	// be a correctness hole, not a saving. Deletions in progress belong to the GC
	// sweeper, whose own startup pass routes them: a registered kind is queued so its
	// controller can clear finalizers, a client-only kind is collected directly (see
	// deletionAdvance). Listing them here too would only duplicate that, per kind.
	r.enqueueOwedPass(ctx)
	// The startup full pass is the part that is a choice, and it is off unless asked
	// for. It re-confirms state belonging to a process that a restart invalidated —
	// liveness conditions read as "verifying" until this process rewrites them — which
	// no owed-work listing can see because nothing records it as outstanding. That is
	// its whole job: nothing above depends on it, and nothing new may come to. It
	// re-lists what the owed pass just listed, and the work queue collapses the overlap
	// rather than reconciling twice.
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
	// Say so when a periodic driver is off. Each is a supported choice, so this is not
	// an error — but if it happened by accident, from an unset config field or a
	// duration that failed to parse, the failure mode is silence: work stops being
	// re-derived and nothing says so. Info level, because the caller still has
	// recourse.
	//
	// Neither full pass being off is logged: both are off by default, so saying so would
	// put two lines in every process's startup for the ordinary case. Their absence is
	// also not a backstop lost, since no reconcile depends on either.
	if r.owedPassInterval <= 0 {
		r.logger.InfoContext(ctx, "owed pass disabled: work the store records as owed (unconverged specs, owed dependency wakes) is drained once at startup and not re-derived after; drive it with Store.ObjectsListUnsettledIDs + Client.Requeue",
			"group", r.gk.Group, "kind", r.gk.Kind)
	}
	// Drain the workers, then cancel any retry/RequeueAfter timers they left
	// pending so a torn-down reconciler doesn't leak timers that wake a dead queue.
	// A live SchedulesWatch needs nothing here: it polls, so it simply reports the
	// stopped queue's empty schedule until its own context ends.
	defer func() {
		wg.Wait()
		if r.work != nil {
			r.work.stop()
		}
	}()

	fullPass, stopFullPass := tickerChan(r.fullPassInterval)
	defer stopFullPass()
	// The owed-pass tick is the cheap, frequent one: it drains only what the store
	// records as owed, so it can run often without scaling with the object count.
	owedPass, stopOwedPass := tickerChan(r.owedPassInterval)
	defer stopOwedPass()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("reconciler stopped")
			return
		case <-owedPass:
			// Owed work only. A dependency wake this listing cannot see — a *settled*
			// dependent is invisible to it, since its own generation never moved — is
			// the waker's, which scans the write log rather than re-deriving state.
			r.tick(ctx, "owed-pass", false)
		case <-fullPass:
			// The full pass: every object, converged or not.
			r.tick(ctx, "full-pass", true)
		}
	}
}

// tick runs one periodic driver's pass. full means every object of the kind,
// converged or not; otherwise only the work the store records as owed. A full
// pass subsumes the owed-pass set, so it stands in for it rather than running both
// — which is why both drivers share this one body.
func (r *reconciler) tick(ctx context.Context, driver string, full bool) {
	r.logger.Debug("periodic tick", "driver", driver, "fullPass", full)
	if full {
		r.enqueueAll(ctx)
		return
	}
	r.enqueueOwedPass(ctx)
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
				// a periodic pass) that arrived mid-reconcile becomes dispatchable. The
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
