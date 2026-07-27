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

// Package beehive is an embedded, Kubernetes-inspired control plane backed by a
// durable store: users declare desired Spec and controllers reconcile actual
// state toward it, level-triggered, coordinating only through the shared store.
package beehive

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/amorey/gobus/conflate"
)

const (
	defaultCatchupInterval = 30 * time.Second
	// The full pass scales with the object count rather than with what is owed, so
	// it is opt-in: the catchup tick and the startup pass cover convergence, and a
	// deployment that wants periodic re-confirmation asks for it.
	defaultResyncInterval time.Duration = 0
	defaultGCInterval                   = 30 * time.Second
)

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
	store Store
	// catchupInterval paces the cheap tick that drains work the store has recorded
	// as owed. Separate from resyncInterval because the two scale differently: the
	// owed set is bounded by what is actually outstanding, a full pass by the
	// object count.
	catchupInterval time.Duration
	resyncInterval  time.Duration
	// gcInterval paces the global GC sweeper (deletion-pending collection and
	// event-log retention). It is deliberately separate from the reconcile
	// intervals: collecting dead rows and re-confirming live ones are different
	// jobs with different costs, and one number governing both means tuning either
	// moves the other. Always positive when the Beehive came from New: WithGCInterval
	// rejects a non-positive value (see its doc for why GC alone can't be disabled),
	// so every error path in the sweeper has a next tick to retry on.
	gcInterval  time.Duration
	concurrency int // default worker count for all controllers; 0/1 = single-threaded
	// Event-log retention, applied globally by the GC sweeper (see WithEventRetention).
	// Zero on both disables the sweep — the log grows unbounded until configured.
	eventRetentionPerObject int
	eventRetentionMaxAge    time.Duration
	// startupResync is the default startup full-pass choice copied into each
	// reconciler. Its zero value is false, so New sets the true default explicitly.
	startupResync bool
	// logger and logLevel are the user-supplied logging config (nil logger =
	// disabled). They stay raw until Start resolves them via resolveLogger; each
	// reconciler inherits them as its own default (see Register).
	logger   *slog.Logger
	logLevel slog.Leveler

	mu          sync.Mutex
	reconcilers map[GroupKind]*reconciler
	// migrators holds the per-kind schema-version converters registered via
	// WithMigrator. It is the single source of truth shared by both decode paths
	// (the user-facing client and the reconciler), so a migrator can't be wired to
	// one but not the other. nil entry / missing key means the kind has none.
	migrators map[GroupKind]Migrator
	// order preserves registration order so Start subscribes dependency wakers
	// and launches reconcile loops deterministically, rather than in random map
	// order.
	order  []*reconciler
	waker  *waker
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

// Start brings the control plane up: it launches the dependency wakers, the
// per-controller reconcile loops, and the GC sweeper. On success it returns a
// stop function that tears the control plane back down (see stop). It is an
// error to start twice or after stop; on error the returned stop is nil. Beehive
// is a one-shot object: once stopped, create a new instance.
//
// startCtx bounds the startup phase only — a startCtx that's already cancelled
// aborts startup. The long-lived reconcile loops do not derive from startCtx;
// the run lifetime ends only when the returned stop is called.
func (bh *Beehive) Start(startCtx context.Context) (func(context.Context) error, error) {
	bh.mu.Lock()
	defer bh.mu.Unlock()
	switch bh.state {
	case beehiveStopped:
		return nil, fmt.Errorf("beehive: already stopped; create a new Beehive to restart")
	case beehiveRunning:
		return nil, fmt.Errorf("beehive: already started")
	}

	// Resolve the control plane's own logger once: nil becomes the discard logger
	// so the goroutines below (GC sweeper, dependency wakers) log unconditionally.
	bh.logger = resolveLogger(bh.logger, bh.logLevel)

	// runCtx lives for the lifetime of the control plane and drives the
	// reconcile loops. It is cancelled by Stop.
	runCtx, cancel := context.WithCancel(context.Background())
	bh.cancel = cancel

	// Controllers own no startup work in beehive — any background work belongs to
	// the embedding application. Abort only if the caller's context is already
	// done before we launch the loops.
	if err := startCtx.Err(); err != nil {
		cancel()
		return nil, fmt.Errorf("beehive: start aborted: %w", err)
	}

	// Subscribe the dependency waker BEFORE launching any reconcile loop: a
	// controller's startup reconcile can modify a target the instant it runs, and
	// that change must not be published before the waker's receiver is registered —
	// otherwise dependents go unwoken under configurations that rely on dependency
	// events (e.g. a settled dependent, which no owed-work listing can see, with
	// every ticker disabled). start subscribes synchronously for that reason; only
	// consuming and retrying are asynchronous.
	bh.waker.start(runCtx)

	// Now launch the reconcile loops.
	for _, r := range bh.order {
		bh.wg.Go(func() {
			r.run(runCtx)
		})
	}

	// The global GC sweeper collects deletion-pending objects of client-only
	// kinds, which no per-controller backstop reaches. Counted in wg so Stop
	// drains it.
	bh.wg.Go(func() {
		bh.gcSweeperRun(runCtx)
	})

	bh.state = beehiveRunning
	bh.logger.Info("control plane started", "controllers", len(bh.order))
	return func(stopCtx context.Context) error { return bh.stop(stopCtx) }, nil
}

// gcSweeperRun is the global garbage-collection backstop. The per-controller
// reconcile loop runs collect for its own kind; this sweeps every kind, so a
// deletion-pending object of a client-only kind (no registered controller) is
// still collected — otherwise it would strand and RESTRICT-block its owner's
// delete forever. It sweeps once at startup and then on the GC cadence.
//
// Every failure inside a sweep is logged and swallowed, which is only sound
// because there is always a next tick: WithGCInterval rejects a non-positive
// interval, so a transient error costs one cadence of latency rather than
// stranding a row for the life of the process.
func (bh *Beehive) gcSweeperRun(ctx context.Context) {
	if bh.gcInterval <= 0 {
		// Unreachable through New — the default is positive and WithGCInterval rejects
		// anything else. Guarded anyway so a Beehive assembled field-by-field (tests
		// that want the sweeper's own enqueues out of the way) simply has no sweeper
		// instead of panicking in NewTicker.
		return
	}
	bh.deletionPendingSweep(ctx)
	bh.eventRetentionSweep(ctx)
	ticker := time.NewTicker(bh.gcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bh.deletionPendingSweep(ctx)
			bh.eventRetentionSweep(ctx)
		}
	}
}

// eventRetentionSweep trims the event log to the configured retention (see
// WithEventRetention). It is a no-op unless a bound is set, and best-effort: a
// failed sweep is retried on the next cadence tick.
func (bh *Beehive) eventRetentionSweep(ctx context.Context) {
	if bh.eventRetentionPerObject <= 0 && bh.eventRetentionMaxAge <= 0 {
		return
	}
	if _, err := bh.store.EventsSweep(ctx, bh.eventRetentionPerObject, bh.eventRetentionMaxAge); err != nil {
		bh.log().Warn("event retention sweep failed; retry next sweep", "err", err)
	}
}

// deletionPendingSweep drives every deletion-pending object one step closer to
// removal (see advanceDeletion for the routing).
func (bh *Beehive) deletionPendingSweep(ctx context.Context) {
	rows, err := bh.store.DeletionRequestsList(ctx)
	if err != nil {
		bh.logger.Warn("gc sweep: listing deletion-pending objects failed; retry next sweep", "err", err)
		return
	}
	for _, row := range rows {
		// Best-effort: a transient error is retried on the next sweep, but it is
		// still logged — for a client-only kind this sweep is the only collector,
		// so a row that fails every time would otherwise strand silently and
		// RESTRICT-block its owner's delete forever. ErrNotFound is the benign
		// already-collected race and stays quiet.
		if err := bh.deletionAdvance(ctx, row.GroupKind(), row.ID); err != nil && !errors.Is(err, ErrNotFound) {
			bh.logger.Warn("gc sweep: collecting object failed; retry next sweep",
				"group", row.Group, "kind", row.Kind, "id", row.ID, "err", err)
		}
	}
}

// deletionAdvance drives one deletion-pending object a step closer to removal,
// routing on whether its kind has a controller. The GC sweeper is its only caller
// — the event-driven path (advanceGCNow) deliberately only requeues, leaving every
// collect to run on the sweeper's goroutine rather than a caller's.
//
// The routing is load-bearing, not an optimization. collect cannot clear a
// finalizer: it cascades to owned children and then returns while any finalizer
// remains, because releasing one is the controller's decision. So an object of a
// registered kind must be *enqueued*, letting its reconcile loop run the
// controller (which clears the finalizer and collects in the same pass); calling
// collect on it directly would make no progress, on every sweep, forever. A
// client-only kind has no reconcile loop to enqueue onto, so it is collected
// here — which is the whole reason the global sweep exists.
//
// Both arms are safe to repeat: enqueue coalesces, and collect is a no-op while
// finalizers or live referrers remain and idempotent if another path got there
// first. The collect error is returned rather than logged so the caller can report
// it in its own terms; the enqueue arm always returns nil.
func (bh *Beehive) deletionAdvance(ctx context.Context, gk GroupKind, id ObjectID) error {
	if r, ok := bh.reconcilerFor(gk); ok {
		r.enqueue(id)
		return nil
	}
	_, err := bh.gcCollect(ctx, id)
	return err
}

// waker is the dependency waker: it rides the store-wide write stream and requeues
// the dependents of every object that changes. One per control plane, and its own
// type so the state it accumulates while running — a resume cursor, and the test
// seams that pace its retries — has an owner. Every field is touched by the waker
// goroutine alone; nothing outside it has any business reading them, which is why
// they need no synchronisation.
type waker struct {
	bh *Beehive

	// watermark is the highest resource_version this waker has consumed. It is the
	// resume point for a recovery: the store-wide cursor is globally monotonic and
	// never reused, so "everything above this" names exactly what was missed. Owned
	// by the waker goroutine — seeded by serve, advanced by dependentsWake — so it
	// needs no synchronisation.
	watermark int64

	// seen is the highest version processed since the last safe point, which is not
	// the same thing as the watermark. The stream delivers in first-touch order, not
	// version order — a re-written object coalesces into its existing queue position
	// — so the newest version in a batch can sit far above changes still queued
	// behind it. Taking it as the resume point would step over them. seen only
	// becomes the watermark on a drained batch, where there is nothing left queued
	// for it to step over.
	seen int64

	// waitRetry replaces the retry delay when set. The recovery loop is the only thing
	// standing between a dropped subscription and a control plane that has silently
	// stopped honouring depends_on, so tests have to drive it — and they must do that
	// without waiting on a real interval, which would be the sleep-paced test the
	// conventions rule out.
	waitRetry func(ctx context.Context, d time.Duration) bool
}

// commitRule says how far a processed batch lets the watermark move. The three
// cases differ because the two delivery paths make different promises: the live
// stream is in first-touch order and may leave changes queued below a batch, while
// a replay page is version-ordered and complete up to its own last row.
type commitRule int

const (
	commitStaged  commitRule = iota // a full live batch: record the high, move nothing
	commitDrained                   // a short live batch: the receiver was empty
	commitOrdered                   // a replay page: complete up to its own high
)

const (
	// wakerRetryBase is the first resubscribe delay; wakerRetryCap bounds every
	// later one. The ceiling is on the interval and never on the number of attempts:
	// a waker that gave up is the dead waker this recovery exists to kill, reached by
	// a slower route. A store unhappy enough to fail a subscription will fail the
	// next one too, and a tight loop against its single connection makes the outage
	// worse.
	wakerRetryBase = 100 * time.Millisecond
	wakerRetryCap  = 30 * time.Second
)

// start launches the single waker over one store-wide change
// stream. Driving requeues off change-events (which the store suppresses for
// no-ops) rather than off every reconcile means a steady state stops waking and
// cycles settle.
//
// Store-wide rather than per registered kind: a depends_on edge may point at an
// object of any kind, including one used through Client with no Register. Such a
// target has no reconciler, so a per-kind subscription list — however it is
// computed — cannot name it, and changes to it would reach no waker at all.
// Routing stays correct because it was never keyed on the subscription:
// dependentsWake enqueues each dependent through enqueueIfRegistered, by the
// dependent's own kind.
//
// With no registered controllers there is nothing to wake, and the stream is not
// free: it would pay a edges query per change in the whole store only to reach
// enqueueIfRegistered's no-op arm.
func (dw *waker) start(runCtx context.Context) {
	if len(dw.bh.order) == 0 {
		return
	}
	// Subscribe here rather than on the goroutine, so the ordering promise Start
	// makes actually holds: registering the receiver is what starts buffering, and
	// it has to happen before any reconcile loop can publish. Draining it can wait —
	// the hub holds what arrives in the meantime, conflated per object.
	w, cursor, err := dw.bh.store.ObjectWritesSubscribe(runCtx)
	dw.bh.wg.Go(func() { dw.serve(runCtx, w, cursor, err) })
}

// serve keeps a subscription alive for the life of the control plane, replaying
// whatever each gap swallowed. It is the driver the watermark needs: a cursor says
// where to resume, but something has to still be running to decide to. Nothing was
// before — a failed subscribe returned, and a closed stream ended the loop — so
// both losses were permanent, repaired only by escalating a periodic pass that at
// the default configuration may not exist.
//
// It takes the first subscription from start rather than opening its own, so the
// stream is established before Start returns. Round 0 has missed nothing and simply
// takes its cursor; every later round has a gap below it and replays instead —
// including the one after a failed first attempt, since writes really could have
// been missed before any cursor was known.
func (dw *waker) serve(ctx context.Context, w *ObjectWritesSubscription, cursor int64, err error) {
	var attempt int
	// seeded says a cursor has been taken. Distinct from "watermark != 0", which a
	// legitimately-zero cursor cannot express.
	var seeded bool
	for round := 0; ctx.Err() == nil; round++ {
		if round > 0 {
			w, cursor, err = dw.bh.store.ObjectWritesSubscribe(ctx)
		}
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return // shutdown, not a loss
			}
			// Every change in the store is reaching no dependent while this holds, for
			// every kind — this is the process's only stream.
			dw.bh.log().WarnContext(ctx, "dependency waker subscription failed; retrying, and dependency wakes are not being delivered for any kind until it succeeds",
				"attempt", attempt+1, "err", err)
		default:
			switch {
			case !seeded && round == 0:
				dw.watermark, dw.seen = cursor, cursor
			case !seeded:
				// The first subscribe failed, so no cursor was ever known and a replay
				// would start at zero — every live object, paged, with an edges lookup
				// each, on the connection every writer shares. That is the whole-world
				// pass this design replaced, bought with one transient error, so take
				// this cursor instead and say plainly what it skips: changes made during
				// the outage are not replayed. The reconciler's startup pass covers them
				// unless it was turned off (see WithStartupResync).
				dw.bh.log().WarnContext(ctx, "dependency waker subscribed after an initial failure; changes made before it are not replayed, and the startup pass is what covers them",
					"cursor", cursor)
				dw.watermark, dw.seen = cursor, cursor
			default:
				// A new stream: nothing staged against the old one can be trusted against
				// this one, so the staging area restarts from the cursor itself.
				dw.seen = dw.watermark
				// Keep replaying until one gets through, exactly as the in-run path does.
				// Dropping a failed replay would leave the watermark below the gap while
				// the live stream carried it past — and those changes would never wake
				// their dependents, which is the loss this branch exists to prevent.
				for !dw.replay(ctx) {
					if !dw.backoff(ctx, attempt) {
						// Giving up on this subscription without handing it to run, which
						// is what would otherwise have closed it. Whoever abandons the
						// stream releases it.
						w.Close()
						return
					}
					attempt++
				}
			}
			seeded = true
			if dw.run(ctx, w) {
				// Reset only when the stream actually got work through. A stream that
				// delivers one batch and closes, over and over, is a run of outages, not
				// a fresh one each time — resetting on mere delivery would pin it at the
				// backoff floor.
				attempt = 0
			}
			if ctx.Err() != nil {
				return
			}
		}
		// Whichever way this round ended, pause before the next subscribe: a store
		// that fails them, or closes streams as fast as it hands them out, would
		// otherwise spin against the single connection every writer shares.
		if !dw.backoff(ctx, attempt) {
			return
		}
		attempt++
	}
}

// backoff waits before the next resubscribe attempt, reporting false if the
// control plane went away first. The delay doubles to wakerRetryCap and stays
// there; the caller keeps trying regardless of how many attempts that takes.
func (dw *waker) backoff(ctx context.Context, attempt int) bool {
	// The inner min is an overflow guard, not a second cap: attempt is unbounded
	// (the loop never gives up), and shifting past 63 wraps to a negative duration,
	// which a timer fires immediately — turning the backoff into the spin it exists
	// to prevent.
	d := min(wakerRetryBase<<min(attempt, 16), wakerRetryCap)
	if dw.waitRetry != nil {
		return dw.waitRetry(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// replayPageCap bounds one replay page. The query is a covering-index range scan,
// so a page is cheap to *run*; what costs is the number of round trips, since each
// page also drives an edges lookup and every one of those serializes against every
// writer on the store's single connection. Sized well above the live stream's batch
// cap for that reason — the point of the bound is to keep an unbounded read from
// materialising the whole table after a long outage, not to keep any one read short.
const replayPageCap = 256

// replay feeds everything above the watermark back through the wake path, a page
// at a time, and reports whether it got through. This is the repair: rather than
// re-deriving every object of every kind to find the dependents a lost change
// stranded, it re-reads the changes themselves, so the cost is what was missed
// rather than what exists.
//
// The cursor advances per page, which is sound here and nowhere else: pages come
// back in resource_version order, so a page that succeeded really does mean
// everything below it is done. A page that fails leaves the cursor where it was —
// the changes are still owed, and the caller retries from there.
func (dw *waker) replay(ctx context.Context) bool {
	for {
		page, err := dw.bh.store.ObjectWritesListSince(ctx, dw.watermark, replayPageCap)
		if err != nil {
			if ctx.Err() != nil {
				return false // shutdown cancelled this read; not a loss of its own
			}
			dw.bh.log().WarnContext(ctx, "replaying missed changes failed; dependency wakes for them are still owed",
				"watermark", dw.watermark, "err", err)
			return false
		}
		if len(page) == 0 {
			return true // caught up
		}
		// dependentsWake advances the watermark itself, which is what makes the next
		// page start where this one ended.
		if !dw.dependentsWake(ctx, page, commitOrdered) {
			return false
		}
		if len(page) < replayPageCap {
			// A short page means nothing was live above it when the store answered.
			// Anything committed since is already on the live stream — the subscription
			// is established before a replay runs — so stopping here saves the empty
			// query that would otherwise end every replay.
			return true
		}
	}
}

// run consumes one subscription until ctx is cancelled or the stream ends,
// reporting whether it got any work through — which is what tells serve a fresh
// outage from a run of them.
//
// It requeues dependents when a target changes, until ctx is
// cancelled or the stream ends. The stream is store-wide and established by
// Start (events-only, no snapshot: the reconciler's own startup pass already
// covers existing objects), and it arrives in batches — a burst of changes costs
// one edges query rather than one per change. The ctx.Done() arm is needed
// because a watcher's channel may never close on its own.
func (dw *waker) run(ctx context.Context, w *ObjectWritesSubscription) bool {
	defer w.Close()
	progressed := false
	for {
		select {
		case <-ctx.Done():
			return progressed
		case batch, ok := <-w.Changes():
			if !ok {
				// Stop closes the stream by cancelling this same ctx, so on shutdown
				// both select arms are ready at once and Go may pick this one. Re-check
				// before calling it a loss: escalating here would arm every later tick
				// of a control plane that is going away, on a stream that ended
				// normally.
				if ctx.Err() != nil {
					return progressed
				}
				// The stream ended without the control plane stopping (that arrives on
				// ctx.Done above, and is not a loss). Nothing re-subscribes, and this is
				// the process's only change stream, so until it is back no change of any
				// kind reaches a dependent.
				dw.bh.log().Warn("dependency waker change stream ended for every kind; resubscribing and replaying the changes it missed")
				return progressed
			}
			// A batch shorter than the cap means the drain ended on an empty receiver,
			// so everything published so far has reached us and the highest version
			// seen is a safe resume point. A full batch may have left changes queued
			// below it (first-touch order), so it stages instead.
			commit := commitStaged
			if len(batch) < writeBatchCap {
				commit = commitDrained
			}
			if dw.dependentsWake(ctx, batch, commit) {
				progressed = true
			} else {
				// The batch was dropped and the watermark still points below it, so the
				// changes are recoverable by re-reading them. Keep replaying until one
				// gets through, before consuming anything further: taking the next batch
				// first would interleave a success past a failure, and the cursor may only
				// move once everything below it is done. Stalling here is safe because the
				// hub conflates per object — what a paused consumer holds is bounded by
				// the store's live key set, not by how much churn it missed.
				//
				// Paced, because the lookup that failed is the one a replay makes again:
				// a store unhappy enough to fail it will fail the retry too, and retrying
				// at stream speed would add two queries per incoming batch to the single
				// connection that is already struggling.
				for attempt := 0; !dw.replay(ctx); attempt++ {
					if !dw.backoff(ctx, attempt) {
						return progressed
					}
				}
				progressed = true
			}
		}
	}
}

// dependentsWake requeues every object that depends_on one of targetIDs,
// each in its own kind's reconciler. Over-eager wakes are harmless: unregistered
// kinds are ignored and the work queue coalesces duplicates.
//
// It resolves the whole batch in one edges query, which is not merely an
// optimization: the store runs on a single connection, so every lookup the waker
// makes serializes against every writer in the process — and the waker sees every
// change in the store, not just the registered kinds'. One query per burst rather
// than one per change is what keeps a write-heavy kind from taxing them all.
//
// It wakes on any present-state change. Added matters as much as Modified: the
// conflating store hub coalesces a create-then-modify into a single Added, so
// skipping it would drop the modify's wake. A brand-new object usually has no
// dependents, so the extra lookup is a cheap no-op — the over-wake is harmless.
// Deleted carries nothing to requeue (a gone object has no dependents).
// Duplicates are collapsed by scan rather than by a map: the hub already
// coalesces per object so a repeat is rare, and the batch is bounded, but the
// wake policy must not depend on how its input was produced.
//
// It addresses dependents by bare id, both to skip the self-edge and to route
// through d.GroupKind(), because an ObjectID is unique across every kind
// (objects.id is one AUTOINCREMENT primary key for the whole table). Under a
// per-kind id scheme the self-edge compare would also need the GroupKind, or it
// would silently drop a foreign object's wake.
func (dw *waker) dependentsWake(ctx context.Context, batch []ObjectWrite, commit commitRule) bool {
	var high int64
	ids := make([]ObjectID, 0, len(batch))
	for _, ref := range batch {
		// Every reference that *arrives* counts toward the cursor, including the ones
		// with nothing to wake: a delete carries no dependents, but its version is
		// still a version this consumer has accounted for, and on a delete-heavy store
		// those are most of what arrives. A cursor that moved only on wakeable changes
		// would trail arbitrarily far behind and turn a bounded replay into a
		// whole-table scan. (A transient the consumer never observed does not arrive at
		// all — writeSignalMerge annihilates the slot — so its version is simply never
		// accounted for, and the next arrival's higher version covers it.)
		high = max(high, ref.ResourceVersion)
		if ref.Type != Added && ref.Type != Modified {
			continue
		}
		if !slices.Contains(ids, ref.ID) {
			ids = append(ids, ref.ID)
		}
	}
	// A batch with nothing to wake is still consumed, so the assignments below cover
	// both: the cursor moves exactly when this reports success.
	if len(ids) > 0 && !dw.dependentsEnqueue(ctx, ids) {
		return false
	}
	dw.seen = max(dw.seen, high)
	switch commit {
	case commitDrained:
		// The receiver was empty, so everything published so far has reached us and
		// been processed — including whatever earlier full batches staged in seen.
		dw.watermark = dw.seen
	case commitOrdered:
		// A replay page: version-ordered and complete, so everything up to this page's
		// own high is done — but nothing above it is, whatever seen may hold from an
		// earlier live batch. Jumping to seen here would step over the range between,
		// which is precisely the range seen exists to keep out of the cursor.
		dw.watermark = max(dw.watermark, high)
	}
	return true
}

// dependentsEnqueue resolves the dependents of targetIDs in one edges query and
// requeues each on its own kind's reconciler, reporting whether the lookup got
// through. A false answer means these changes are still owed: the caller holds the
// watermark so a replay can re-read them.
func (dw *waker) dependentsEnqueue(ctx context.Context, targetIDs []ObjectID) bool {
	byTarget, err := dw.bh.store.EdgesGroupIncomingByID(ctx, targetIDs, RelationDependsOn)
	if err != nil {
		// Shutdown cancels this same ctx, so a change already dequeued when Stop
		// lands fails here for no reason of its own — the same re-check the
		// stream-ended path makes, for the same reason.
		if ctx.Err() != nil {
			return false
		}
		// Every dependent of these targets just missed their changes, and a dependent
		// that has settled is invisible to every owed-work listing — its own
		// generation never moved. Nothing here can name who was missed: the lookup
		// that failed is exactly the one that would have said. Replaying the changes
		// themselves is what repairs it.
		dw.bh.log().WarnContext(ctx, "dependents lookup failed; replaying these changes from the watermark",
			"targetIDs", targetIDs, "err", err)
		return false
	}
	for _, targetID := range targetIDs {
		for _, d := range byTarget[targetID] {
			if d.ID == targetID {
				// Self-edge: nothing here is owed a wake. A spec write requeues through
				// wakeAfterCommit; a status or condition write is this object's own pass,
				// which just ran. Waking it re-enqueues at full speed with nothing to
				// converge it. Cycles of two or more still do; see the cycle entry in
				// TODO.md.
				continue
			}
			dw.bh.enqueueIfRegistered(d.GroupKind(), d.ID)
		}
	}
	return true
}

// stop tears the control plane down: it cancels the reconcile loops and waits
// for them to drain (bounded by ctx). It returns a non-nil error only when the
// loops did not drain before ctx was cancelled, so callers can detect a shutdown
// that hit its deadline. It is the closure returned by Start, and is a no-op
// (returning nil) if the control plane isn't running. Controllers own no
// teardown in beehive — the embedding application is responsible for its own
// background work, and may still write status through its ControllerClient until
// the store is closed.
func (bh *Beehive) stop(ctx context.Context) error {
	bh.mu.Lock()
	if bh.state != beehiveRunning {
		bh.mu.Unlock()
		return nil
	}
	// Transition and cancel under the lock, then release it before waiting on wg.
	// The dependency wakers (counted in wg) acquire bh.mu via enqueueIfRegistered;
	// holding it across wg.Wait would deadlock a waker mid-event against Stop when
	// ctx is unbounded. order is frozen after Start, so it's safe to read unlocked.
	bh.state = beehiveStopped
	bh.cancel()
	bh.log().Info("control plane stopping")
	bh.mu.Unlock()

	// Wait for reconcile loops to exit, but don't block past ctx. A drain that
	// loses the race to ctx is reported to the caller.
	done := make(chan struct{})
	go func() {
		bh.wg.Wait()
		close(done)
	}()
	var drainErr error
	select {
	case <-done:
	case <-ctx.Done():
		drainErr = ctx.Err()
	}

	// Watch subscriptions are owned by the store, not the control plane, so stop
	// does not terminate them: an active watcher ends when its context is
	// cancelled or the store is closed.
	bh.log().Info("control plane stopped")
	return drainErr
}

// New creates a control plane backed by store s. Register controllers on the
// returned Beehive before calling Start.
func New(s Store, opts ...Option) (*Beehive, error) {
	bh := &Beehive{
		store:           s,
		startupResync:   true,
		catchupInterval: defaultCatchupInterval,
		resyncInterval:  defaultResyncInterval,
		gcInterval:      defaultGCInterval,
		reconcilers:     make(map[GroupKind]*reconciler),
		migrators:       make(map[GroupKind]Migrator),
	}
	bh.waker = &waker{bh: bh}
	for _, o := range opts {
		if err := o(bh); err != nil {
			return nil, err
		}
	}
	return bh, nil
}

// Register installs controller c for the resource kind gk and returns the
// kind's ControllerClient — the status-write surface, which the embedding
// application can inject into the controller and use from its own goroutines.
// It must be called before Start, and only once per kind. On any error it
// returns (nil, err).
func Register[Spec, Status any](bh *Beehive, gk GroupKind, c Controller[Spec, Status], opts ...Option) (ControllerClient[Status], error) {
	bh.mu.Lock()
	defer bh.mu.Unlock()
	if bh.state != beehiveNew {
		return nil, fmt.Errorf("beehive: cannot register %s/%s after Start", gk.Group, gk.Kind)
	}
	if _, exists := bh.reconcilers[gk]; exists {
		return nil, fmt.Errorf("beehive: controller already registered for %s/%s", gk.Group, gk.Kind)
	}

	r := &reconciler{
		gk:               gk,
		store:            bh.store,
		work:             newWorkQueue(),
		scheduleHub:      conflate.New[ObjectID](scheduleMerge),
		catchupInterval:  bh.catchupInterval,
		resyncInterval:   bh.resyncInterval,
		maxRetryInterval: defaultMaxRetryInterval,
		concurrency:      bh.concurrency,
		startupResync:    bh.startupResync,
		backoffFor:       make(map[ObjectID]time.Duration),
		// Inherit the control plane's logging config as the default; the options
		// below may override it for this controller.
		logger:   bh.logger,
		logLevel: bh.logLevel,
	}
	// Feed the work queue's schedule changes into the hub (see schedulePublish).
	r.work.onSchedule = r.schedulePublish
	// Build the client once here so it's allocated per kind, not per reconcile,
	// and hand the same instance to both the adapter and the caller.
	client := &controllerClientImpl[Status]{bh: bh, gk: gk}
	adapter := &typedController[Spec, Status]{gk: gk, bh: bh, inner: c, client: client}
	r.adapter = adapter

	// Per-controller option overrides (e.g. WithResyncInterval, WithMaxRetryInterval).
	for _, o := range opts {
		if err := o(r); err != nil {
			return nil, err
		}
	}

	// Resolve once now that overrides are applied, and tag every record with the
	// kind so per-object logs need only add the id. The adapter shares the same
	// resolved logger for its reconcile-scoped messages.
	r.logger = resolveLogger(r.logger, r.logLevel).With("group", gk.Group, "kind", gk.Kind)
	adapter.logger = r.logger

	// A WithMigrator option sets r.migrator; promote it to the shared registry so
	// both decode paths (client and reconciler) resolve the same migrator via
	// migratorFor. Done under bh.mu, already held.
	if r.migrator != nil {
		bh.migrators[gk] = r.migrator
	}

	bh.reconcilers[gk] = r
	bh.order = append(bh.order, r)
	return client, nil
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

// migratorFor returns the migrator registered for gk, or nil if the kind opted
// out. Both decode paths (the user-facing client and the reconciler) call it so
// they share one migrator per kind.
func (bh *Beehive) migratorFor(gk GroupKind) Migrator {
	bh.mu.Lock()
	defer bh.mu.Unlock()
	return bh.migrators[gk]
}

// reconcilerFor returns the reconciler registered for gk, if one exists. The
// client's Requeue reaches the per-kind work queue through it, and SchedulesGet /
// SchedulesWatch read schedule state through it; a client-only kind (no Register)
// has none.
func (bh *Beehive) reconcilerFor(gk GroupKind) (*reconciler, bool) {
	bh.mu.Lock()
	defer bh.mu.Unlock()
	r, ok := bh.reconcilers[gk]
	return r, ok
}

// enqueueIfRegistered wakes the reconciler for (gk, id) if one exists.
// It is a no-op when gk has no registered controller (e.g. a client-only kind).
func (bh *Beehive) enqueueIfRegistered(gk GroupKind, id ObjectID) {
	if r, ok := bh.reconcilerFor(gk); ok {
		r.enqueue(id)
	}
}
