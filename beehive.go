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
	state  beehiveState
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// resyncKindsNextTick repairs a dependency-wake failure that dropped a single
// change: every registered reconciler runs one full pass on its next catchup
// tick, then returns to draining owed work.
//
// resyncKindsEveryTick is the sticky counterpart, for a failure that will keep
// dropping changes for the life of the process.
//
// Both reach every reconciler rather than the failing kind's, and that is forced
// by the failures themselves: dependency edges are deliberately cross-kind, so a
// lost wake strands dependents of any kind, and the lookup that fails in
// wakeDependentsBatch is the very thing that would have named them. Escalating one kind
// would repair one arbitrary kind and silently spend the repair for the rest.
// order is frozen once Start runs, so it reads without bh.mu — the same reasoning
// stop relies on.
func (bh *Beehive) resyncKindsNextTick() {
	for _, r := range bh.order {
		r.resyncNextTick()
	}
}

// resyncKindsEveryTick is the sticky counterpart of resyncKindsNextTick; see that
// method's doc for why both are cross-kind.
func (bh *Beehive) resyncKindsEveryTick() {
	for _, r := range bh.order {
		r.resyncEveryTick()
	}
}

// hasPeriodicPass reports whether any registered reconciler still has a tick for
// an escalation to ride. Asked across every kind rather than the failing one,
// because the escalation is cross-kind too (see resyncKindsNextTick): a tick on
// any kind repairs dependents of that kind, whatever kind's waker was lost.
func (bh *Beehive) hasPeriodicPass() bool {
	for _, r := range bh.order {
		if r.hasPeriodicPass() {
			return true
		}
	}
	return false
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

	// One dependency waker, on one store-wide change stream, requeues dependents
	// on each change. Driving it off change-events (which the store suppresses for
	// no-ops) rather than every reconcile means a steady state stops waking and
	// cycles settle.
	//
	// Store-wide rather than per registered kind: a depends_on edge may point at
	// an object of any kind, including one used through Client with no Register.
	// Such a target has no reconciler, so a per-kind subscription list — however
	// it is computed — cannot name it, and changes to it would reach no waker at
	// all. Routing stays correct because it was never keyed on the subscription:
	// wakeDependentsBatch enqueues each dependent through enqueueIfRegistered, by
	// the dependent's own kind.
	//
	// Subscribe and start consuming BEFORE launching any reconcile loop: a
	// controller's startup reconcile can modify a target the instant it runs, and
	// that Modified event must not be published before the waker is listening —
	// otherwise dependents go unwoken under configurations that rely on dependency
	// events (e.g. a settled dependent, which no owed-work listing can see, with
	// every ticker disabled). A subscribe failure is non-fatal: it escalates every
	// periodic pass to a full resync, which is the only thing that reaches a
	// settled dependent (see the branch below).
	//
	// With no registered controllers there is nothing to wake, and the stream
	// would pay a refs query per change in the store only to reach
	// enqueueIfRegistered's no-op arm.
	if len(bh.order) > 0 {
		w, err := bh.store.WatchChangeRefs(runCtx)
		if err != nil {
			// A waker that never starts is a dead waker: no change anywhere in the
			// store will wake a dependent for the life of the process — every kind's,
			// not one kind's, since this is the process's only stream. This used to
			// claim the resync covered it, which was never true — a settled dependent
			// is exactly what an owed-work tick cannot see — so escalate to make it
			// true, and report which situation the operator is actually in.
			bh.resyncKindsEveryTick()
			msg := "dependency waker subscription failed; no dependency wakes will be delivered for any kind, so escalating every periodic pass to a full resync to converge dependents"
			if !bh.hasPeriodicPass() {
				msg = "dependency waker subscription failed and there is no periodic pass to fall back on; no dependency wakes will be delivered for any kind — drive them with Client.Requeue"
			}
			bh.logger.Warn(msg, "err", err)
		} else {
			bh.wg.Go(func() {
				bh.runDependencyWaker(runCtx, w)
			})
		}
	}

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
		bh.runGCSweeper(runCtx)
	})

	bh.state = beehiveRunning
	bh.logger.Info("control plane started", "controllers", len(bh.order))
	return func(stopCtx context.Context) error { return bh.stop(stopCtx) }, nil
}

// runGCSweeper is the global garbage-collection backstop. The per-controller
// reconcile loop runs collect for its own kind; this sweeps every kind, so a
// deletion-pending object of a client-only kind (no registered controller) is
// still collected — otherwise it would strand and RESTRICT-block its owner's
// delete forever. It sweeps once at startup and then on the GC cadence.
//
// Every failure inside a sweep is logged and swallowed, which is only sound
// because there is always a next tick: WithGCInterval rejects a non-positive
// interval, so a transient error costs one cadence of latency rather than
// stranding a row for the life of the process.
func (bh *Beehive) runGCSweeper(ctx context.Context) {
	if bh.gcInterval <= 0 {
		// Unreachable through New — the default is positive and WithGCInterval rejects
		// anything else. Guarded anyway so a Beehive assembled field-by-field (tests
		// that want the sweeper's own enqueues out of the way) simply has no sweeper
		// instead of panicking in NewTicker.
		return
	}
	bh.sweepDeletionPending(ctx)
	bh.sweepEventRetention(ctx)
	ticker := time.NewTicker(bh.gcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bh.sweepDeletionPending(ctx)
			bh.sweepEventRetention(ctx)
		}
	}
}

// sweepEventRetention trims the event log to the configured retention (see
// WithEventRetention). It is a no-op unless a bound is set, and best-effort: a
// failed sweep is retried on the next cadence tick.
func (bh *Beehive) sweepEventRetention(ctx context.Context) {
	if bh.eventRetentionPerObject <= 0 && bh.eventRetentionMaxAge <= 0 {
		return
	}
	if _, err := bh.store.SweepEvents(ctx, bh.eventRetentionPerObject, bh.eventRetentionMaxAge); err != nil {
		bh.log().Warn("event retention sweep failed; retry next sweep", "err", err)
	}
}

// sweepDeletionPending drives every deletion-pending object one step closer to
// removal (see advanceDeletion for the routing).
func (bh *Beehive) sweepDeletionPending(ctx context.Context) {
	rows, err := bh.store.ListAllDeletionPending(ctx)
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
		if err := bh.advanceDeletion(ctx, row.GroupKind(), row.ID); err != nil && !errors.Is(err, ErrNotFound) {
			bh.logger.Warn("gc sweep: collecting object failed; retry next sweep",
				"group", row.Group, "kind", row.Kind, "id", row.ID, "err", err)
		}
	}
}

// advanceDeletion drives one deletion-pending object a step closer to removal,
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
func (bh *Beehive) advanceDeletion(ctx context.Context, gk GroupKind, id ObjectID) error {
	if r, ok := bh.reconcilerFor(gk); ok {
		r.enqueue(id)
		return nil
	}
	_, err := bh.collect(ctx, id)
	return err
}

// runDependencyWaker requeues dependents when a target changes, until ctx is
// cancelled or the stream ends. The stream is store-wide and established by
// Start (events-only, no snapshot: the reconciler's own startup pass already
// covers existing objects), and it arrives in batches — a burst of changes costs
// one refs query rather than one per change. The ctx.Done() arm is needed
// because a watcher's channel may never close on its own.
func (bh *Beehive) runDependencyWaker(ctx context.Context, w ChangeRefWatcher) {
	defer w.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case batch, ok := <-w.Batches():
			if !ok {
				// Stop closes the stream by cancelling this same ctx, so on shutdown
				// both select arms are ready at once and Go may pick this one. Re-check
				// before calling it a loss: escalating here would arm every later tick
				// of a control plane that is going away, on a stream that ended
				// normally.
				if ctx.Err() != nil {
					return
				}
				// The stream ended without the control plane stopping (that arrives on
				// ctx.Done above, and is not a loss). Nothing re-subscribes, and this is
				// the process's only change stream, so every future change to every kind
				// now reaches no dependent at all.
				bh.log().Warn("dependency waker stopped: its change stream ended, so dependency wakes are dead for every kind for the life of the process; escalating every catchup tick to a full resync pass")
				bh.resyncKindsEveryTick()
				return
			}
			// Wake on any present-state change. We must handle Added, not just
			// Modified: the conflating store hub coalesces a create-then-modify into
			// a single Added, so skipping it would drop the modify's wake. A
			// brand-new object usually has no dependents, so the extra lookup is a
			// cheap no-op — the over-wake is harmless. Deleted carries nothing to
			// requeue (a gone object has no dependents).
			targets := make([]ObjectID, 0, len(batch))
			for _, ref := range batch {
				if ref.Type == Added || ref.Type == Modified {
					targets = append(targets, ref.ID)
				}
			}
			bh.wakeDependentsBatch(ctx, targets)
		}
	}
}

// wakeDependentsBatch requeues every object that depends_on one of targetIDs,
// each in its own kind's reconciler. Over-eager wakes are harmless: unregistered
// kinds are ignored and the work queue coalesces duplicates.
//
// It resolves the whole batch in one refs query, which is not merely an
// optimization: the store runs on a single connection, so every lookup the waker
// makes serializes against every writer in the process — and the waker now sees
// every change in the store, not just the registered kinds'. One query per burst
// rather than one per change is what keeps a write-heavy kind from taxing them
// all. Duplicate ids are collapsed first; the store's conflating hub already
// coalesces per object, but the wake policy must not depend on how its input was
// produced.
//
// It addresses dependents by bare id, both to skip the self-edge and to route
// through d.GroupKind(), because an ObjectID is unique across every kind
// (objects.id is one AUTOINCREMENT primary key for the whole table). Under a
// per-kind id scheme the self-edge compare would also need the GroupKind, or it
// would silently drop a foreign object's wake.
func (bh *Beehive) wakeDependentsBatch(ctx context.Context, targetIDs []ObjectID) {
	ids := make([]ObjectID, 0, len(targetIDs))
	seen := make(map[ObjectID]struct{}, len(targetIDs))
	for _, id := range targetIDs {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return
	}
	byTarget, err := bh.store.GroupIncomingRefsByID(ctx, ids, RelationDependsOn)
	if err != nil {
		// Shutdown cancels this same ctx, so a change already dequeued when Stop
		// lands fails here for no reason of its own. Escalating would arm a full
		// pass on every reconciler of a control plane that is going away — the same
		// re-check the stream-ended path above makes, for the same reason.
		if ctx.Err() != nil {
			return
		}
		// Every dependent of these targets just missed their changes. A dependent
		// that has settled is invisible to every owed-work listing — its own
		// generation never moved — so with no full pass configured the miss is
		// permanent, not slow. Nothing here can name who was missed: the lookup
		// that failed is exactly the one that would have said.
		bh.log().WarnContext(ctx, "dependents lookup failed; wakes for these changes were dropped, forcing a full resync pass",
			"targetIDs", ids, "err", err)
		bh.resyncKindsNextTick()
		return
	}
	for _, targetID := range ids {
		for _, d := range byTarget[targetID] {
			if d.ID == targetID {
				// Self-edge: nothing here is owed a wake. A spec write requeues through
				// wakeAfterCommit; a status or condition write is this object's own pass,
				// which just ran. Waking it re-enqueues at full speed with nothing to
				// converge it. Cycles of two or more still do; see the cycle entry in
				// TODO.md.
				continue
			}
			bh.enqueueIfRegistered(d.GroupKind(), d.ID)
		}
	}
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
		scheduleHub:      conflate.New[ObjectID](mergeSchedule),
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
	// Feed the work queue's schedule changes into the hub (see publishSchedule).
	r.work.onSchedule = r.publishSchedule
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
// client's Requeue reaches the per-kind work queue through it, and GetSchedule /
// WatchSchedule read schedule state through it; a client-only kind (no Register)
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
