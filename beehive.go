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

	"github.com/amorey/beehive/internal/driver"
	"github.com/amorey/beehive/internal/logging"
)

const (
	defaultOwedPassInterval = 30 * time.Second
	// Both full passes scale with the object count, so both are opt-in. No
	// reconcile may depend on the periodic one; a kind whose reconcile holds
	// in-process state may depend on the startup one. See
	// docs/adr/2026-08-07-the-startup-pass-may-be-depended-on.md.
	defaultFullPassInterval time.Duration = 0
	defaultStartupFullPass                = false
	defaultGCInterval                     = 30 * time.Second
	// The default resume window: how long a subscriber may be disconnected and
	// still resume without a full resync. A day covers a restart, a deploy, and a
	// night of maintenance.
	defaultWriteLogMaxAge = 24 * time.Hour
	// Free pages the GC sweeper releases per tick (~4MB/30s at ~3.7µs a page).
	// Not an option: there is no measurement a caller could tune it against.
	freePagesPerSweep = 1000
	// Timelines one event-retention sweep may trim. The store applies the same
	// number when a sweep names none; both are sized against gcBudgetInterval.
	eventCapPerSweep = 256
	// The cadence both budgets above were sized against. A longer WithGCInterval
	// scales them up rather than slowing the work down — for the event cap that
	// is the difference between a bounded log and one that sits over its cap. See
	// docs/adr/2026-08-06-driver-cadences-are-configurable.md.
	gcBudgetInterval = 30 * time.Second
	// defaultWakeScanMinInterval floors the gap between two wake-driven scans.
	// It is what a chain hop costs, and what bounds the loop's duty cycle under
	// a sustained write stream. See
	// docs/adr/2026-08-05-a-commit-wakes-the-dependency-waker.md.
	defaultWakeScanMinInterval = 100 * time.Millisecond
	// defaultWakePersistInterval floors the waker's cursor write, which lands on
	// the connection every commit needs. It is also where the retry ladder for a
	// failing one starts.
	defaultWakePersistInterval = 1 * time.Second
	// defaultMinRequeueInterval floors the gap between two dispatches of one
	// object. It is the whole of what bounds a dependency cycle now that the
	// waker has no cadence of its own; lowering it changes that bound.
	defaultMinRequeueInterval = 1 * time.Second
	// The stale-dependents pass is the waker's backstop; its cadence is set by
	// acceptable staleness after a crash, not by cost.
	defaultStaleDependentsInterval = 60 * time.Second
	// A watch reads on a commit wake, so this floor is not the latency of a
	// local write — it bounds staleness for what a wake cannot cover.
	defaultWatchFloorInterval = 30 * time.Second
	// Floors the gap between two wake-driven drains, so a write stream cannot
	// make its kind's tailer hold the single connection back from the writers.
	defaultWatchScanMinInterval = 100 * time.Millisecond
	// The first retry after a failed tail step; it doubles up to watchRetryMax.
	watchRetryBase = 100 * time.Millisecond
	// watchRetryMax caps that ladder. Its own constant rather than the floor: the
	// floor is what a healthy quiet kind costs, which is the right ceiling only
	// while it stays seconds — an embedder that lengthens it to spare an idle
	// laptop would otherwise be lengthening error recovery with it. See
	// docs/adr/2026-08-06-driver-cadences-are-configurable.md.
	watchRetryMax = 30 * time.Second
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
	// Driver cadences. Owed work is bounded by what is outstanding, a full pass
	// by the object count, GC by deletion-pending rows, the wake scan by what
	// changed. gcInterval and staleDependentsInterval are always positive when
	// the Beehive came from New; wakerOff turns the waker off.
	wakerOff                bool
	owedPassInterval        time.Duration
	minRequeueInterval      time.Duration
	fullPassInterval        time.Duration
	gcInterval              time.Duration
	wakeScanMinInterval     time.Duration
	wakePersistInterval     time.Duration
	staleDependentsInterval time.Duration
	watchFloorInterval      time.Duration
	watchScanMinInterval    time.Duration
	concurrency             int // default worker count for all controllers; 0/1 = single-threaded
	// Event-log retention, applied globally by the GC sweeper. Zero on both
	// disables the sweep.
	eventRetentionPerTimeline int
	eventRetentionMaxAge      time.Duration
	// Write-log retention, applied globally by the GC sweeper. Bounded by
	// default, unlike the event log: an entry lands on every object write, so
	// the log grows at reconcile rate whether or not the user opts in.
	writeLogRetentionPerKind int
	writeLogRetentionMaxAge  time.Duration
	// startupFullPass is the default copied into each reconciler; off unless
	// asked for (see WithStartupFullPass).
	startupFullPass bool
	// logger/logLevel stay raw until Start resolves them (nil logger = disabled);
	// each reconciler inherits them as its default.
	logger   *slog.Logger
	logLevel slog.Leveler

	mu          sync.Mutex
	reconcilers map[GroupKind]*reconciler
	// migrators is the single per-kind registry both decode paths (client and
	// reconciler) resolve through.
	migrators map[GroupKind]Migrator
	// order preserves registration order so Start launches loops deterministically.
	order []*reconciler

	waker *waker

	// eventWriteHub is a message hub for object-scoped event writes.
	eventWriteHub eventWriteHub
	// kindWriteHub is a message hub for GroupKind-scoped writes
	kindWriteHub kindWriteHub

	// tailers is one shared reader per watched kind, started on the kind's first
	// watch and ended by its last. A tailer is here exactly while it has
	// subscribers. Guarded by tailMu — never bh.mu; see tailerFor.
	tailMu  sync.Mutex
	tailers map[GroupKind]*objectTailer

	state  beehiveState
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// log returns a non-nil logger; Stop and tests can run before Start resolves it.
func (bh *Beehive) log() *slog.Logger {
	if bh.logger == nil {
		return logging.Discard
	}
	return bh.logger
}

// Start brings the control plane up: the dependency waker, the reconcile loops,
// the stale-dependents pass, and the GC sweeper — all periodic. It returns a
// stop function that tears everything down. A Beehive is one-shot: starting
// twice, or after stop, is an error.
//
// startCtx covers startup only; the long-lived loops end when the returned stop
// is called. Startup reads the store to seed the dependency waker, so a startCtx
// that expires while the store is busy fails the start — a store *error* there
// does not, since the waker is an optimisation.
func (bh *Beehive) Start(startCtx context.Context) (func(context.Context) error, error) {
	bh.mu.Lock()
	defer bh.mu.Unlock()
	switch bh.state {
	case beehiveStopped:
		return nil, fmt.Errorf("beehive: already stopped; create a new Beehive to restart")
	case beehiveRunning:
		return nil, fmt.Errorf("beehive: already started")
	}

	bh.logger = logging.Resolve(bh.logger, bh.logLevel)

	// runCtx lives for the lifetime of the control plane; Stop cancels it.
	runCtx, cancel := context.WithCancel(context.Background())
	bh.cancel = cancel

	abort := func(err error) error {
		bh.waker.teardown() // a no-op before prime
		cancel()
		return fmt.Errorf("beehive: start aborted: %w", err)
	}
	if err := startCtx.Err(); err != nil {
		return nil, abort(err)
	}

	// Before the loops launch, and before any caller holds the stop func: the
	// watermark then precedes every write a caller could make, and the
	// subscription every commit that could wake it. A failed seed is not a
	// failed start.
	bh.waker.prime(startCtx)
	if err := startCtx.Err(); err != nil {
		return nil, abort(err)
	}

	// None of the goroutines below need ordering against each other: the waker
	// took its ordering above, and the stale-dependents pass reads current
	// state, so nothing depends on when they happen to start.
	bh.wg.Go(func() {
		bh.waker.run(runCtx)
	})

	for _, r := range bh.order {
		bh.wg.Go(func() {
			r.run(runCtx)
		})
	}

	bh.wg.Go(func() {
		bh.staleDependentsRun(runCtx)
	})

	// The global GC sweeper reaches client-only kinds, which no per-controller
	// backstop covers.
	bh.wg.Go(func() {
		bh.gcSweeperRun(runCtx)
	})

	bh.state = beehiveRunning
	bh.logger.Info("control plane started", "controllers", len(bh.order))
	return func(stopCtx context.Context) error { return bh.stop(stopCtx) }, nil
}

// gcSweeperRun is the global garbage-collection loop: once at startup, then on the GC
// interval. Failures inside a sweep are logged and swallowed — WithGCInterval
// rejects a non-positive interval, so there is always a next tick.
func (bh *Beehive) gcSweeperRun(ctx context.Context) {
	driver.Run(ctx, bh.gcInterval, func(ctx context.Context) bool {
		bh.deletionPendingSweep(ctx)
		bh.eventRetentionSweep(ctx)
		bh.writeLogRetentionSweep(ctx)
		bh.reconcileOwedSweep(ctx)
		bh.freePagesSweep(ctx)
		return true
	})
}

// gcBudget scales a per-sweep work budget sized against gcBudgetInterval to the
// configured cadence, so what a sweeper does per unit time holds however long
// the interval is. Never below the unscaled budget: a shorter interval already
// buys the rate back by sweeping more often.
func (bh *Beehive) gcBudget(perSweep int) int {
	if bh.gcInterval <= gcBudgetInterval {
		return perSweep
	}
	return perSweep * int(bh.gcInterval/gcBudgetInterval)
}

// eventRetentionSweep trims the event log to the configured retention. No-op unless a
// bound is set; a failed sweep is retried on the next tick.
func (bh *Beehive) eventRetentionSweep(ctx context.Context) {
	if bh.eventRetentionPerTimeline <= 0 && bh.eventRetentionMaxAge <= 0 {
		return
	}
	if _, err := bh.store.EventsSweep(ctx, bh.eventRetentionPerTimeline, bh.eventRetentionMaxAge, bh.gcBudget(eventCapPerSweep)); err != nil {
		bh.log().Warn("event retention sweep failed; retry next sweep", "err", err)
	}
}

// writeLogRetentionSweep trims the object write log to the configured retention.
// A failed sweep is retried on the next tick; the horizon it did raise stands.
func (bh *Beehive) writeLogRetentionSweep(ctx context.Context) {
	if bh.writeLogRetentionPerKind <= 0 && bh.writeLogRetentionMaxAge <= 0 {
		return
	}
	if _, err := bh.store.ObjectWritesSweep(ctx, bh.writeLogRetentionPerKind, bh.writeLogRetentionMaxAge); err != nil {
		bh.log().Warn("write log retention sweep failed; retry next sweep", "err", err)
	}
}

// reconcileOwedSweep zeroes the owed count on rows whose kind has no reconcile
// loop, which nothing else drains.
// See docs/adr/2026-08-05-reclaim-a-client-only-owed-count.md.
func (bh *Beehive) reconcileOwedSweep(ctx context.Context) {
	cleared, err := bh.store.ReconcileOwedSweep(ctx, bh.registeredKinds())
	if err != nil {
		bh.log().Warn("reconcile-owed reclaim failed; retry next sweep", "err", err)
		return
	}
	if cleared > 0 {
		bh.log().Debug("reclaimed owed counts", "rows", cleared)
	}
}

// freePagesSweep hands space freed by the sweeps above back to the OS.
// Best-effort: nothing is incorrect while the space is unreclaimed, and a store
// that reclaims nothing reports 0.
func (bh *Beehive) freePagesSweep(ctx context.Context) {
	released, err := bh.store.ReclaimSpace(ctx, bh.gcBudget(freePagesPerSweep))
	if err != nil {
		bh.log().Warn("free-page release failed; retry next sweep", "err", err)
		return
	}
	if released > 0 {
		bh.log().Debug("released free pages", "pages", released)
	}
}

// deletionPendingSweep drives every deletion-pending object one step closer to
// removal.
func (bh *Beehive) deletionPendingSweep(ctx context.Context) {
	rows, err := bh.store.DeletionRequestsList(ctx)
	if err != nil {
		bh.log().Warn("gc sweep: listing deletion-pending objects failed; retry next sweep", "err", err)
		return
	}
	for _, row := range rows {
		// Logged, not just retried: for a client-only kind this sweep is the only
		// collector, so a row that fails every time would strand silently.
		// ErrNotFound is the benign already-collected race.
		if err := bh.deletionAdvance(ctx, row.GroupKind(), row.ID); err != nil && !errors.Is(err, ErrNotFound) {
			bh.log().Warn("gc sweep: collecting object failed; retry next sweep",
				"group", row.Group, "kind", row.Kind, "id", row.ID, "err", err)
		}
	}
}

// deletionAdvance routes one deletion-pending object: a registered kind is
// queued so its controller can clear finalizers (gcCollect can't — calling it
// directly would make no progress, forever), a client-only kind is collected
// here. Both arms are safe to repeat.
//
// Never from a commit hook: the client-only arm runs gcCollect inline, which
// would put a whole subtree's collect on the committer's goroutine. A push
// resolves the reconciler itself and leaves a client-only kind to the sweeper.
func (bh *Beehive) deletionAdvance(ctx context.Context, gk GroupKind, id ObjectID) error {
	if r, ok := bh.reconcilerFor(gk); ok {
		r.enqueue(id)
		return nil
	}
	_, err := bh.gcCollect(ctx, id)
	return err
}

// stop cancels the reconcile loops and waits for them to drain, bounded by ctx.
// It returns non-nil only when the drain hit ctx's deadline. No-op for the
// reconcile loops if not running.
func (bh *Beehive) stop(ctx context.Context) error {
	bh.mu.Lock()
	// beehiveStopped means another call owns the teardown. Returning without
	// closing the wake hub is the point: that call may still be draining, and
	// closing here would end every watch before it sees what the draining loops
	// write. A never-started Beehive falls through — it has no loops to drain,
	// but it can have tailers, and this is what ends them.
	if bh.state == beehiveStopped {
		bh.mu.Unlock()
		return nil
	}
	running := bh.state == beehiveRunning
	bh.state = beehiveStopped
	if running {
		// Release bh.mu before waiting on wg: the waker (counted in wg) takes
		// bh.mu to resolve reconcilers, so holding it across wg.Wait would
		// deadlock.
		bh.cancel()
		bh.log().Info("control plane stopping")
	}
	bh.mu.Unlock()

	var drainErr error
	if running {
		done := make(chan struct{})
		go func() {
			bh.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			drainErr = ctx.Err()
		}
	}

	// After the drain, so a stream whose caller is still reading sees what the
	// draining reconcile loops wrote. That ordering is what the drain buys, and
	// a drain that hit ctx's deadline does not buy it — the loops are still
	// running, and the tailers are torn down anyway rather than leaked.
	//
	// This races a client write's AfterCommit wake, which watch.Sender.Close
	// allows: the racing send either publishes or answers ErrClosed, never both
	// and never partially. Which one wins is unspecified and nothing here needs
	// it pinned, since every stream is ending.
	bh.kindWriteHub.Close()
	bh.eventWriteHub.Close()

	// The watch tailers are not counted in wg: each ends with its own last
	// subscriber, or with the hub close above. SchedulesWatch streams
	// report the work queue instead and end here, after their final value.
	bh.log().Info("control plane stopped")
	return drainErr
}

// New creates a control plane backed by store s. Register controllers on the
// returned Beehive before calling Start.
func New(s Store, opts ...Option) (*Beehive, error) {
	bh := &Beehive{
		store:                   s,
		startupFullPass:         defaultStartupFullPass,
		owedPassInterval:        defaultOwedPassInterval,
		fullPassInterval:        defaultFullPassInterval,
		gcInterval:              defaultGCInterval,
		writeLogRetentionMaxAge: defaultWriteLogMaxAge,
		wakeScanMinInterval:     defaultWakeScanMinInterval,
		wakePersistInterval:     defaultWakePersistInterval,
		minRequeueInterval:      defaultMinRequeueInterval,
		watchFloorInterval:      defaultWatchFloorInterval,
		watchScanMinInterval:    defaultWatchScanMinInterval,
		staleDependentsInterval: defaultStaleDependentsInterval,
		reconcilers:             make(map[GroupKind]*reconciler),
		migrators:               make(map[GroupKind]Migrator),
		eventWriteHub:           newEventWriteHub(),
		kindWriteHub:            newKindWriteHub(),
	}
	for _, o := range opts {
		if err := o(bh); err != nil {
			return nil, err
		}
	}
	// After the options: the waker reads its cadences at construction, so
	// building it above would capture the defaults and ignore them.
	bh.waker = newWaker(bh)
	return bh, nil
}

// Register installs controller c for the resource kind gk and returns the
// kind's ControllerClient. It must be called before Start, and only once per
// kind.
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
		owedPassInterval: bh.owedPassInterval,
		fullPassInterval: bh.fullPassInterval,
		maxRetryInterval: defaultMaxRetryInterval,
		concurrency:      bh.concurrency,
		startupFullPass:  bh.startupFullPass,
		backoffFor:       make(map[ObjectID]time.Duration),
		logger:           bh.logger,
		logLevel:         bh.logLevel,
	}
	r.work.setFloor(bh.minRequeueInterval) // withMinRequeueInterval may override below

	// One client per kind, shared by the adapter and the caller.
	client := &controllerClientImpl[Status]{bh: bh, gk: gk}
	adapter := &typedController[Spec, Status]{gk: gk, bh: bh, inner: c, client: client}
	r.adapter = adapter

	for _, o := range opts {
		if err := o(r); err != nil {
			return nil, err
		}
	}

	// Resolve once with overrides applied; tag every record with the kind.
	r.logger = logging.Resolve(r.logger, r.logLevel).With("group", gk.Group, "kind", gk.Kind)
	adapter.logger = r.logger

	// Promote a WithMigrator option to the shared registry so both decode paths
	// resolve the same migrator.
	if r.migrator != nil {
		bh.migrators[gk] = r.migrator
	}

	bh.reconcilers[gk] = r
	bh.order = append(bh.order, r)
	return client, nil
}

// isRegistered reports whether a controller is registered for gk.
func (bh *Beehive) isRegistered(gk GroupKind) bool {
	bh.mu.Lock()
	defer bh.mu.Unlock()
	_, ok := bh.reconcilers[gk]
	return ok
}

// registeredKinds returns the kinds with a reconcile loop, in registration order.
func (bh *Beehive) registeredKinds() []GroupKind {
	bh.mu.Lock()
	defer bh.mu.Unlock()
	kinds := make([]GroupKind, 0, len(bh.order))
	for _, r := range bh.order {
		kinds = append(kinds, r.gk)
	}
	return kinds
}

// migratorFor returns the migrator registered for gk, or nil.
func (bh *Beehive) migratorFor(gk GroupKind) Migrator {
	bh.mu.Lock()
	defer bh.mu.Unlock()
	return bh.migrators[gk]
}

// reconcilerFor returns the reconciler registered for gk, if one exists.
func (bh *Beehive) reconcilerFor(gk GroupKind) (*reconciler, bool) {
	bh.mu.Lock()
	defer bh.mu.Unlock()
	r, ok := bh.reconcilers[gk]
	return r, ok
}

// signalRequeueNow enqueues ref's reconcile at commit, cancelling any pending
// alarm. For a write that carries new information, which must not wait out a
// backoff or a re-enqueue floor. AfterCommit means a rollback (or savepoint
// unwind) discards the enqueue, and outside a transaction it runs inline.
// Callers gate on what the write actually changed (see signalSpecWritten and
// DependenciesAdd).
func (bh *Beehive) signalRequeueNow(ctx context.Context, ref ObjectRef) {
	bh.store.AfterCommit(ctx, func(context.Context) {
		if r, ok := bh.reconcilerFor(ref.GroupKind()); ok {
			r.requeueNow(ref.ID)
		}
	})
}

// signalRequeueThrottled enqueues ref's reconcile at commit through the
// ordinary wake path, so a pending alarm absorbs it. For a write a controller
// may repeat on every pass. Same commit semantics as signalRequeueNow.
func (bh *Beehive) signalRequeueThrottled(ctx context.Context, ref ObjectRef) {
	bh.store.AfterCommit(ctx, func(context.Context) {
		if r, ok := bh.reconcilerFor(ref.GroupKind()); ok {
			r.enqueue(ref.ID)
		}
	})
}

// signalRequeueManyNow is signalRequeueNow for a batch, in one hook: a per-ref
// hook would take bh.mu for each, where resolverForPage resolves each kind once.
// Empty refs queue nothing.
func (bh *Beehive) signalRequeueManyNow(ctx context.Context, refs []ObjectRef) {
	if len(refs) == 0 {
		return
	}
	bh.store.AfterCommit(ctx, func(context.Context) {
		resolve := bh.resolverForPage()
		for _, ref := range refs {
			if r := resolve(ref.GroupKind()); r != nil {
				r.requeueNow(ref.ID)
			}
		}
	})
}

// signalKindWritten wakes gk's tailer and the dependency waker once a write to
// gk commits. The signal is the kind, never the object: each holds its own
// cursor and reads the log to learn what moved, so it carries no id and a burst
// of writes to one kind collapses into one. AfterCommit for the same reasons as
// signalRequeue: a rollback publishes nothing, and the wake cannot arrive before
// the row is readable. Callers check that the write changed something only where
// the store already reports it — an extra wake costs one position read; a missed
// one costs a watch up to the floor tick, and a dependent up to the
// stale-dependents pass, which is the waker's only backstop now that it has no
// tick.
func (bh *Beehive) signalKindWritten(ctx context.Context, gk GroupKind) {
	bh.store.AfterCommit(ctx, func(context.Context) {
		_ = bh.kindWriteHub.Send(gk) // ErrClosed after stop; nothing is left to wake
	})
}

// signalEventsWritten wakes id's event readers once an event write to it
// commits. Same commit semantics as signalKindWritten, and the same reason it
// carries no value: a reader reads its position from the store.
func (bh *Beehive) signalEventsWritten(ctx context.Context, id ObjectID) {
	bh.store.AfterCommit(ctx, func(context.Context) {
		_ = bh.eventWriteHub.Send(id) // ErrClosed after stop; nothing is left to wake
	})
}

// resolverForPage returns a reconciler lookup that resolves each kind once and
// caches it, for callers queueing many ids across a few kinds — resolving per
// id would take bh.mu every time. nil for a client-only kind. The registration
// set is frozen after Start, so the cache cannot go stale.
func (bh *Beehive) resolverForPage() func(GroupKind) *reconciler {
	resolved := map[GroupKind]*reconciler{}
	return func(gk GroupKind) *reconciler {
		r, ok := resolved[gk]
		if !ok {
			r, _ = bh.reconcilerFor(gk)
			resolved[gk] = r
		}
		return r
	}
}

// enqueuerForPage is resolverForPage on the ordinary wake path, so a pending
// alarm absorbs the enqueue. For a driver's page, which repeats every pass.
func (bh *Beehive) enqueuerForPage() func(GroupKind, ObjectID) {
	resolve := bh.resolverForPage()
	return func(gk GroupKind, id ObjectID) {
		if r := resolve(gk); r != nil {
			r.enqueue(id)
		}
	}
}
