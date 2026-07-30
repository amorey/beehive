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
)

const (
	defaultOwedPassInterval = 30 * time.Second
	// Both full passes scale with the object count rather than with what is owed, so
	// both are opt-in, and nothing may depend on either: convergence is carried by the
	// owed pass, which drains what the store *records* as owed. A deployment that wants
	// periodic or at-startup re-confirmation of process-scoped state asks for it. The
	// two are declared together because they are one choice made at two cadences —
	// keep their defaults in step. See WithStartupFullPass for the argument.
	defaultFullPassInterval time.Duration = 0
	defaultStartupFullPass                = false
	defaultGCInterval                     = 30 * time.Second
	// How many free pages the GC sweeper releases per tick, for a store that
	// implements FreePagesReleaser. Draining costs about 3.7µs a page, so 1000 is
	// ~3.7ms of held write lock once per GC interval — negligible beside the sweep
	// it runs after — and reclaims ~4MB per 30s tick, which is the rate that
	// matters: a cap of 100 gives back 400KB a tick and would take the better part
	// of a day on a gigabyte of freed space, slower than a churning store can
	// produce it. Not an option, because there is no measurement a caller could
	// tune it against that the sweeper does not already have.
	freePagesPerSweep = 1000
	// The dependency-wake scan is the cheapest of the drivers — one indexed range
	// query that returns nothing in a quiet system — so it runs an order of
	// magnitude more often than the passes that scale with the object count.
	defaultWakeInterval = 1 * time.Second
	// The stale-dependents pass is the waker's backstop, so its cadence is set by
	// acceptable staleness after a crash rather than by cost: the scan is reads-only,
	// bounded by the depends_on edges of registered kinds, and in a steady state finds
	// nothing and enqueues nothing. Five minutes of silent divergence is a long time
	// for a control plane whose ordinary wake latency is one second; 60s keeps the
	// backstop in the same order as the owed pass and the GC sweeper while staying
	// above them.
	defaultStaleDependentsInterval = 60 * time.Second
	// The client's watch surface polls, so this is the latency a subscriber sees.
	defaultWatchPollInterval = 1 * time.Second
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
	// owedPassInterval paces the cheap tick that drains work the store records as
	// owed. It is separate from fullPassInterval because the two scale differently: owed
	// work is bounded by what is outstanding, a full pass by the object count.
	owedPassInterval time.Duration
	fullPassInterval time.Duration
	// gcInterval paces the global GC sweeper: collecting deletion-pending rows and
	// trimming the event log. It is separate from the reconcile intervals because
	// collecting dead rows and re-confirming live ones are different jobs with
	// different costs, and one number for both would mean tuning either moves the
	// other. It is always positive when the Beehive came from New, since
	// WithGCInterval rejects a non-positive value, so every error path in the sweeper
	// has a next tick to retry on.
	gcInterval time.Duration
	// wakeInterval paces the dependency waker's scan of the write log (see waker). It
	// is separate from the reconcile intervals because it scales with what *changed*
	// rather than with what exists or what is owed, which is what lets it run often
	// and cost nothing in a quiet system. Non-positive turns it off.
	wakeInterval time.Duration
	// staleDependentsInterval paces the stale-dependents pass, which re-derives owed
	// dependency reconciles from the durable watermarks rather than from anything the
	// waker recorded (see staleDependentsRun). It is always positive when the Beehive
	// came from New, since withStaleDependentsInterval rejects a non-positive value.
	staleDependentsInterval time.Duration
	// watchPollInterval paces the client's watch surface, which polls the store and
	// diffs (see watchpoll.go). It bounds how stale a subscriber's view can be.
	watchPollInterval time.Duration
	concurrency       int // default worker count for all controllers; 0/1 = single-threaded
	// Event-log retention, applied globally by the GC sweeper (see WithEventRetention).
	// Zero on both disables the sweep — the log grows unbounded until configured.
	eventRetentionPerObject int
	eventRetentionMaxAge    time.Duration
	// startupFullPass is the default startup full-pass choice copied into each
	// reconciler. Off unless asked for, like fullPassInterval and for the same
	// reason: no reconcile may depend on a pass that scales with the object count,
	// so its zero value is also the correct default (see WithStartupFullPass).
	startupFullPass bool
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
	// order preserves registration order so Start launches reconcile loops
	// deterministically, rather than in random map order.
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

// Start brings the control plane up: the dependency waker, the per-controller
// reconcile loops, and the GC sweeper. All of them are periodic, since nothing is
// pushed at them. On success it returns a stop function that tears everything back
// down (see stop). Starting twice, or after stop, is an error, and the returned stop
// is nil in that case. A Beehive is one-shot: once stopped, make a new one.
//
// startCtx covers startup only. If it is already cancelled, startup aborts. The
// long-lived loops do not derive from it — the run ends when the returned stop is
// called.
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
	// so the goroutines below (GC sweeper, dependency waker) log unconditionally.
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

	// The dependency waker scans the write log on its own interval. It needs no
	// ordering against the reconcile loops below, because its first scan is bounded by
	// a cursor read rather than by when it happened to start: a change made before the
	// goroutine runs is either below the seed, and covered by the startup pass, or
	// above it, and picked up by the first tick.
	bh.wg.Go(func() {
		bh.waker.run(runCtx)
	})

	// Now launch the reconcile loops.
	for _, r := range bh.order {
		bh.wg.Go(func() {
			r.run(runCtx)
		})
	}

	// The stale-dependents pass, the waker's backstop. It needs no ordering against
	// the loops below either: it reads current state, so whatever it finds on its
	// first step is owed a pass whether or not the loops are up yet, and the work
	// queue holds the enqueue until one is.
	bh.wg.Go(func() {
		bh.staleDependentsRun(runCtx)
	})

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

// gcSweeperRun is the global garbage-collection backstop. Each reconcile loop
// collects its own kind; this one sweeps every kind, so a deletion-pending object of
// a client-only kind is still collected instead of stranding and RESTRICT-blocking
// its owner's delete forever. It sweeps once at startup, then on the GC interval.
//
// Failures inside a sweep are logged and swallowed, which is only safe because there
// is always a next tick: WithGCInterval rejects a non-positive interval, so a
// transient error costs one interval of latency rather than stranding a row for the
// life of the process. (runDriver still honours a non-positive interval by not
// running, which a Beehive built field by field in a test can reach.)
func (bh *Beehive) gcSweeperRun(ctx context.Context) {
	runDriver(ctx, bh.gcInterval, func(ctx context.Context) bool {
		bh.deletionPendingSweep(ctx)
		bh.eventRetentionSweep(ctx)
		bh.freePagesSweep(ctx)
		return true
	})
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

// freePagesSweep hands space freed by the two sweeps above it back to the operating
// system, for a store that can (see FreePagesReleaser) — collected rows and trimmed
// event runs are exactly what leaves reusable space behind, so this runs where that
// space is produced rather than on a cadence of its own.
//
// It is best-effort in the same way the sweeps are: a failure is logged and retried
// on the next tick, because nothing is incorrect while the space is unreclaimed. The
// store decides how much is worth releasing and may release none; the cap here only
// bounds how long one tick can hold the write lock.
func (bh *Beehive) freePagesSweep(ctx context.Context) {
	releaser, ok := bh.store.(FreePagesReleaser)
	if !ok {
		return
	}
	released, err := releaser.FreePagesRelease(ctx, freePagesPerSweep)
	if err != nil {
		bh.log().Warn("free-page release failed; retry next sweep", "err", err)
		return
	}
	if released > 0 {
		bh.log().Debug("released free pages", "pages", released)
	}
}

// deletionPendingSweep drives every deletion-pending object one step closer to
// removal (see deletionAdvance for the routing).
func (bh *Beehive) deletionPendingSweep(ctx context.Context) {
	rows, err := bh.store.DeletionRequestsList(ctx)
	if err != nil {
		bh.log().Warn("gc sweep: listing deletion-pending objects failed; retry next sweep", "err", err)
		return
	}
	for _, row := range rows {
		// Best-effort: a transient error is retried on the next sweep, but it is
		// still logged — for a client-only kind this sweep is the only collector,
		// so a row that fails every time would otherwise strand silently and
		// RESTRICT-block its owner's delete forever. ErrNotFound is the benign
		// already-collected race and stays quiet.
		if err := bh.deletionAdvance(ctx, row.GroupKind(), row.ID); err != nil && !errors.Is(err, ErrNotFound) {
			bh.log().Warn("gc sweep: collecting object failed; retry next sweep",
				"group", row.Group, "kind", row.Kind, "id", row.ID, "err", err)
		}
	}
}

// deletionAdvance drives one deletion-pending object a step closer to removal,
// routing on whether its kind has a controller. The GC sweeper is its only caller,
// since a delete records deletion_requested_at and nothing else, so every collect
// runs on the sweeper's goroutine and ctx rather than on whoever requested the
// delete.
//
// The routing matters for correctness, not speed. gcCollect cannot clear a finalizer:
// it cascades to owned children, then returns while any finalizer remains, because
// releasing one is the controller's decision. So an object of a registered kind has
// to be *queued*, letting its reconcile loop run the controller, which clears the
// finalizer and collects in the same pass. Calling gcCollect directly would make no
// progress, on every sweep, forever. A client-only kind has no loop to queue onto, so
// it is collected here — which is the whole reason the global sweep exists.
//
// Both arms are safe to repeat: queueing coalesces, and gcCollect does nothing while
// finalizers or live referrers remain and is harmless if another path got there
// first. The collect error is returned rather than logged, so the caller can report
// it in its own terms; the queueing arm always returns nil.
func (bh *Beehive) deletionAdvance(ctx context.Context, gk GroupKind, id ObjectID) error {
	if r, ok := bh.reconcilerFor(gk); ok {
		r.enqueue(id)
		return nil
	}
	_, err := bh.gcCollect(ctx, id)
	return err
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
	// The dependency waker (counted in wg) acquires bh.mu to resolve reconcilers;
	// holding it across wg.Wait would deadlock a waker mid-scan against Stop when
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

	// Client watch streams poll the store directly and are not counted in wg, so
	// stop does not terminate them: one ends when its own context is cancelled or
	// the store is closed under it.
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
		wakeInterval:            defaultWakeInterval,
		watchPollInterval:       defaultWatchPollInterval,
		staleDependentsInterval: defaultStaleDependentsInterval,
		reconcilers:             make(map[GroupKind]*reconciler),
		migrators:               make(map[GroupKind]Migrator),
	}
	// Assigned once here, like FreePagesReleaser's assertion in freePagesSweep,
	// except stored on the waker rather than asserted per tick: the waker already
	// owns per-run state (watermark, persisted) that this belongs beside.
	cursors, _ := s.(DriverCursorer)
	bh.waker = &waker{bh: bh, cursors: cursors}
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
		owedPassInterval: bh.owedPassInterval,
		fullPassInterval: bh.fullPassInterval,
		maxRetryInterval: defaultMaxRetryInterval,
		concurrency:      bh.concurrency,
		startupFullPass:  bh.startupFullPass,
		backoffFor:       make(map[ObjectID]time.Duration),
		// Inherit the control plane's logging config as the default; the options
		// below may override it for this controller.
		logger:   bh.logger,
		logLevel: bh.logLevel,
	}
	// Build the client once here so it's allocated per kind, not per reconcile,
	// and hand the same instance to both the adapter and the caller.
	client := &controllerClientImpl[Status]{bh: bh, gk: gk}
	adapter := &typedController[Spec, Status]{gk: gk, bh: bh, inner: c, client: client}
	r.adapter = adapter

	// Per-controller option overrides (e.g. WithFullPassInterval, WithMaxRetryInterval).
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

// enqueuerForPage returns an enqueue function that resolves each kind once and then
// caches it, for a caller queueing many ids across a few kinds at once. Resolving per
// id would take bh.mu every time, and one page of the dependency waker's scan can
// reach thousands of dependents — thousands of acquisitions of a mutex Register and
// stop also want.
//
// The registration set is frozen after Start (Register rejects a late call), so a
// cache that outlives one page would still be correct; it is scoped to the caller
// anyway, since nothing needs it longer and a per-call map needs no locking of
// its own.
func (bh *Beehive) enqueuerForPage() func(GroupKind, ObjectID) {
	resolved := map[GroupKind]*reconciler{}
	return func(gk GroupKind, id ObjectID) {
		r, ok := resolved[gk]
		if !ok {
			r, _ = bh.reconcilerFor(gk) // nil for a client-only kind, cached as such
			resolved[gk] = r
		}
		if r != nil {
			r.enqueue(id)
		}
	}
}
