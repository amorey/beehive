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
	// Both full passes scale with the object count, so both are opt-in and no
	// reconcile may depend on either; convergence is carried by the owed pass.
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
	// The dependency-wake scan costs nothing in a quiet system, so it runs an
	// order of magnitude more often than the passes that scale with object count.
	defaultWakeInterval = 1 * time.Second
	// The stale-dependents pass is the waker's backstop; its cadence is set by
	// acceptable staleness after a crash, not by cost.
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
	// Driver cadences. Owed work is bounded by what is outstanding, a full pass
	// by the object count, GC by deletion-pending rows, the wake scan by what
	// changed. gcInterval and staleDependentsInterval are always positive when
	// the Beehive came from New; wakeInterval <= 0 turns the waker off.
	owedPassInterval        time.Duration
	fullPassInterval        time.Duration
	gcInterval              time.Duration
	wakeInterval            time.Duration
	staleDependentsInterval time.Duration
	watchPollInterval       time.Duration
	concurrency             int // default worker count for all controllers; 0/1 = single-threaded
	// Event-log retention, applied globally by the GC sweeper. Zero on both
	// disables the sweep.
	eventRetentionPerObject int
	eventRetentionMaxAge    time.Duration
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
	// wakes carries each committed object write's log position to the kind's
	// tailer. Built in New, not Start: watches work on a Beehive that never ran.
	wakes  wakeHub
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
// is called.
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

	if err := startCtx.Err(); err != nil {
		cancel()
		return nil, fmt.Errorf("beehive: start aborted: %w", err)
	}

	// None of the goroutines below need ordering against each other: the waker's
	// first scan is bounded by a cursor read, and the stale-dependents pass reads
	// current state, so nothing depends on when they happen to start.
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
		bh.freePagesSweep(ctx)
		return true
	})
}

// eventRetentionSweep trims the event log to the configured retention. No-op unless a
// bound is set; a failed sweep is retried on the next tick.
func (bh *Beehive) eventRetentionSweep(ctx context.Context) {
	if bh.eventRetentionPerObject <= 0 && bh.eventRetentionMaxAge <= 0 {
		return
	}
	if _, err := bh.store.EventsSweep(ctx, bh.eventRetentionPerObject, bh.eventRetentionMaxAge); err != nil {
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

// freePagesSweep hands space freed by the sweeps above back to the OS, for
// a store that implements FreePagesReleaser. Best-effort: nothing is incorrect
// while the space is unreclaimed.
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
func (bh *Beehive) deletionAdvance(ctx context.Context, gk GroupKind, id ObjectID) error {
	if r, ok := bh.reconcilerFor(gk); ok {
		r.enqueue(id)
		return nil
	}
	_, err := bh.gcCollect(ctx, id)
	return err
}

// stop cancels the reconcile loops and waits for them to drain, bounded by ctx.
// It returns non-nil only when the drain hit ctx's deadline. No-op if not
// running.
func (bh *Beehive) stop(ctx context.Context) error {
	bh.mu.Lock()
	if bh.state != beehiveRunning {
		bh.mu.Unlock()
		return nil
	}
	// Release bh.mu before waiting on wg: the waker (counted in wg) takes bh.mu
	// to resolve reconcilers, so holding it across wg.Wait would deadlock.
	bh.state = beehiveStopped
	bh.cancel()
	bh.log().Info("control plane stopping")
	bh.mu.Unlock()

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

	// The store-backed client watches poll the store and are not counted in wg:
	// each ends when its own context is cancelled. SchedulesWatch streams report
	// the work queue instead and end here, after their final value.
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
		wakeInterval:            defaultWakeInterval,
		watchPollInterval:       defaultWatchPollInterval,
		staleDependentsInterval: defaultStaleDependentsInterval,
		reconcilers:             make(map[GroupKind]*reconciler),
		migrators:               make(map[GroupKind]Migrator),
		wakes:                   newWakeHub(),
	}
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

// signalRequeue enqueues ref's reconcile when the write that owes it commits.
// AfterCommit means a rollback (or savepoint unwind) discards the enqueue, and
// outside a transaction it runs inline. The enqueue does not clear the backoff
// ladder, and requeueNow on an in-flight id makes it dispatchable again at
// once — so callers must gate on what the write actually changed (see
// signalSpecWritten and DependenciesAdd).
func (bh *Beehive) signalRequeue(ctx context.Context, ref ObjectRef) {
	bh.store.AfterCommit(ctx, func(context.Context) {
		if r, ok := bh.reconcilerFor(ref.GroupKind()); ok {
			r.requeueNow(ref.ID)
		}
	})
}

// signalObjectWritten wakes gk's tailer with the write's log position once the
// write commits. AfterCommit for the same reasons as signalRequeue: a rollback
// publishes nothing, and the wake cannot outrun the row it announces.
func (bh *Beehive) signalObjectWritten(ctx context.Context, gk GroupKind, rv int64) {
	bh.store.AfterCommit(ctx, func(context.Context) {
		_ = bh.wakes.Send(gk, rv) // ErrClosed after stop; nothing is left to wake
	})
}

// enqueuerForPage returns an enqueue function that resolves each kind once and
// caches it, for callers queueing many ids across a few kinds — resolving per
// id would take bh.mu every time. The registration set is frozen after Start,
// so the cache cannot go stale.
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
