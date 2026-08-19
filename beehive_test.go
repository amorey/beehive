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
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/amorey/beehive/internal/storeapi"
	"github.com/amorey/beehive/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// eventRetentionSweep trims the log to the configured bound and is a no-op until
// WithEventRetention sets one.
func TestSweepEventRetention(t *testing.T) {
	store, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	ctx := context.Background()

	gk := GroupKind{Kind: "Widget"}
	obj, err := store.Objects().Create(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
	require.NoError(t, err)
	for _, r := range []string{"R1", "R2", "R3", "R4"} {
		err := store.Events().Add(ctx, gk, obj.ID, EventsAddInput{Category: "c", Type: "Normal", Reason: r})
		require.NoError(t, err)
	}

	t.Run("unconfigured is a no-op", func(t *testing.T) {
		bh, err := New(store)
		require.NoError(t, err)
		bh.eventRetentionSweep(ctx)
		got, err := store.Events().List(ctx, obj.ID, storeapi.EventQuery{})
		require.NoError(t, err)
		assert.Len(t, got, 4)
	})

	t.Run("store error is logged, not fatal", func(t *testing.T) {
		bad, err := New(eventErrStore{newClientTestStore(t)}, WithEventRetention(2, time.Hour))
		require.NoError(t, err)
		bad.eventRetentionSweep(ctx) // EventsSweep errors → warn branch, must not panic
	})

	t.Run("caps per object", func(t *testing.T) {
		bh, err := New(store, WithEventRetention(2, 0))
		require.NoError(t, err)
		bh.eventRetentionSweep(ctx)
		got, err := store.Events().List(ctx, obj.ID, storeapi.EventQuery{})
		require.NoError(t, err)
		assert.Len(t, got, 2)
	})
}

// freePagesStore records the drain calls the sweeper makes, and can fail them.
type freePagesStore struct {
	Store
	called chan int
	err    error
}

func (s *freePagesStore) ReclaimSpace(_ context.Context, maxPages int) (int, error) {
	select {
	case s.called <- maxPages:
	default:
	}
	if s.err != nil {
		return 0, s.err
	}
	return maxPages, nil
}

// freePagesSweep is the third thing the GC sweeper does, and the only one that is
// optional: releasing free pages is a backend capability, not part of Store, so the
// sweep has to work whether or not the store has it.
func TestSweepFreePages(t *testing.T) {
	ctx := context.Background()

	t.Run("drains a store that implements the capability", func(t *testing.T) {
		store := &freePagesStore{Store: &fakeStore{}, called: make(chan int, 4)}
		bh, err := New(store)
		require.NoError(t, err)

		bh.freePagesSweep(ctx)

		assert.Equal(t, freePagesPerSweep, recv(t, store.called),
			"the sweeper should pass its own cap, not a store-chosen one")
	})

	t.Run("a store that reclaims nothing is a no-op", func(t *testing.T) {
		bh, err := New(&fakeStore{})
		require.NoError(t, err)
		bh.freePagesSweep(ctx) // fakeStore reclaims 0; nothing to log, nothing to fail
	})

	t.Run("a failed drain is logged, not fatal", func(t *testing.T) {
		store := &freePagesStore{Store: &fakeStore{}, called: make(chan int, 4), err: errBoom}
		bh, err := New(store)
		require.NoError(t, err)
		bh.freePagesSweep(ctx) // warn branch; the next tick retries
		assert.Equal(t, freePagesPerSweep, recv(t, store.called))
	})
}

// eventSweepStore records the retention arguments each sweep passes down.
type eventSweepStore struct {
	Store
	budget chan int
}

func (s *eventSweepStore) Events() storeapi.Events {
	return eventsOverride{Events: s.Store.Events(), sweep: s.sweep}
}

func (s *eventSweepStore) sweep(_ context.Context, _ int, _ time.Duration, capBudget int) (int, error) {
	select {
	case s.budget <- capBudget:
	default:
	}
	return 0, nil
}

// The sweeper's per-sweep budgets are sized against gcBudgetInterval, so a
// longer WithGCInterval has to buy proportionally more work per sweep rather
// than the same work at a lower rate. For the event cap that is the difference
// between a bounded log and one that sits over its cap indefinitely.
func TestGCBudgetsScaleWithTheInterval(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		interval time.Duration
		factor   int
	}{
		{time.Second, 1}, // shorter than the base: already at rate, sweeping more often
		{30 * time.Second, 1},
		{5 * time.Minute, 10},
		{45 * time.Second, 1}, // truncated, never rounded up
	} {
		t.Run(tc.interval.String(), func(t *testing.T) {
			// Two stores, because the free-page drain is a capability the sweeper
			// type-asserts for: an embedded Store interface would not carry it.
			pages := &freePagesStore{Store: &fakeStore{}, called: make(chan int, 4)}
			pagesBH, err := New(pages, WithGCInterval(tc.interval))
			require.NoError(t, err)
			pagesBH.freePagesSweep(ctx)

			events := &eventSweepStore{Store: &fakeStore{}, budget: make(chan int, 4)}
			eventsBH, err := New(events, WithGCInterval(tc.interval), WithEventRetention(10, 0))
			require.NoError(t, err)
			eventsBH.eventRetentionSweep(ctx)

			assert.Equal(t, freePagesPerSweep*tc.factor, recv(t, pages.called))
			assert.Equal(t, eventCapPerSweep*tc.factor, recv(t, events.budget))
		})
	}
}

// owedClearStore records the keep set each reclaim sweep passes down.
type owedClearStore struct {
	Store
	kept chan []GroupKind
	err  error
}

func (s *owedClearStore) ReconcileOwed() storeapi.ReconcileOwed {
	return owedOverride{ReconcileOwed: s.Store.ReconcileOwed(), sweep: s.sweep}
}

func (s *owedClearStore) sweep(_ context.Context, keep []GroupKind) (int, error) {
	select {
	case s.kept <- keep:
	default:
	}
	return 0, s.err
}

// The reclaim keeps exactly the kinds that have a reconcile loop to drain their
// count.
func TestSweepReconcileOwedKeepsRegisteredKinds(t *testing.T) {
	store := &owedClearStore{Store: &fakeStore{}, kept: make(chan []GroupKind, 4)}
	bh := newTestBeehive(t, store)
	widget, drone := GroupKind{Kind: "Widget"}, GroupKind{Kind: "Drone"}
	registerNoop[tSpec, tStatus](t, bh, widget)
	registerNoop[tSpec, tStatus](t, bh, drone)

	bh.reconcileOwedSweep(context.Background())

	assert.ElementsMatch(t, []GroupKind{widget, drone}, recv(t, store.kept))
}

// A failed reclaim must not cost the tick: the sweeps after it still run.
func TestSweepReconcileOwedFailureIsNotFatal(t *testing.T) {
	owed := &owedClearStore{Store: &fakeStore{}, kept: make(chan []GroupKind, 4), err: errBoom}
	store := &freePagesStore{Store: owed, called: make(chan int, 4)}
	bh := newTestBeehive(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go bh.gcSweeperRun(ctx)

	assert.Empty(t, recv(t, owed.kept), "no controllers registered, so nothing is kept")
	assert.Equal(t, freePagesPerSweep, recv(t, store.called), "the sweep after it still runs")
}

// The reclaim runs on every GC tick, so emitting would wake every tailer and the
// dependency waker for a write no consumer can act on. The control write after
// the sweep is what makes the absence assertable: an emitting reclaim would put
// the swept object ahead of it in the stream.
func TestSweepReconcileOwedEmitsNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bh := newTestBeehive(t, newClientTestStore(t), fast()...)
	registerNoop[cSpec, cStatus](t, bh, clientTestGK)
	loose := NewClient[cSpec, cStatus](bh, clientOnlyGK)
	swept := mustCreate(t, ctx, loose, uniqueName(), cSpec{Val: "swept"})
	target := mustCreate(t, ctx, loose, uniqueName(), cSpec{Val: "target"})
	// The production stamp: a new depends_on edge owes swept a reconcile no loop
	// will ever run.
	_, err := bh.store.Edges().Add(ctx, swept.ID, target.ID, RelationDependsOn)
	require.NoError(t, err)

	stream, err := loose.WatchList(ctx)
	require.NoError(t, err)

	bh.reconcileOwedSweep(ctx)
	control := mustCreate(t, ctx, loose, uniqueName(), cSpec{Val: "control"})

	ev := recv(t, stream.Changes)
	assert.Equal(t, control.ID, ev.Object.ID, "the reclaim must not reach the change stream")
}

// A Beehive with no controllers at all reclaims every count: the counts here are
// left by a prior process, and nothing in this one consumes them.
func TestSweepReconcileOwedWithNoControllers(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	bh := newTestBeehive(t, store)
	loose := NewClient[cSpec, cStatus](bh, clientOnlyGK)
	from := mustCreate(t, ctx, loose, uniqueName(), cSpec{Val: "from"})
	to := mustCreate(t, ctx, loose, uniqueName(), cSpec{Val: "to"})
	res, err := bh.store.Edges().Add(ctx, from.ID, to.ID, RelationDependsOn)
	require.NoError(t, err)
	require.True(t, res.ReconcileOwedStamped, "the edge must owe a reconcile to begin with")

	bh.reconcileOwedSweep(ctx)

	raw, err := store.Objects().Get(ctx, from.ID)
	require.NoError(t, err)
	assert.Zero(t, raw.ReconcileOwed, "no reconcile loop can drain it, so the sweep does")
}

// The GC sweeper's three steps run together on every tick, so a store that grows a
// freelist through the first two gets it drained by the third without a cadence of
// its own.
func TestGCSweeperDrainsFreePages(t *testing.T) {
	store := &freePagesStore{Store: &fakeStore{}, called: make(chan int, 8)}
	bh := newTestBeehive(t, store, WithGCInterval(time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bh.gcSweeperRun(ctx)

	recv(t, store.called) // startup pass
	recv(t, store.called) // a periodic tick
}

func TestNewAppliesDefaults(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{})
	// Literals, not the constants: comparing a default to its own constant passes
	// whatever the value is, so it would not notice a default changing. These four
	// are the contract — cheap owed-work drain and GC on, both object-count-scaled
	// full passes off — so changing one should be a deliberate edit here.
	assert.Equal(t, 30*time.Second, bh.owedPassInterval, "owed work drains by default")
	assert.Equal(t, time.Duration(0), bh.fullPassInterval, "the periodic full pass is opt-in")
	assert.False(t, bh.startupFullPass, "the startup full pass is opt-in too; a kind that needs it says so")
	assert.Equal(t, 30*time.Second, bh.gcInterval, "dead rows are collected by default")
	assert.NotNil(t, bh.reconcilers)
}

func TestNewPropagatesOptionError(t *testing.T) {
	_, err := New(&fakeStore{}, func(any) error { return errBoom })
	require.ErrorIs(t, err, errBoom)
}

func TestRegisterStoresReconciler(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{})

	gk := GroupKind{Kind: "Widget"}
	err := Register(bh, gk, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	r, ok := bh.reconcilers[gk]
	require.True(t, ok, "reconciler should be registered under its GroupKind")
	assert.Equal(t, gk, r.gk)
	assert.Equal(t, defaultOwedPassInterval, r.owedPassInterval, "inherits the Beehive default")
	assert.Equal(t, defaultFullPassInterval, r.fullPassInterval, "inherits the Beehive default")
	assert.Equal(t, defaultIndividualPassInterval, r.individualPassInterval, "inherits the Beehive default")
	assert.Equal(t, defaultMaxRetryInterval, r.maxRetryInterval)
}

func TestWithMigratorRegisters(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{})

	gk := GroupKind{Kind: "Widget"}
	mig := &fakeMigrator{specVersion: 2, statusVersion: 1}
	err := Register(bh, gk, &noopController[tSpec, tStatus]{}, WithMigrator(mig))
	require.NoError(t, err)

	assert.Same(t, mig, bh.migratorFor(gk), "the migrator passed to Register is installed for the kind")
}

func TestMigratorForReturnsNilWhenUnset(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{})

	// Registered without WithMigrator.
	gk := GroupKind{Kind: "Widget"}
	err := Register(bh, gk, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	assert.Nil(t, bh.migratorFor(gk), "a kind registered without a migrator has none")
	assert.Nil(t, bh.migratorFor(GroupKind{Kind: "Unknown"}), "an unregistered kind has none")
}

func TestRegisterRejectsDuplicate(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{})

	gk := GroupKind{Kind: "Widget"}
	err := Register(bh, gk, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)
	err = Register(bh, gk, &noopController[tSpec, tStatus]{})
	require.Error(t, err)
}

func TestRegisterRejectedAfterStart(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{})
	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	defer stop(context.Background())

	err = Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
	require.Error(t, err)
}

func TestRegisterPerControllerOverride(t *testing.T) {
	// Global default set at New; one controller overrides it, another inherits.
	bh := newTestBeehive(t, &fakeStore{}, WithFullPassInterval(10*time.Second))
	assert.Equal(t, 10*time.Second, bh.fullPassInterval)

	overridden := GroupKind{Kind: "Overridden"}
	err := Register(bh, overridden, &noopController[tSpec, tStatus]{},
		WithFullPassInterval(2*time.Second), WithMaxRetryInterval(7*time.Second))
	require.NoError(t, err)

	inherited := GroupKind{Kind: "Inherited"}
	err = Register(bh, inherited, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	assert.Equal(t, 2*time.Second, bh.reconcilers[overridden].fullPassInterval)
	assert.Equal(t, 7*time.Second, bh.reconcilers[overridden].maxRetryInterval)
	assert.Equal(t, 10*time.Second, bh.reconcilers[inherited].fullPassInterval,
		"controller without an override inherits the Beehive default")
}

func TestStartStopLifecycle(t *testing.T) {
	// Disable the full pass so the reconcile loop just blocks on ctx until Stop.
	bh := newTestBeehive(t, &fakeStore{}, WithFullPassInterval(0))

	err := Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	assert.Equal(t, beehiveRunning, bh.state)

	require.NoError(t, stop(context.Background()))
	assert.Equal(t, beehiveStopped, bh.state)
}

func TestStartRejectsSecondStart(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{})

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	defer stop(context.Background())
	_, err = bh.Start(context.Background())
	require.Error(t, err)
}

func TestStopWithoutStartIsNoOp(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{})

	err := Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	// never started: must not panic, and reports no error.
	require.NoError(t, bh.stop(context.Background()))
}

func TestStopReturnsWithExpiredContext(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{}, WithFullPassInterval(0))

	err := Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(context.Background())
	require.NoError(t, err)

	// An already-expired ctx caps the drain wait. stop must still return (the
	// test completing proves no hang) and report the expired context so the caller
	// can tell the drain didn't complete in time.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, stop(ctx), context.Canceled)

	assert.Equal(t, beehiveStopped, bh.state)
}

// TestStartAbortsOnCancelledContext exercises the startCtx.Err() abort path in
// Start: an already-cancelled start context makes Start bail before launching
// the reconcile loops, returning an error and no stop function.
func TestStartAbortsOnCancelledContext(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{})

	err := Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before Start runs

	stop, err := bh.Start(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, stop, "no stop function on a failed Start")
	assert.Equal(t, beehiveNew, bh.state)
}

// The watermark is taken before Start hands back the stop func, so a write made
// the instant it does lands above it. Nothing else can close that window: a
// waker seeded on its own goroutine takes its mark whenever the runtime gets to
// it, which is after the caller has already written.
func TestStartSeedsTheWakerBeforeItReturns(t *testing.T) {
	store := &seedProbe{Store: &fakeStore{}, mark: 500}
	bh := newTestBeehive(t, store, WithFullPassInterval(0))
	err := Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	defer stop(context.Background())

	assert.True(t, bh.waker.seeded, "the waker must be seeded by the time a caller can write")
	assert.EqualValues(t, 500, bh.waker.watermark)
}

// A store that cannot answer the seed read must not fail Start: the waker is an
// optimisation over the stale-dependents pass, so an unseeded one costs latency.
func TestStartSurvivesAFailedWakerSeed(t *testing.T) {
	store := &seedProbe{Store: &fakeStore{}, err: errBoom}
	bh := newTestBeehive(t, store, WithFullPassInterval(0))
	err := Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	stop, err := bh.Start(context.Background())
	require.NoError(t, err, "a failed seed is not a failed start")
	require.NotNil(t, stop)
	// Stopped first, so the waker goroutine is done and its fields are settled.
	require.NoError(t, stop(context.Background()))

	assert.False(t, bh.waker.seeded, "an unseeded waker scans nothing until the seed lands")
	assert.Positive(t, store.reads, "and the loop retried the seed rather than scanning from zero")
}

// A waker with nothing to wake reads nothing at startup either.
func TestStartSkipsPrimingADisabledWaker(t *testing.T) {
	t.Run("turned off", func(t *testing.T) {
		store := &seedProbe{Store: &fakeStore{}}
		bh := newTestBeehive(t, store, WithFullPassInterval(0), withDependencyWakerOff())
		err := Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
		require.NoError(t, err)

		stop, err := bh.Start(context.Background())
		require.NoError(t, err)
		require.NoError(t, stop(context.Background()))

		assert.Zero(t, store.reads, "a disabled waker has no watermark to take")
	})

	t.Run("no controllers", func(t *testing.T) {
		store := &seedProbe{Store: &fakeStore{}}
		bh := newTestBeehive(t, store)

		stop, err := bh.Start(context.Background())
		require.NoError(t, err)
		require.NoError(t, stop(context.Background()))

		assert.Zero(t, store.reads, "nothing registered, nowhere to queue a wake")
	})
}

// The seed read is the one place Start can now notice a caller abandoning
// startup. It aborts as it does for an already-cancelled context, and takes the
// wake subscription back down with it.
func TestStartAbortsWhenTheStartContextIsCancelledDuringTheSeed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &seedProbe{Store: &fakeStore{}, err: context.Canceled, onRead: cancel}
	bh := newTestBeehive(t, store, WithFullPassInterval(0))
	err := Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	stop, err := bh.Start(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, stop, "no stop function on a failed Start")
	assert.Equal(t, beehiveNew, bh.state)
	assert.Nil(t, bh.waker.rx, "an aborted start leaves no subscriber on the hub")
}

// An aborted Start leaves the Beehive startable, so the next attempt must seed
// from scratch. Inheriting the last attempt's seed would leave a failed one
// scanning from a watermark this attempt never read.
func TestStartRePrimesAfterAnAbortedStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The seed itself succeeds; the abort is the caller cancelling around it.
	store := &seedProbe{Store: &fakeStore{}, mark: 500, onRead: cancel}
	bh := newTestBeehive(t, store, WithFullPassInterval(0))
	err := Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	_, err = bh.Start(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, bh.waker.seeded, "the first attempt did seed before the abort")

	store.onRead, store.err = nil, errBoom
	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	require.NoError(t, stop(context.Background()))

	assert.False(t, bh.waker.seeded, "a failed seed leaves nothing to scan from")
}

func TestRegisterPropagatesOptionError(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{})
	err := Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{}, func(any) error { return errBoom })
	require.ErrorIs(t, err, errBoom)
}

// TestRunGCSweeperTicks covers gcSweeperRun's periodic branch: after the startup
// pass it sweeps again on every full-pass tick. The store signals each sweep, so the
// second signal proves the ticker.C arm ran.
func TestRunGCSweeperTicks(t *testing.T) {
	store := &listProbeStore{Store: &fakeStore{}, gcSwept: make(chan struct{}, 8)}
	bh := newTestBeehive(t, store, WithGCInterval(time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bh.gcSweeperRun(ctx)

	recv(t, store.gcSwept) // startup pass
	recv(t, store.gcSwept) // a periodic tick
}

// writeLogRetentionSweep trims the write log to the configured bound. Unlike the
// event log it is bounded by default: entries land on every object write, and a
// status write bumps resource_version, so the log grows at reconcile rate
// whether or not anyone opts in.
func TestSweepWriteLogRetention(t *testing.T) {
	ctx := context.Background()

	t.Run("bounded by default", func(t *testing.T) {
		bh, err := New(newClientTestStore(t))
		require.NoError(t, err)
		assert.Equal(t, defaultWriteLogMaxAge, bh.writeLogRetentionMaxAge)
		assert.Zero(t, bh.writeLogRetentionPerKind, "no count bound by default")
	})

	t.Run("both zero disables the sweep", func(t *testing.T) {
		store := newClientTestStore(t)
		bh, err := New(store, WithWriteLogRetention(0, 0))
		require.NoError(t, err)
		gk := GroupKind{Kind: "Widget"}
		for range 3 {
			_, err := store.Objects().Create(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
			require.NoError(t, err)
		}

		bh.writeLogRetentionSweep(ctx)

		page, _, err := store.ObjectWrites().ListSince(ctx, gk, 0, 10)
		require.NoError(t, err)
		assert.Len(t, page, 3)
	})

	t.Run("caps per kind", func(t *testing.T) {
		store := newClientTestStore(t)
		bh, err := New(store, WithWriteLogRetention(2, 0))
		require.NoError(t, err)
		gk := GroupKind{Kind: "Widget"}
		for range 3 {
			_, err := store.Objects().Create(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
			require.NoError(t, err)
		}

		bh.writeLogRetentionSweep(ctx)

		page, _, err := store.ObjectWrites().ListSince(ctx, gk, 0, 10)
		require.NoError(t, err)
		assert.Len(t, page, 2)
	})
}

// A second stop must not tear the watches down while the first is still
// draining. state flips to stopped before the drain begins, so a call that finds
// it — a retry after a drain timeout, a signal handler racing the first — is not
// the one that owns the teardown: it returns without closing the wake hub, which
// is what leaves every stream open for the writes the draining loops still owe.
func TestSecondStopLeavesTheFirstDrainAlone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	ctrl := &blockingController[cSpec, cStatus]{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	bh := newTestBeehive(t, newClientTestStore(t))
	err := Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	stopFirst, err := bh.Start(ctx)
	require.NoError(t, err)
	mustCreate(t, ctx, client, "held", cSpec{})
	<-ctrl.entered // the drain now has something to wait for

	stream, err := client.WatchList(ctx)
	require.NoError(t, err)

	firstDone := make(chan error, 1)
	go func() { firstDone <- stopFirst(ctx) }()

	// stop takes ownership under bh.mu before it drains, so wait for that rather
	// than racing it. A spin on observable state, not a sleep: it waits for the
	// condition however long the scheduler takes to get there.
	for deadline := time.Now().Add(testTimeout); ; runtime.Gosched() {
		bh.mu.Lock()
		owned := bh.state == beehiveStopped
		bh.mu.Unlock()
		if owned {
			break
		}
		require.False(t, time.Now().After(deadline), "the first stop never took ownership")
	}

	// The second call must return without touching the wake hub. A live write
	// arriving on the stream afterwards is the proof: a closed hub would have
	// ended it instead.
	require.NoError(t, bh.stop(ctx))
	obj := mustCreate(t, ctx, client, "after-second-stop", cSpec{})
	for {
		ev := recv(t, stream.Changes)
		if ev.Object != nil && ev.Object.ID == obj.ID {
			break
		}
	}

	close(ctrl.release)
	require.NoError(t, <-firstDone)
	for range stream.Changes { // the first stop's close ends it, once its drain is done
	}
}

// blockingController holds a reconcile loop open until release is closed, so a
// test can keep a drain from finishing. It reports entering, since a drain only
// blocks once a reconcile is actually in flight, and it does not watch ctx —
// stop cancels that, which is exactly what must not end the wait.
type blockingController[Spec, Status any] struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (c *blockingController[Spec, Status]) Reconcile(context.Context, ControllerClient[Status], *Object[Spec, Status]) ReconcileResult {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return Settled()
}

// The New form of the option reaches a controller only if Register copies it
// out of the Beehive: a field missing from that literal compiles and does
// nothing.
func TestRegisterInheritsIndividualPassInterval(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{}, WithIndividualPassInterval(90*time.Second))

	gk := GroupKind{Kind: "Widget"}
	require.NoError(t, Register(bh, gk, &noopController[tSpec, tStatus]{}))

	r := bh.reconcilers[gk]
	assert.Equal(t, 90*time.Second, r.individualPassInterval)
	assert.NotNil(t, r.individualPassRand, "the jitter source is inherited too")
}

// Register wins over New, as every other inherited cadence does.
func TestRegisterOverridesIndividualPassInterval(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{}, WithIndividualPassInterval(90*time.Second))

	gk := GroupKind{Kind: "Widget"}
	require.NoError(t, Register(bh, gk, &noopController[tSpec, tStatus]{}, WithIndividualPassInterval(time.Second)))

	assert.Equal(t, time.Second, bh.reconcilers[gk].individualPassInterval)
}
