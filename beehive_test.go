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
	obj, err := store.ObjectsCreate(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
	require.NoError(t, err)
	for _, r := range []string{"R1", "R2", "R3", "R4"} {
		err := store.EventsAdd(ctx, gk, obj.ID, RawEvent{Category: "c", Type: "Normal", Reason: r})
		require.NoError(t, err)
	}

	t.Run("unconfigured is a no-op", func(t *testing.T) {
		bh, err := New(store)
		require.NoError(t, err)
		bh.eventRetentionSweep(ctx)
		got, err := store.EventsList(ctx, obj.ID, storeapi.EventQuery{})
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
		got, err := store.EventsList(ctx, obj.ID, storeapi.EventQuery{})
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

func (s *freePagesStore) FreePagesRelease(_ context.Context, maxPages int) (int, error) {
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

	t.Run("a store without the capability is skipped", func(t *testing.T) {
		bh, err := New(&fakeStore{})
		require.NoError(t, err)
		bh.freePagesSweep(ctx) // must not panic: fakeStore has no FreePagesRelease
	})

	t.Run("a failed drain is logged, not fatal", func(t *testing.T) {
		store := &freePagesStore{Store: &fakeStore{}, called: make(chan int, 4), err: errBoom}
		bh, err := New(store)
		require.NoError(t, err)
		bh.freePagesSweep(ctx) // warn branch; the next tick retries
		assert.Equal(t, freePagesPerSweep, recv(t, store.called))
	})
}

// owedClearStore records the keep set each reclaim sweep passes down.
type owedClearStore struct {
	Store
	kept chan []GroupKind
	err  error
}

func (s *owedClearStore) ReconcileOwedSweep(_ context.Context, keep []GroupKind) (int, error) {
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
	_, err := bh.store.EdgesAdd(ctx, swept.ID, target.ID, RelationDependsOn)
	require.NoError(t, err)

	_, ch, err := loose.WatchList(ctx)
	require.NoError(t, err)

	bh.reconcileOwedSweep(ctx)
	control := mustCreate(t, ctx, loose, uniqueName(), cSpec{Val: "control"})

	ev := recv(t, ch)
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
	res, err := bh.store.EdgesAdd(ctx, from.ID, to.ID, RelationDependsOn)
	require.NoError(t, err)
	require.True(t, res.ReconcileOwedStamped, "the edge must owe a reconcile to begin with")

	bh.reconcileOwedSweep(ctx)

	raw, err := store.ObjectsGet(ctx, from.ID)
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
	assert.False(t, bh.startupFullPass, "the startup full pass is opt-in too; no reconcile may depend on it")
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
	_, err := Register(bh, gk, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	r, ok := bh.reconcilers[gk]
	require.True(t, ok, "reconciler should be registered under its GroupKind")
	assert.Equal(t, gk, r.gk)
	assert.Equal(t, defaultOwedPassInterval, r.owedPassInterval, "inherits the Beehive default")
	assert.Equal(t, defaultFullPassInterval, r.fullPassInterval, "inherits the Beehive default")
	assert.Equal(t, defaultMaxRetryInterval, r.maxRetryInterval)
}

func TestWithMigratorRegisters(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{})

	gk := GroupKind{Kind: "Widget"}
	mig := &fakeMigrator{specVersion: 2, statusVersion: 1}
	_, err := Register(bh, gk, &noopController[tSpec, tStatus]{}, WithMigrator(mig))
	require.NoError(t, err)

	assert.Same(t, mig, bh.migratorFor(gk), "the migrator passed to Register is installed for the kind")
}

func TestMigratorForReturnsNilWhenUnset(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{})

	// Registered without WithMigrator.
	gk := GroupKind{Kind: "Widget"}
	_, err := Register(bh, gk, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	assert.Nil(t, bh.migratorFor(gk), "a kind registered without a migrator has none")
	assert.Nil(t, bh.migratorFor(GroupKind{Kind: "Unknown"}), "an unregistered kind has none")
}

func TestRegisterRejectsDuplicate(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{})

	gk := GroupKind{Kind: "Widget"}
	_, err := Register(bh, gk, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)
	_, err = Register(bh, gk, &noopController[tSpec, tStatus]{})
	require.Error(t, err)
}

func TestRegisterRejectedAfterStart(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{})
	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	defer stop(context.Background())

	_, err = Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
	require.Error(t, err)
}

func TestRegisterPerControllerOverride(t *testing.T) {
	// Global default set at New; one controller overrides it, another inherits.
	bh := newTestBeehive(t, &fakeStore{}, WithFullPassInterval(10*time.Second))
	assert.Equal(t, 10*time.Second, bh.fullPassInterval)

	overridden := GroupKind{Kind: "Overridden"}
	_, err := Register(bh, overridden, &noopController[tSpec, tStatus]{},
		WithFullPassInterval(2*time.Second), WithMaxRetryInterval(7*time.Second))
	require.NoError(t, err)

	inherited := GroupKind{Kind: "Inherited"}
	_, err = Register(bh, inherited, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	assert.Equal(t, 2*time.Second, bh.reconcilers[overridden].fullPassInterval)
	assert.Equal(t, 7*time.Second, bh.reconcilers[overridden].maxRetryInterval)
	assert.Equal(t, 10*time.Second, bh.reconcilers[inherited].fullPassInterval,
		"controller without an override inherits the Beehive default")
}

func TestStartStopLifecycle(t *testing.T) {
	// Disable the full pass so the reconcile loop just blocks on ctx until Stop.
	bh := newTestBeehive(t, &fakeStore{}, WithFullPassInterval(0))

	_, err := Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
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

	_, err := Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	// never started: must not panic, and reports no error.
	require.NoError(t, bh.stop(context.Background()))
}

func TestStopReturnsWithExpiredContext(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{}, WithFullPassInterval(0))

	_, err := Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
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

	_, err := Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before Start runs

	stop, err := bh.Start(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, stop, "no stop function on a failed Start")
	assert.Equal(t, beehiveNew, bh.state)
}

func TestRegisterPropagatesOptionError(t *testing.T) {
	bh := newTestBeehive(t, &fakeStore{})
	_, err := Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{}, func(any) error { return errBoom })
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
			_, err := store.ObjectsCreate(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
			require.NoError(t, err)
		}

		bh.writeLogRetentionSweep(ctx)

		page, _, err := store.ObjectWritesListSince(ctx, gk, 0, 10)
		require.NoError(t, err)
		assert.Len(t, page, 3)
	})

	t.Run("caps per kind", func(t *testing.T) {
		store := newClientTestStore(t)
		bh, err := New(store, WithWriteLogRetention(2, 0))
		require.NoError(t, err)
		gk := GroupKind{Kind: "Widget"}
		for range 3 {
			_, err := store.ObjectsCreate(ctx, gk, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
			require.NoError(t, err)
		}

		bh.writeLogRetentionSweep(ctx)

		page, _, err := store.ObjectWritesListSince(ctx, gk, 0, 10)
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
	_, err := Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	stopFirst, err := bh.Start(ctx)
	require.NoError(t, err)
	mustCreate(t, ctx, client, "held", cSpec{})
	<-ctrl.entered // the drain now has something to wait for

	_, stream, err := client.WatchList(ctx)
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
		ev := recv(t, stream)
		require.NotEqual(t, Failed, ev.Type, "the second stop ended the stream: %v", ev.Err)
		if ev.Object != nil && ev.Object.ID == obj.ID {
			break
		}
	}

	close(ctrl.release)
	require.NoError(t, <-firstDone)
	for range stream { // the first stop's close ends it, once its drain is done
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

func (c *blockingController[Spec, Status]) Reconcile(context.Context, ControllerClient[Status], *Object[Spec, Status]) (Result, error) {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return Result{}, nil
}
