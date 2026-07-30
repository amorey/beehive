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
		_, err := store.EventsAdd(ctx, gk, obj.ID, RawEvent{Category: "c", Type: "Normal", Reason: r})
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

// The GC sweeper's three steps run together on every tick, so a store that grows a
// freelist through the first two gets it drained by the third without a cadence of
// its own.
func TestGCSweeperDrainsFreePages(t *testing.T) {
	store := &freePagesStore{Store: &fakeStore{}, called: make(chan int, 8)}
	bh, err := New(store, WithGCInterval(time.Millisecond))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bh.gcSweeperRun(ctx)

	recv(t, store.called) // startup pass
	recv(t, store.called) // a periodic tick
}

func TestNewAppliesDefaults(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)
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
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	gk := GroupKind{Kind: "Widget"}
	_, err = Register(bh, gk, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	r, ok := bh.reconcilers[gk]
	require.True(t, ok, "reconciler should be registered under its GroupKind")
	assert.Equal(t, gk, r.gk)
	assert.Equal(t, defaultOwedPassInterval, r.owedPassInterval, "inherits the Beehive default")
	assert.Equal(t, defaultFullPassInterval, r.fullPassInterval, "inherits the Beehive default")
	assert.Equal(t, defaultMaxRetryInterval, r.maxRetryInterval)
}

func TestWithMigratorRegisters(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	gk := GroupKind{Kind: "Widget"}
	mig := &fakeMigrator{specVersion: 2, statusVersion: 1}
	_, err = Register(bh, gk, &noopController[tSpec, tStatus]{}, WithMigrator(mig))
	require.NoError(t, err)

	assert.Same(t, mig, bh.migratorFor(gk), "the migrator passed to Register is installed for the kind")
}

func TestMigratorForReturnsNilWhenUnset(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	// Registered without WithMigrator.
	gk := GroupKind{Kind: "Widget"}
	_, err = Register(bh, gk, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	assert.Nil(t, bh.migratorFor(gk), "a kind registered without a migrator has none")
	assert.Nil(t, bh.migratorFor(GroupKind{Kind: "Unknown"}), "an unregistered kind has none")
}

func TestRegisterRejectsDuplicate(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	gk := GroupKind{Kind: "Widget"}
	_, err = Register(bh, gk, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)
	_, err = Register(bh, gk, &noopController[tSpec, tStatus]{})
	require.Error(t, err)
}

func TestRegisterRejectedAfterStart(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)
	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	defer stop(context.Background())

	_, err = Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
	require.Error(t, err)
}

func TestRegisterPerControllerOverride(t *testing.T) {
	// Global default set at New; one controller overrides it, another inherits.
	bh, err := New(&fakeStore{}, WithFullPassInterval(10*time.Second))
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, bh.fullPassInterval)

	overridden := GroupKind{Kind: "Overridden"}
	_, err = Register(bh, overridden, &noopController[tSpec, tStatus]{},
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
	bh, err := New(&fakeStore{}, WithFullPassInterval(0))
	require.NoError(t, err)

	_, err = Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	assert.Equal(t, beehiveRunning, bh.state)

	require.NoError(t, stop(context.Background()))
	assert.Equal(t, beehiveStopped, bh.state)
}

func TestStartRejectsSecondStart(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	defer stop(context.Background())
	_, err = bh.Start(context.Background())
	require.Error(t, err)
}

func TestStopWithoutStartIsNoOp(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	_, err = Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	// never started: must not panic, and reports no error.
	require.NoError(t, bh.stop(context.Background()))
}

func TestStopReturnsWithExpiredContext(t *testing.T) {
	bh, err := New(&fakeStore{}, WithFullPassInterval(0))
	require.NoError(t, err)

	_, err = Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
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
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	_, err = Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before Start runs

	stop, err := bh.Start(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, stop, "no stop function on a failed Start")
	assert.Equal(t, beehiveNew, bh.state)
}

func TestRegisterPropagatesOptionError(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)
	_, err = Register(bh, GroupKind{Kind: "Widget"}, &noopController[tSpec, tStatus]{}, func(any) error { return errBoom })
	require.ErrorIs(t, err, errBoom)
}

// TestRunGCSweeperTicks covers gcSweeperRun's periodic branch: after the startup
// pass it sweeps again on every full-pass tick. The store signals each sweep, so the
// second signal proves the ticker.C arm ran.
func TestRunGCSweeperTicks(t *testing.T) {
	store := &listProbeStore{Store: &fakeStore{}, gcSwept: make(chan struct{}, 8)}
	bh, err := New(store, WithGCInterval(time.Millisecond))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bh.gcSweeperRun(ctx)

	recv(t, store.gcSwept) // startup pass
	recv(t, store.gcSwept) // a periodic tick
}
