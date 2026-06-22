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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAppliesDefaults(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)
	assert.Equal(t, defaultResyncInterval, bh.resyncInterval)
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
	require.NoError(t, Register(bh, gk, newFakeController()))

	r, ok := bh.reconcilers[gk]
	require.True(t, ok, "reconciler should be registered under its GroupKind")
	assert.Equal(t, gk, r.gk)
	assert.Equal(t, defaultResyncInterval, r.resyncInterval, "inherits the Beehive default")
	assert.Equal(t, defaultMaxRetryInterval, r.maxRetryInterval)
}

func TestRegisterRejectsDuplicate(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	gk := GroupKind{Kind: "Widget"}
	require.NoError(t, Register(bh, gk, newFakeController()))
	require.Error(t, Register(bh, gk, newFakeController()))
}

func TestRegisterRejectedAfterStart(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)
	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	defer stop(context.Background())

	require.Error(t, Register(bh, GroupKind{Kind: "Widget"}, newFakeController()))
}

func TestRegisterPerControllerOverride(t *testing.T) {
	// Global default set at New; one controller overrides it, another inherits.
	bh, err := New(&fakeStore{}, WithResyncInterval(10*time.Second))
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, bh.resyncInterval)

	overridden := GroupKind{Kind: "Overridden"}
	require.NoError(t, Register(bh, overridden, newFakeController(),
		WithResyncInterval(2*time.Second), WithMaxRetryInterval(7*time.Second)))

	inherited := GroupKind{Kind: "Inherited"}
	require.NoError(t, Register(bh, inherited, newFakeController()))

	assert.Equal(t, 2*time.Second, bh.reconcilers[overridden].resyncInterval)
	assert.Equal(t, 7*time.Second, bh.reconcilers[overridden].maxRetryInterval)
	assert.Equal(t, 10*time.Second, bh.reconcilers[inherited].resyncInterval,
		"controller without an override inherits the Beehive default")
}

func TestStartStopLifecycle(t *testing.T) {
	// Disable resync so the reconcile loop just blocks on ctx until Stop.
	bh, err := New(&fakeStore{}, WithResyncInterval(0))
	require.NoError(t, err)

	fc := newFakeController()
	require.NoError(t, Register(bh, GroupKind{Kind: "Widget"}, fc))

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	waitClosed(t, fc.startedCh, "controller Start")
	assert.Equal(t, beehiveRunning, bh.state)

	require.NoError(t, stop(context.Background()))
	waitClosed(t, fc.stoppedCh, "controller Stop")
	assert.Equal(t, 1, fc.startCount())
	assert.Equal(t, 1, fc.stopCount())
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

	fc := newFakeController()
	require.NoError(t, Register(bh, GroupKind{Kind: "Widget"}, fc))

	// never started: must not panic or stop controllers, and reports no error.
	require.NoError(t, bh.stop(context.Background()))
	assert.Equal(t, 0, fc.stopCount())
}

func TestStopReturnsWithExpiredContext(t *testing.T) {
	bh, err := New(&fakeStore{}, WithResyncInterval(0))
	require.NoError(t, err)

	fc := newFakeController()
	require.NoError(t, Register(bh, GroupKind{Kind: "Widget"}, fc))
	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	waitClosed(t, fc.startedCh, "controller Start")

	// An already-expired ctx caps the drain wait. stop must still return (the
	// test completing proves no hang), still stop the controllers, and report the
	// expired context so the caller can tell the drain didn't complete in time.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, stop(ctx), context.Canceled)

	assert.Equal(t, 1, fc.stopCount())
	assert.Equal(t, beehiveStopped, bh.state)
}

// TestStartRollsBackStartedController exercises the rollback loop body in Start:
// the good controller, registered first, starts; the bad one then fails, and the
// good one must be rolled back. Start iterates in registration order, so this is
// deterministic.
func TestStartRollsBackStartedController(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	good := newFakeController()
	bad := &fakeController{startedCh: make(chan struct{}), stoppedCh: make(chan struct{}), startErr: errBoom}

	require.NoError(t, Register(bh, GroupKind{Kind: "Good"}, good))
	require.NoError(t, Register(bh, GroupKind{Kind: "Bad"}, bad))

	_, err = bh.Start(context.Background())
	require.ErrorIs(t, err, errBoom)

	// good started before bad failed, so it must have been rolled back.
	assert.Equal(t, 1, good.startCount())
	assert.Equal(t, 1, good.stopCount(), "a started controller must be rolled back")
	// bad's Start failed, so it is never added to the started set and not stopped.
	assert.Equal(t, 0, bad.stopCount())
}

func TestRegisterPropagatesOptionError(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)
	err = Register(bh, GroupKind{Kind: "Widget"}, newFakeController(), func(any) error { return errBoom })
	require.ErrorIs(t, err, errBoom)
}

func TestStartRollsBackOnFailure(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	good := newFakeController()
	bad := newFakeController()
	bad.startErr = errBoom
	require.NoError(t, Register(bh, GroupKind{Kind: "Good"}, good))
	require.NoError(t, Register(bh, GroupKind{Kind: "Bad"}, bad))

	_, err = bh.Start(context.Background())
	require.ErrorIs(t, err, errBoom)
	assert.Equal(t, beehiveNew, bh.state)

	// Map iteration order is randomized, so we can't say whether `good` started
	// before `bad` failed. Assert the order-independent invariant instead: any
	// controller that started successfully must have been rolled back (Stopped).
	if good.startCount() > 0 {
		assert.Equal(t, 1, good.stopCount(), "a started controller must be rolled back")
	}
	// The controller whose Start failed is never added to the started set, so it
	// is not stopped.
	assert.Equal(t, 0, bad.stopCount())
}
