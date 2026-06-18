package beehive

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errBoom = errors.New("boom")

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
	require.NoError(t, bh.Start())
	defer bh.Stop(context.Background())

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

	require.NoError(t, bh.Start())
	waitClosed(t, fc.startedCh, "controller Start")
	assert.True(t, bh.started)

	bh.Stop(context.Background())
	waitClosed(t, fc.stoppedCh, "controller Stop")
	assert.Equal(t, 1, fc.startCount())
	assert.Equal(t, 1, fc.stopCount())
	assert.False(t, bh.started)
}

func TestStartRejectsSecondStart(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	require.NoError(t, bh.Start())
	defer bh.Stop(context.Background())
	require.Error(t, bh.Start())
}

func TestStopWithoutStartIsNoOp(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	fc := newFakeController()
	require.NoError(t, Register(bh, GroupKind{Kind: "Widget"}, fc))

	bh.Stop(context.Background()) // never started: must not panic or stop controllers
	assert.Equal(t, 0, fc.stopCount())
}

func TestStopReturnsWithExpiredContext(t *testing.T) {
	bh, err := New(&fakeStore{}, WithResyncInterval(0))
	require.NoError(t, err)

	fc := newFakeController()
	require.NoError(t, Register(bh, GroupKind{Kind: "Widget"}, fc))
	require.NoError(t, bh.Start())
	waitClosed(t, fc.startedCh, "controller Start")

	// An already-expired ctx caps the drain wait. Stop must still return (the
	// test completing proves no hang) and still stop the controllers.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	bh.Stop(ctx)

	assert.Equal(t, 1, fc.stopCount())
	assert.False(t, bh.started)
}

func TestStartRollsBackOnFailure(t *testing.T) {
	bh, err := New(&fakeStore{})
	require.NoError(t, err)

	good := newFakeController()
	bad := newFakeController()
	bad.startErr = errBoom
	require.NoError(t, Register(bh, GroupKind{Kind: "Good"}, good))
	require.NoError(t, Register(bh, GroupKind{Kind: "Bad"}, bad))

	require.ErrorIs(t, bh.Start(), errBoom)
	assert.False(t, bh.started)

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
