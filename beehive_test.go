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
	_, err = Register(bh, gk, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

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
	bh, err := New(&fakeStore{}, WithResyncInterval(10*time.Second))
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, bh.resyncInterval)

	overridden := GroupKind{Kind: "Overridden"}
	_, err = Register(bh, overridden, &noopController[tSpec, tStatus]{},
		WithResyncInterval(2*time.Second), WithMaxRetryInterval(7*time.Second))
	require.NoError(t, err)

	inherited := GroupKind{Kind: "Inherited"}
	_, err = Register(bh, inherited, &noopController[tSpec, tStatus]{})
	require.NoError(t, err)

	assert.Equal(t, 2*time.Second, bh.reconcilers[overridden].resyncInterval)
	assert.Equal(t, 7*time.Second, bh.reconcilers[overridden].maxRetryInterval)
	assert.Equal(t, 10*time.Second, bh.reconcilers[inherited].resyncInterval,
		"controller without an override inherits the Beehive default")
}

func TestStartStopLifecycle(t *testing.T) {
	// Disable resync so the reconcile loop just blocks on ctx until Stop.
	bh, err := New(&fakeStore{}, WithResyncInterval(0))
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
	bh, err := New(&fakeStore{}, WithResyncInterval(0))
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
