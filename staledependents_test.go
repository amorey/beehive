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
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// settlingCapture reports every object it reconciles and settles it, so a later
// pass cannot be attributed to the owed-work listing: a settled object is
// invisible to it.
type settlingCapture struct{ ch chan ObjectID }

func (c *settlingCapture) Reconcile(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
	if err := cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{}); err != nil {
		return Result{}, err
	}
	c.ch <- obj.ID
	return Result{}, nil
}

// TestStaleDependentsPassEnqueuesStaleDependents is the driver end to end, with
// every other route to the dependent closed: the waker is off, the full pass is
// off, and the dependent is settled — so no owed-work listing can name it. What
// is left is re-derivation from the watermark.
func TestStaleDependentsPassEnqueuesStaleDependents(t *testing.T) {
	ctx := context.Background()
	probe := &listProbeStore{
		Store:        newClientTestStore(t),
		watermarkSet: make(chan struct{}, 1),
	}
	// The waker off, so only re-derivation can reach the dependent.
	bh, err := New(probe, fast(withDependencyWakeInterval(0))...)
	require.NoError(t, err)

	reconciled := make(chan ObjectID, 8)
	_, err = Register(bh, clientTestGK, &settlingCapture{ch: reconciled}, WithFullPassInterval(0))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	target, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)
	dep, err := client.Create(ctx, cSpec{Val: "b"})
	require.NoError(t, err)
	require.NoError(t, addEdge(ctx, probe.Store, dep.ID, target.ID, RelationDependsOn))

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { stop(ctx) })

	// Wait for the dependent's own watermark write, not just its pass: the write
	// lands after Reconcile returns, and a target change that slipped under it would
	// be recorded as observed.
	waitClosed(t, probe.watermarkSet, "the dependent's watermark write")
	drainProbe(reconciled)

	_, err = client.Update(ctx, target.ID, cSpec{Val: "moved"})
	require.NoError(t, err)

	awaitMatch(t, reconciled, func(id ObjectID) bool { return id == dep.ID },
		"the stale pass to re-derive a wake nothing recorded")
}

// TestStaleDependentsPassIgnoresUnregisteredKinds pins the filter the scan is
// asked for: only kinds with a reconcile loop, since a client-only dependent is
// stale forever and would otherwise be re-scanned on every pass for the life of
// the row. A kind registered in a later build joins the list and is found on the
// next pass, so nothing is stranded by the exclusion.
func TestStaleDependentsPassIgnoresUnregisteredKinds(t *testing.T) {
	ctx := context.Background()
	probe := &listProbeStore{
		Store:       newClientTestStore(t),
		staleListed: make(chan struct{}, 1),
	}
	bh, err := New(probe, fast()...)
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { stop(ctx) })
	waitClosed(t, probe.staleListed, "the stale-dependents listing")

	asked := probe.kindsAsked()
	require.NotEmpty(t, asked)
	assert.Equal(t, []GroupKind{clientTestGK}, asked[0],
		"the scan is asked only for kinds that have somewhere to enqueue")
}

// staleListErrorStore fails the staleness listing, for the sweep's failure arm.
type staleListErrorStore struct {
	fakeStore
	calls atomic.Int64
}

func (s *staleListErrorStore) DependentsListStale(context.Context, []GroupKind, ObjectID, int) ([]ObjectRef, error) {
	s.calls.Add(1)
	return nil, errBoom
}

// TestStaleDependentsSweepWarnsAndRetriesOnListFailure pins the failure contract:
// a sweep that cannot read gives up on this pass and says so. There is no cursor
// to hold and nothing was drained — the listing derives its answer from current
// state — so the next tick re-derives the same set, which is why abandoning the
// sweep is the whole of the repair.
func TestStaleDependentsSweepWarnsAndRetriesOnListFailure(t *testing.T) {
	ctx := context.Background()
	store := &staleListErrorStore{}
	logger, logs := captureLogger(slog.LevelWarn)
	bh := &Beehive{store: store, logger: logger}
	kinds := []GroupKind{clientTestGK}

	bh.staleDependentsSweep(ctx, kinds)
	require.EqualValues(t, 1, store.calls.Load(), "a failed page abandons the sweep")
	assert.Contains(t, logs.String(), "listing stale dependents failed")

	bh.staleDependentsSweep(ctx, kinds)
	assert.EqualValues(t, 2, store.calls.Load(), "and the next pass asks again from the start")
}

// TestStaleDependentsSweepIsQuietOnShutdown separates the two reasons a listing
// fails. Stop cancels the same ctx the sweep reads on, so a pass in flight when
// the control plane goes down fails for no reason of its own — warning there
// would report a fault on every clean shutdown.
func TestStaleDependentsSweepIsQuietOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	logger, logs := captureLogger(slog.LevelWarn)
	bh := &Beehive{store: &staleListErrorStore{}, logger: logger}

	bh.staleDependentsSweep(ctx, []GroupKind{clientTestGK})

	assert.Empty(t, logs.String(), "a cancelled read is shutdown, not a lost pass")
}
