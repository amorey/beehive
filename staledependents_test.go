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

	require.NotEmpty(t, probe.staleKinds)
	assert.Equal(t, []GroupKind{clientTestGK}, probe.staleKinds[0],
		"the scan is asked only for kinds that have somewhere to enqueue")
}
