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
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/amorey/beehive/internal/storeapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// depDroppingController models a dependent that outlives its target: it depends
// on targetID and, the moment that target starts finalizing, releases the edge
// (DependenciesDelete) so the target can be collected. The dependent itself is
// never deleted.
type depDroppingController struct {
	mu       sync.Mutex
	reader   Client[cSpec, cStatus]
	depID    ObjectID
	targetID ObjectID
}

func (c *depDroppingController) Reconcile(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
	c.mu.Lock()
	reader, depID, targetID := c.reader, c.depID, c.targetID
	c.mu.Unlock()
	if obj.ID != depID {
		return Result{}, nil // only the dependent acts
	}
	target, err := reader.Get(ctx, targetID)
	if errors.Is(err, ErrNotFound) {
		return Result{}, nil // target already gone
	}
	if err != nil {
		return Result{}, err
	}
	if target.DeletionRequestedAt != nil {
		return Result{}, cc.DependenciesDelete(ctx, depID, targetID)
	}
	return Result{}, nil
}

// finalizerClearingController clears finalizer (if it holds one) the moment an
// object is finalizing, so GC can then remove the row. With no finalizer it is a
// pure no-op reconciler — exactly what a cascade-only owner needs.
type finalizerClearingController struct {
	finalizer string // empty => never clears anything
}

func (c *finalizerClearingController) Reconcile(ctx context.Context, client ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
	if obj.DeletionRequestedAt == nil || c.finalizer == "" {
		return Result{}, nil
	}
	for _, f := range obj.Finalizers {
		if f == c.finalizer {
			return Result{}, client.FinalizersDelete(ctx, obj.ID, c.finalizer)
		}
	}
	return Result{}, nil
}

// hasIncomingRefsGatingController models the documented finalizer workflow: an
// object holding `finalizer` clears it only once RefsHasIncoming reports no live
// claim, so a shared resource outlives its last real user. Objects that don't
// hold the finalizer are left for GC directly.
type hasIncomingRefsGatingController struct {
	finalizer string
}

func (c *hasIncomingRefsGatingController) Reconcile(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
	if obj.DeletionRequestedAt == nil {
		return Result{}, nil
	}
	held := false
	for _, f := range obj.Finalizers {
		if f == c.finalizer {
			held = true
		}
	}
	if !held {
		return Result{}, nil
	}
	referenced, err := cc.RefsHasIncoming(ctx, obj.ID)
	if err != nil || referenced {
		return Result{}, err // a live user remains; keep the finalizer
	}
	return Result{}, cc.FinalizersDelete(ctx, obj.ID, c.finalizer)
}

// waitForDeletions consumes w until it has seen a Deleted event for every id in
// want, failing on timeout. The watcher must be subscribed before the deletions
// are triggered so no event is missed.
func waitForDeletions(t *testing.T, w <-chan Change[cSpec, cStatus], want ...ObjectID) {
	t.Helper()
	pending := make(map[ObjectID]struct{}, len(want))
	for _, id := range want {
		pending[id] = struct{}{}
	}
	timeout := time.After(testTimeout)
	for len(pending) > 0 {
		select {
		case ev, ok := <-w:
			if !ok {
				t.Fatal("watch channel closed before all deletions observed")
			}
			if ev.Type == Deleted {
				delete(pending, ev.Object.ID)
			}
		case <-timeout:
			t.Fatalf("timed out waiting for deletions; still pending: %v", pending)
		}
	}
}

// collectFakeStore drives collect's transaction body with controllable results
// so each store-call error branch can be exercised in isolation. ObjectsGetMeta
// returns a finalizing object (so collect proceeds past the live-object guard);
// the per-method hooks default to success and are overridden per test. Within
// runs fn inline (from the embedded fakeStore), so all of collect runs here.
type collectFakeStore struct {
	fakeStore
	finalizers      []string // on the collected object
	getMetaErr      error    // ObjectsGetMeta
	markErr         error    // DeletionsMarkOwned
	dropDependsErr  error    // RefsDeleteFinalizingDependsOn
	hasRefs         bool     // RefsHasIncoming result
	hasRefsErr      error    // RefsHasIncoming error
	outgoingErr     error    // RefsListOutgoing error
	deleteObjectErr error    // ObjectsDelete error
}

func (s *collectFakeStore) ObjectsGetMeta(_ context.Context, id ObjectID) (*RawObject, error) {
	if s.getMetaErr != nil {
		return nil, s.getMetaErr
	}
	now := time.Now()
	return &RawObject{ID: id, DeletionRequestedAt: &now, Finalizers: s.finalizers}, nil
}
func (s *collectFakeStore) DeletionsMarkOwned(context.Context, ObjectID) ([]storeapi.Referrer, error) {
	return nil, s.markErr
}
func (s *collectFakeStore) RefsDeleteFinalizingDependsOn(context.Context, ObjectID) error {
	return s.dropDependsErr
}
func (s *collectFakeStore) RefsHasIncoming(context.Context, ObjectID) (bool, error) {
	return s.hasRefs, s.hasRefsErr
}
func (s *collectFakeStore) RefsListOutgoing(context.Context, ObjectID) ([]storeapi.Referrer, error) {
	return nil, s.outgoingErr
}
func (s *collectFakeStore) ObjectsDelete(context.Context, ObjectID) error {
	return s.deleteObjectErr
}

func TestCollectGetObjectMetaError(t *testing.T) {
	bh, err := New(&collectFakeStore{getMetaErr: errBoom})
	require.NoError(t, err)
	_, err = bh.collect(context.Background(), 1)
	require.ErrorIs(t, err, errBoom)
}

func TestCollectDeletionsMarkOwnedError(t *testing.T) {
	bh, err := New(&collectFakeStore{markErr: errBoom})
	require.NoError(t, err)
	_, err = bh.collect(context.Background(), 1)
	require.ErrorIs(t, err, errBoom)
}

func TestCollectDropDependsRefsError(t *testing.T) {
	bh, err := New(&collectFakeStore{dropDependsErr: errBoom})
	require.NoError(t, err)
	_, err = bh.collect(context.Background(), 1)
	require.ErrorIs(t, err, errBoom)
}

func TestCollectHasIncomingRefsError(t *testing.T) {
	bh, err := New(&collectFakeStore{hasRefsErr: errBoom})
	require.NoError(t, err)
	_, err = bh.collect(context.Background(), 1)
	require.ErrorIs(t, err, errBoom)
}

func TestCollectListOutgoingRefsError(t *testing.T) {
	// hasRefs false so collect proceeds to the delete prep where RefsListOutgoing runs.
	bh, err := New(&collectFakeStore{outgoingErr: errBoom})
	require.NoError(t, err)
	_, err = bh.collect(context.Background(), 1)
	require.ErrorIs(t, err, errBoom)
}

func TestCollectDeleteObjectError(t *testing.T) {
	bh, err := New(&collectFakeStore{deleteObjectErr: errBoom})
	require.NoError(t, err)
	_, err = bh.collect(context.Background(), 1)
	require.ErrorIs(t, err, errBoom)
}

// gcFixture builds a Beehive over a real sqlite store plus a client, so collect
// tests can exercise real DeletionsRequest/ObjectsDelete/ref semantics. No
// controller is started: collect is driven directly. The default resync is left
// enabled, so collect's post-commit wakes for this client-only kind defer to the
// (idle) sweeper rather than recursively collecting synchronously — letting these
// tests observe each intermediate state one collect call at a time.
func gcFixture(t *testing.T) (*Beehive, Client[cSpec, cStatus]) {
	t.Helper()
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	return bh, NewClient[cSpec, cStatus](bh, clientTestGK)
}

func TestCollectIgnoresLiveObject(t *testing.T) {
	ctx := context.Background()
	bh, client := gcFixture(t)

	obj, err := client.Create(ctx, cSpec{Val: "alive"})
	require.NoError(t, err)

	gone, err := bh.collect(ctx, obj.ID)
	require.NoError(t, err)
	assert.False(t, gone, "live object not collected")

	_, err = client.Get(ctx, obj.ID) // not deletion-pending: untouched
	require.NoError(t, err)
}

func TestCollectDeletesUnfinalizedObject(t *testing.T) {
	ctx := context.Background()
	bh, client := gcFixture(t)

	obj, err := client.Create(ctx, cSpec{Val: "doomed"})
	require.NoError(t, err)
	require.NoError(t, client.Delete(ctx, obj.ID))

	gone, err := bh.collect(ctx, obj.ID)
	require.NoError(t, err)
	assert.True(t, gone, "unfinalized object collected")

	_, err = client.Get(ctx, obj.ID) // no finalizers, no refs: physically gone
	require.ErrorIs(t, err, ErrNotFound)
}

// TestCollectDeletesSelfDependentObject pins the deadlock collect names the self
// case for: a self-dependency is its own referrer, so refs' ON DELETE RESTRICT
// would pin the row forever if RefsDeleteFinalizingDependsOn did not drop the
// edge first. Verified by mutation — excluding from_id = to_id there leaves the
// object undeletable.
//
// It is deliberately *not* the twin of TestClientListDependentsIncludesSelfEdge:
// collect reads refs through RefsHasIncoming and RefsDeleteFinalizingDependsOn,
// never RefsListIncoming, so a self-edge filtered out of that call would leave
// this path untouched. The two tests cover different consumers, not two halves of
// one mistake.
func TestCollectDeletesSelfDependentObject(t *testing.T) {
	ctx := context.Background()
	bh, client := gcFixture(t)

	obj, err := client.Create(ctx, cSpec{Val: "self"})
	require.NoError(t, err)
	require.NoError(t, addRef(ctx, bh.store, obj.ID, obj.ID, RelationDependsOn))
	require.NoError(t, client.Delete(ctx, obj.ID))

	gone, err := bh.collect(ctx, obj.ID)
	require.NoError(t, err)
	assert.True(t, gone, "a self-dependency must not hold its own object open")

	_, err = client.Get(ctx, obj.ID)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestCollectKeepsFinalizedObject(t *testing.T) {
	ctx := context.Background()
	bh, client := gcFixture(t)

	obj, err := client.Create(ctx, cSpec{Val: "guarded"}, WithFinalizers("f"))
	require.NoError(t, err)
	require.NoError(t, client.Delete(ctx, obj.ID))

	gone, err := bh.collect(ctx, obj.ID)
	require.NoError(t, err)
	assert.False(t, gone, "object with a finalizer is not collected")

	got, err := client.Get(ctx, obj.ID) // finalizer still set: lingers
	require.NoError(t, err)
	assert.Equal(t, []string{"f"}, got.Finalizers)
}

func TestCollectCascadesAndBlocksOnChild(t *testing.T) {
	ctx := context.Background()
	bh, client := gcFixture(t)

	owner, err := client.Create(ctx, cSpec{Val: "owner"})
	require.NoError(t, err)
	child, err := client.Create(ctx, cSpec{Val: "child"}, WithOwner(owner.ID))
	require.NoError(t, err)

	require.NoError(t, client.Delete(ctx, owner.ID))
	gone, err := bh.collect(ctx, owner.ID)
	require.NoError(t, err)
	assert.False(t, gone, "owner blocked by child ref")

	// The owner lingers while the child still references it (RESTRICT).
	_, err = client.Get(ctx, owner.ID)
	require.NoError(t, err)

	// The cascade requested the child's deletion.
	gotChild, err := client.Get(ctx, child.ID)
	require.NoError(t, err)
	assert.NotNil(t, gotChild.DeletionRequestedAt, "child deletion requested by cascade")
}

func TestCollectDeletesOwnerAfterChildGone(t *testing.T) {
	ctx := context.Background()
	bh, client := gcFixture(t)

	owner, err := client.Create(ctx, cSpec{Val: "owner"})
	require.NoError(t, err)
	child, err := client.Create(ctx, cSpec{Val: "child"}, WithOwner(owner.ID))
	require.NoError(t, err)

	require.NoError(t, client.Delete(ctx, owner.ID))
	require.NoError(t, client.Delete(ctx, child.ID))

	// Collect the child first: no finalizers, so it's removed and its owned_by
	// edge cascades away, freeing the owner.
	gone, err := bh.collect(ctx, child.ID)
	require.NoError(t, err)
	assert.True(t, gone)
	_, err = client.Get(ctx, child.ID)
	require.ErrorIs(t, err, ErrNotFound)

	// Now the owner has no referrers and is collectable.
	gone, err = bh.collect(ctx, owner.ID)
	require.NoError(t, err)
	assert.True(t, gone)
	_, err = client.Get(ctx, owner.ID)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestCollectBreaksSelfDependency(t *testing.T) {
	ctx := context.Background()
	bh, client := gcFixture(t)

	obj, err := client.Create(ctx, cSpec{Val: "self"})
	require.NoError(t, err)
	// A controller accidentally recorded a self-dependency.
	require.NoError(t, addRef(ctx, bh.store, obj.ID, obj.ID, RelationDependsOn))
	require.NoError(t, client.Delete(ctx, obj.ID))

	gone, err := bh.collect(ctx, obj.ID)
	require.NoError(t, err)
	assert.True(t, gone, "a self-dependency must not block collection")

	_, err = client.Get(ctx, obj.ID)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestIntegrationGCBreaksDependencyCycle(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)

	// Resync disabled: the cycle must break purely event-driven.
	bh, err := New(store, WithResyncInterval(0))
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &finalizerClearingController{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	a, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)
	b, err := client.Create(ctx, cSpec{Val: "b"})
	require.NoError(t, err)
	// A and B depend on each other: neither can be collected until the cycle breaks.
	require.NoError(t, addRef(ctx, store, a.ID, b.ID, RelationDependsOn))
	require.NoError(t, addRef(ctx, store, b.ID, a.ID, RelationDependsOn))

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := client.WatchList(wctx)
	require.NoError(t, err)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	require.NoError(t, client.Delete(ctx, a.ID))
	require.NoError(t, client.Delete(ctx, b.ID))
	waitForDeletions(t, w, a.ID, b.ID)
}

func TestIntegrationGCFinalizerGateIgnoresFinalizingDependent(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)

	// Resync disabled: the finalizer gate must clear purely event-driven.
	bh, err := New(store, WithResyncInterval(0))
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &hasIncomingRefsGatingController{finalizer: "gate"})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	obj, err := client.Create(ctx, cSpec{Val: "self"}, WithFinalizers("gate"))
	require.NoError(t, err)
	// A finalizing dependent that points at obj — modeled as a self-dependency, so
	// the referrer is itself deletion-pending the instant obj is deleted. Without
	// the fix, the gate sees this edge, never clears the finalizer, and GC stalls.
	require.NoError(t, addRef(ctx, store, obj.ID, obj.ID, RelationDependsOn))

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := client.WatchList(wctx)
	require.NoError(t, err)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	require.NoError(t, client.Delete(ctx, obj.ID))
	waitForDeletions(t, w, obj.ID)
}

func TestIntegrationGCResumesDanglingDeleteOnStartup(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)

	// Simulate a crash mid-delete: a deletion-pending row is already in the durable
	// store before any control plane runs. (Written through the store directly, so
	// no reconcile has touched it.)
	raw, err := store.ObjectsCreate(ctx, &RawObject{
		Group: clientTestGK.Group, Kind: clientTestGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	// Settle it first, so the GC sweeper's startup pass really is the *only* path
	// that can reach this row. A raw ObjectsCreate leaves observed_generation NULL,
	// which the startup resumption of owed work would pick up as unsettled — the row
	// would then be removed for two reasons and this test would stop pinning either
	// one. Deletion does not bump generation, so the row stays settled below.
	_, err = store.ObjectsUpdateStatus(ctx, clientTestGK, raw.ID, raw.Generation, []byte(`{}`), 0)
	require.NoError(t, err)
	_, _, err = store.DeletionsRequest(ctx, clientTestGK, raw.ID)
	require.NoError(t, err)

	// A fresh Beehive with no spec-startup pass and resync disabled: the GC
	// sweeper's unconditional startup pass is the only thing that can drive this row
	// to removal. It is the sweeper's alone now — the reconciler no longer keeps a
	// per-kind deletion-pending listing that duplicated it.
	bh, err := New(store, WithResyncInterval(0))
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &finalizerClearingController{},
		WithStartupResync(false))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	// Subscribe before Start so the Deleted event can't be missed.
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := client.WatchList(wctx)
	require.NoError(t, err)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	waitForDeletions(t, w, raw.ID)

	_, err = client.Get(ctx, raw.ID)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestIntegrationGCDeletesAfterFinalizerCleared(t *testing.T) {
	ctx := context.Background()

	// Resync disabled: the post-reconcile GC hook alone must remove the row once
	// the controller clears the finalizer in the same pass.
	bh, err := New(newClientTestStore(t), WithResyncInterval(0))
	require.NoError(t, err)

	_, err = Register(bh, clientTestGK, &finalizerClearingController{finalizer: "f"})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "doomed"}, WithFinalizers("f"))
	require.NoError(t, err)

	// Subscribe before deleting so the Deleted event can't be missed.
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := client.Watch(wctx, obj.ID)
	require.NoError(t, err)

	require.NoError(t, client.Delete(ctx, obj.ID))
	waitForDeletions(t, w, obj.ID)

	_, err = client.Get(ctx, obj.ID)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestIntegrationGCCascadeWithResyncDisabled(t *testing.T) {
	ctx := context.Background()

	// Resync disabled: the cascade must complete purely event-driven. Deleting the
	// child frees the owner's RESTRICT, and removing the child must wake the owner
	// directly — there is no backstop tick to re-check it.
	bh, err := New(newClientTestStore(t), WithResyncInterval(0))
	require.NoError(t, err)

	_, err = Register(bh, clientTestGK, &finalizerClearingController{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	owner, err := client.Create(ctx, cSpec{Val: "owner"})
	require.NoError(t, err)
	child, err := client.Create(ctx, cSpec{Val: "child"}, WithOwner(owner.ID))
	require.NoError(t, err)

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := client.WatchList(wctx)
	require.NoError(t, err)

	require.NoError(t, client.Delete(ctx, owner.ID))
	waitForDeletions(t, w, owner.ID, child.ID)
}

func TestIntegrationGCCascadeDeletesOwnerAndChild(t *testing.T) {
	ctx := context.Background()

	// A short resync drives the deletion-pending backstop, which re-checks the
	// owner once its child (and the owned_by edge) is gone.
	bh, err := New(newClientTestStore(t), WithGCInterval(5*time.Millisecond))
	require.NoError(t, err)

	_, err = Register(bh, clientTestGK, &finalizerClearingController{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	owner, err := client.Create(ctx, cSpec{Val: "owner"})
	require.NoError(t, err)
	child, err := client.Create(ctx, cSpec{Val: "child"}, WithOwner(owner.ID))
	require.NoError(t, err)

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := client.WatchList(wctx)
	require.NoError(t, err)

	// Deleting only the owner must cascade to the child and remove both.
	require.NoError(t, client.Delete(ctx, owner.ID))
	waitForDeletions(t, w, owner.ID, child.ID)

	_, err = client.Get(ctx, owner.ID)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = client.Get(ctx, child.ID)
	require.ErrorIs(t, err, ErrNotFound)
}

// TestIntegrationGCSweepsClientOnlyKind verifies the global GC sweeper collects
// a deletion-pending object whose kind has no registered controller. The owner
// kind has a controller; the child kind is client-only. Deleting the owner
// cascades to the child, but only the global sweeper can collect that child —
// without it the child strands and its owned_by edge RESTRICT-blocks the owner
// forever.
func TestIntegrationGCSweepsClientOnlyKind(t *testing.T) {
	ctx := context.Background()

	bh, err := New(newClientTestStore(t), WithGCInterval(5*time.Millisecond))
	require.NoError(t, err)

	// Only the owner kind has a controller; the child kind is client-only.
	_, err = Register(bh, clientTestGK, &finalizerClearingController{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	owners := NewClient[cSpec, cStatus](bh, clientTestGK)
	childGK := GroupKind{Group: "", Kind: "ClientOnlyChild"}
	children := NewClient[cSpec, cStatus](bh, childGK)

	owner, err := owners.Create(ctx, cSpec{Val: "owner"})
	require.NoError(t, err)
	child, err := children.Create(ctx, cSpec{Val: "child"}, WithOwner(owner.ID))
	require.NoError(t, err)

	// The client rejects watches on unregistered kinds, so watch only the owner.
	// Its deletion is itself proof the sweeper collected the child: the owner
	// can't be physically deleted until the child's owned_by edge is gone, and
	// only the sweeper can collect that client-only child.
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wOwner, err := owners.WatchList(wctx)
	require.NoError(t, err)

	require.NoError(t, owners.Delete(ctx, owner.ID))
	waitForDeletions(t, wOwner, owner.ID)

	_, err = owners.Get(ctx, owner.ID)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = children.Get(ctx, child.ID)
	require.ErrorIs(t, err, ErrNotFound)
}

// TestIntegrationGCSweepCollectsStandaloneClientOnlyDelete is the unowned
// counterpart of TestIntegrationGCSweepsClientOnlyKind: no owner to cascade from
// and no controller to reconcile, so the global sweep is the only thing that can
// collect the row. The delete path itself deliberately only requeues — for a
// client-only kind that is a no-op — which is why the row must still be present
// immediately after Delete and gone after one sweep.
//
// The sweep is called directly rather than waiting on a tick: what is under test is
// that the sweep collects this row, not that Start wires a ticker to it (which
// TestIntegrationGCSweepsClientOnlyKind covers end to end).
func TestIntegrationGCSweepCollectsStandaloneClientOnlyDelete(t *testing.T) {
	ctx := context.Background()

	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)

	// No controller registered for this kind: it is entirely client-only.
	client := NewClient[cSpec, cStatus](bh, GroupKind{Kind: "ClientOnly"})
	obj, err := client.Create(ctx, cSpec{Val: "doomed"})
	require.NoError(t, err)

	require.NoError(t, client.Delete(ctx, obj.ID))
	got, err := client.Get(ctx, obj.ID)
	require.NoError(t, err, "the delete marks the row; collecting it is the sweeper's job")
	require.NotNil(t, got.DeletionRequestedAt, "the row must be deletion-pending for the sweep to find it")

	bh.sweepDeletionPending(ctx)

	_, err = client.Get(ctx, obj.ID)
	require.ErrorIs(t, err, ErrNotFound)
}

// sweepFailStore hands the GC sweep one deletion-pending row of a client-only
// kind (no controller registered, so the sweep collects it itself) and lets the
// embedded collectFakeStore decide how that collect fails.
type sweepFailStore struct {
	collectFakeStore
	rows []Referrer
}

func (s *sweepFailStore) DeletionsListPending(context.Context) ([]Referrer, error) {
	return s.rows, nil
}

// A per-row collect failure must be logged, not swallowed: for a client-only kind
// the sweep is the only collector, so a row that fails every pass would otherwise
// strand silently and RESTRICT-block its owner's delete forever. The one
// exception is ErrNotFound — another path collected the row first, which is the
// benign race and not worth a warning on every sweep.
func TestGCSweepLogsCollectFailure(t *testing.T) {
	ctx := context.Background()
	rows := []Referrer{{ID: 7, Kind: "ClientOnly"}}

	t.Run("real error", func(t *testing.T) {
		logger, buf := captureLogger(slog.LevelWarn)
		store := &sweepFailStore{rows: rows}
		store.markErr = errBoom
		bh, err := New(store, WithLogger(logger))
		require.NoError(t, err)

		bh.sweepDeletionPending(ctx)

		assert.Contains(t, buf.String(), "gc sweep: collecting object failed")
		assert.Contains(t, buf.String(), errBoom.Error())
		assert.Contains(t, buf.String(), "ClientOnly")
	})

	t.Run("already collected", func(t *testing.T) {
		logger, buf := captureLogger(slog.LevelWarn)
		store := &sweepFailStore{rows: rows}
		store.getMetaErr = ErrNotFound
		bh, err := New(store, WithLogger(logger))
		require.NoError(t, err)

		bh.sweepDeletionPending(ctx)

		assert.Empty(t, buf.String())
	})
}

func TestIntegrationGCDeleteDependencyUnblocksTarget(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)

	// Resync disabled: when the dependent releases its depends_on edge, that ref
	// removal must wake the target directly — there's no backstop to re-check it.
	bh, err := New(store, WithResyncInterval(0))
	require.NoError(t, err)

	ctrl := &depDroppingController{}
	_, err = Register(bh, clientTestGK, ctrl)
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	target, err := client.Create(ctx, cSpec{Val: "target"})
	require.NoError(t, err)
	dep, err := client.Create(ctx, cSpec{Val: "dependent"})
	require.NoError(t, err)

	// dep depends_on target (not owned: the dependent survives the target).
	require.NoError(t, addRef(ctx, store, dep.ID, target.ID, RelationDependsOn))

	ctrl.mu.Lock()
	ctrl.reader = client
	ctrl.depID = dep.ID
	ctrl.targetID = target.ID
	ctrl.mu.Unlock()

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := client.WatchList(wctx)
	require.NoError(t, err)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	// Deleting the target wakes the dependent (depends_on waker); the dependent
	// drops the edge, which must then wake the target so GC removes it.
	require.NoError(t, client.Delete(ctx, target.ID))
	waitForDeletions(t, w, target.ID)

	// The dependent is untouched.
	_, err = client.Get(ctx, dep.ID)
	require.NoError(t, err)
}

// TestGCSweepsOnItsOwnInterval pins that garbage collection has a cadence of its
// own, independent of the reconcile knobs. Collecting dead rows and re-confirming
// live ones are different jobs with different costs, and one interval governing
// both means tuning either moves the other — with the sharp edge that disabling
// the reconcile tick silently disabled the GC sweeper too, stranding rows whose
// owned_by edge then RESTRICT-blocks the owner forever.
//
// The row is marked deletion-pending only after the sweeper's startup pass has
// provably run, and through the store rather than the client, so neither that
// pass nor advanceGC's post-Delete wake can be what collects it: a periodic sweep
// is the only path left. The kind has no registered controller, so nothing
// dispatches a reconcile either.
func TestGCSweepsOnItsOwnInterval(t *testing.T) {
	ctx := context.Background()
	real := newClientTestStore(t)
	store := &listProbeStore{Store: real, gcSwept: make(chan struct{}, 8)}

	raw, err := real.ObjectsCreate(ctx, &RawObject{
		Group: clientTestGK.Group, Kind: clientTestGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	// Reconcile tick off, GC on: the sweeper must still run on its own timer.
	bh, err := New(store, WithResyncInterval(0), WithGCInterval(10*time.Millisecond))
	require.NoError(t, err)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	select {
	case <-store.gcSwept:
	case <-time.After(testTimeout):
		t.Fatal("sweeper never ran its startup pass")
	}

	_, _, err = real.DeletionsRequest(ctx, clientTestGK, raw.ID)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, err := real.ObjectsGetMeta(ctx, raw.ID)
		return errors.Is(err, ErrNotFound)
	}, testTimeout, 5*time.Millisecond, "deletion-pending row was never collected: GC is still riding the reconcile interval")
}

// TestGCSweepDispatchesRegisteredKind pins that the GC sweep *routes* rather than
// only collecting. collect cannot clear a finalizer — it cascades, then returns
// while any remain, because releasing a finalizer is the controller's decision.
// So for a registered kind the sweep has to enqueue the object and let its
// reconcile loop run the controller; calling collect directly makes no progress,
// forever.
//
// Every other driver is removed: resync is off, the startup pass is None, and the
// row is marked deletion-pending only after both startup listings have provably
// run — so neither the reconciler's own startup enqueue nor the sweeper's startup
// pass can be what dispatches it. A periodic GC sweep is the only path left.
func TestGCSweepDispatchesRegisteredKind(t *testing.T) {
	ctx := context.Background()
	real := newClientTestStore(t)
	store := &listProbeStore{
		Store:      real,
		wakeListed: make(chan struct{}, 8),
		gcSwept:    make(chan struct{}, 8),
	}

	bh, err := New(store, WithResyncInterval(0), WithGCInterval(10*time.Millisecond))
	require.NoError(t, err)
	// The controller clears "gate" once the object is finalizing, which is the step
	// only a reconcile can take.
	_, err = Register(bh, clientTestGK, &finalizerClearingController{finalizer: "gate"},
		WithStartupResync(false))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj, err := client.Create(ctx, cSpec{Val: "a"}, WithFinalizers("gate"))
	require.NoError(t, err)

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := client.WatchList(wctx)
	require.NoError(t, err)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer stop(ctx)

	for _, probe := range []struct {
		name string
		ch   chan struct{}
	}{
		{"reconciler startup enqueue", store.wakeListed},
		{"gc sweeper startup pass", store.gcSwept},
	} {
		select {
		case <-probe.ch:
		case <-time.After(testTimeout):
			t.Fatalf("%s never ran", probe.name)
		}
	}

	// Mark it deletion-pending through the store, so the client's own advanceGC
	// wake isn't what drives this either.
	_, _, err = real.DeletionsRequest(ctx, clientTestGK, obj.ID)
	require.NoError(t, err)

	waitForDeletions(t, w, obj.ID)
}
