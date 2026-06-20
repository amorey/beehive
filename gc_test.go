package beehive

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// depDroppingController models a dependent that outlives its target: it depends
// on targetID and, the moment that target starts finalizing, releases the edge
// (DeleteDependency) so the target can be collected. The dependent itself is
// never deleted.
type depDroppingController struct {
	mu       sync.Mutex
	cc       ControllerClient[cStatus]
	reader   Client[cSpec, cStatus]
	depID    ObjectID
	targetID ObjectID
}

func (c *depDroppingController) Start(cc ControllerClient[cStatus]) error {
	c.mu.Lock()
	c.cc = cc
	c.mu.Unlock()
	return nil
}
func (c *depDroppingController) Stop(context.Context) error { return nil }
func (c *depDroppingController) Reconcile(ctx context.Context, obj *Object[cSpec, cStatus]) (Result, error) {
	c.mu.Lock()
	cc, reader, depID, targetID := c.cc, c.reader, c.depID, c.targetID
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
		return Result{}, cc.DeleteDependency(ctx, depID, targetID)
	}
	return Result{}, nil
}

// finalizerClearingController clears finalizer (if it holds one) the moment an
// object is finalizing, so GC can then remove the row. With no finalizer it is a
// pure no-op reconciler — exactly what a cascade-only owner needs.
type finalizerClearingController struct {
	finalizer string // empty => never clears anything

	mu     sync.Mutex
	client ControllerClient[cStatus]
}

func (c *finalizerClearingController) Start(client ControllerClient[cStatus]) error {
	c.mu.Lock()
	c.client = client
	c.mu.Unlock()
	return nil
}
func (c *finalizerClearingController) Stop(context.Context) error { return nil }
func (c *finalizerClearingController) Reconcile(ctx context.Context, obj *Object[cSpec, cStatus]) (Result, error) {
	if obj.DeletionRequestedAt == nil || c.finalizer == "" {
		return Result{}, nil
	}
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	for _, f := range obj.Finalizers {
		if f == c.finalizer {
			return Result{}, client.DeleteFinalizer(ctx, obj.ID, c.finalizer)
		}
	}
	return Result{}, nil
}

// hasReferrersGatingController models the documented finalizer workflow: an
// object holding `finalizer` clears it only once HasReferrers reports no live
// claim, so a shared resource outlives its last real user. Objects that don't
// hold the finalizer are left for GC directly.
type hasReferrersGatingController struct {
	finalizer string

	mu sync.Mutex
	cc ControllerClient[cStatus]
}

func (c *hasReferrersGatingController) Start(cc ControllerClient[cStatus]) error {
	c.mu.Lock()
	c.cc = cc
	c.mu.Unlock()
	return nil
}
func (c *hasReferrersGatingController) Stop(context.Context) error { return nil }
func (c *hasReferrersGatingController) Reconcile(ctx context.Context, obj *Object[cSpec, cStatus]) (Result, error) {
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
	c.mu.Lock()
	cc := c.cc
	c.mu.Unlock()
	referenced, err := cc.HasReferrers(ctx, obj.ID)
	if err != nil || referenced {
		return Result{}, err // a live user remains; keep the finalizer
	}
	return Result{}, cc.DeleteFinalizer(ctx, obj.ID, c.finalizer)
}

// waitForDeletions consumes w until it has seen a Deleted event for every id in
// want, failing on timeout. The watcher must be subscribed before the deletions
// are triggered so no event is missed.
func waitForDeletions(t *testing.T, w <-chan WatchEvent[cSpec, cStatus], want ...ObjectID) {
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
			if ev.Type == WatchEventDeleted {
				delete(pending, ev.Object.ID)
			}
		case <-timeout:
			t.Fatalf("timed out waiting for deletions; still pending: %v", pending)
		}
	}
}

// gcFixture builds a Beehive over a real sqlite store plus a client, so collect
// tests can exercise real RequestDeletion/DeleteObject/ref semantics. No
// controller is started: collect is driven directly.
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
	require.NoError(t, bh.store.AddRef(ctx, obj.ID, obj.ID, RelationDependsOn))
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
	require.NoError(t, Register(bh, clientTestGK, &finalizerClearingController{}))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	a, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)
	b, err := client.Create(ctx, cSpec{Val: "b"})
	require.NoError(t, err)
	// A and B depend on each other: neither can be collected until the cycle breaks.
	require.NoError(t, store.AddRef(ctx, a.ID, b.ID, RelationDependsOn))
	require.NoError(t, store.AddRef(ctx, b.ID, a.ID, RelationDependsOn))

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := client.WatchList(wctx)
	require.NoError(t, err)

	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

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
	require.NoError(t, Register(bh, clientTestGK, &hasReferrersGatingController{finalizer: "gate"}))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	obj, err := client.Create(ctx, cSpec{Val: "self"}, WithFinalizers("gate"))
	require.NoError(t, err)
	// A finalizing dependent that points at obj — modeled as a self-dependency, so
	// the referrer is itself deletion-pending the instant obj is deleted. Without
	// the fix, the gate sees this edge, never clears the finalizer, and GC stalls.
	require.NoError(t, store.AddRef(ctx, obj.ID, obj.ID, RelationDependsOn))

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := client.WatchList(wctx)
	require.NoError(t, err)

	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

	require.NoError(t, client.Delete(ctx, obj.ID))
	waitForDeletions(t, w, obj.ID)
}

func TestIntegrationGCResumesDanglingDeleteOnStartup(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)

	// Simulate a crash mid-delete: a deletion-pending row is already in the durable
	// store before any control plane runs. (Written through the store directly, so
	// no reconcile has touched it.)
	raw, err := store.CreateObject(ctx, &RawObject{
		Group: clientTestGK.Group, Kind: clientTestGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	_, _, err = store.RequestDeletion(ctx, raw.ID)
	require.NoError(t, err)

	// A fresh Beehive with no spec-startup pass and resync disabled: the startup
	// enqueueDeletionPending is the only thing that can drive this row to removal.
	bh, err := New(store, WithResyncInterval(0))
	require.NoError(t, err)
	require.NoError(t, Register(bh, clientTestGK, &finalizerClearingController{},
		WithStartupReconcileStrategy(StartupReconcileNone)))

	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	// Subscribe before Start so the Deleted event can't be missed.
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := client.WatchList(wctx)
	require.NoError(t, err)

	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

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

	require.NoError(t, Register(bh, clientTestGK, &finalizerClearingController{finalizer: "f"}))
	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

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

	require.NoError(t, Register(bh, clientTestGK, &finalizerClearingController{}))
	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

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
	bh, err := New(newClientTestStore(t), WithResyncInterval(5*time.Millisecond))
	require.NoError(t, err)

	require.NoError(t, Register(bh, clientTestGK, &finalizerClearingController{}))
	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

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

func TestIntegrationGCDeleteDependencyUnblocksTarget(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)

	// Resync disabled: when the dependent releases its depends_on edge, that ref
	// removal must wake the target directly — there's no backstop to re-check it.
	bh, err := New(store, WithResyncInterval(0))
	require.NoError(t, err)

	ctrl := &depDroppingController{}
	require.NoError(t, Register(bh, clientTestGK, ctrl))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	target, err := client.Create(ctx, cSpec{Val: "target"})
	require.NoError(t, err)
	dep, err := client.Create(ctx, cSpec{Val: "dependent"})
	require.NoError(t, err)

	// dep depends_on target (not owned: the dependent survives the target).
	require.NoError(t, store.AddRef(ctx, dep.ID, target.ID, RelationDependsOn))

	ctrl.mu.Lock()
	ctrl.reader = client
	ctrl.depID = dep.ID
	ctrl.targetID = target.ID
	ctrl.mu.Unlock()

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := client.WatchList(wctx)
	require.NoError(t, err)

	require.NoError(t, bh.Start())
	defer bh.Stop(ctx)

	// Deleting the target wakes the dependent (depends_on waker); the dependent
	// drops the edge, which must then wake the target so GC removes it.
	require.NoError(t, client.Delete(ctx, target.ID))
	waitForDeletions(t, w, target.ID)

	// The dependent is untouched.
	_, err = client.Get(ctx, dep.ID)
	require.NoError(t, err)
}
