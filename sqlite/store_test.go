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

package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/internal/storeapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testGK = beehive.GroupKind{Group: "", Kind: "Greeting"}

func newTestStore(t *testing.T) beehive.Store {
	t.Helper()
	store, err := OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	return store
}

func TestCreateObjectAssignsIdentity(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	obj, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group,
		Kind:  testGK.Kind,
		Slug:  new("world"),
		Spec:  []byte(`{"name":"world"}`),
	})
	require.NoError(t, err)

	assert.NotZero(t, obj.ID)
	assert.EqualValues(t, 1, obj.Generation, "generation starts at 1")
	assert.NotZero(t, obj.ResourceVersion)
	assert.Nil(t, obj.Status, "status is nil until first write")
	assert.Nil(t, obj.ObservedGeneration)
	assert.Empty(t, obj.Finalizers)
	assert.False(t, obj.CreatedAt.IsZero())
	assert.Equal(t, obj.CreatedAt, obj.UpdatedAt)
	require.NotNil(t, obj.Slug)
	assert.Equal(t, "world", *obj.Slug)
}

func TestCreateObjectPersistsFinalizers(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	want := []string{"kstack.sh/cluster", "kstack.sh/dns"}
	created, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Slug: new("guarded"),
		Spec: []byte(`{}`), Finalizers: want,
	})
	require.NoError(t, err)
	assert.Equal(t, want, created.Finalizers)

	// Round-trips through the JSON column, not just the returned struct.
	reloaded, err := store.GetObject(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, want, reloaded.Finalizers)
}

func TestGetByIdAndSlug(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Slug: new("world"),
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	byID, err := store.GetObject(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, created.ID, byID.ID)

	byName, err := store.GetObjectBySlug(ctx, testGK, "world")
	require.NoError(t, err)
	assert.Equal(t, created.ID, byName.ID)
}

func TestGetNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.GetObject(ctx, 999)
	assert.ErrorIs(t, err, beehive.ErrNotFound)

	_, err = store.GetObjectBySlug(ctx, testGK, "nope")
	assert.ErrorIs(t, err, beehive.ErrNotFound)
}

func TestDuplicateSlugRejected(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	mk := func() error {
		_, err := store.CreateObject(ctx, &beehive.RawObject{
			Group: testGK.Group, Kind: testGK.Kind, Slug: new("dup"),
			Spec: []byte(`{}`),
		})
		return err
	}
	require.NoError(t, mk())
	assert.Error(t, mk(), "second create with same slug should violate UNIQUE")
}

func TestUnnamedObjectsCoexist(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	mk := func() *beehive.RawObject {
		obj, err := store.CreateObject(ctx, &beehive.RawObject{
			Group: testGK.Group, Kind: testGK.Kind, // Slug nil
			Spec: []byte(`{}`),
		})
		require.NoError(t, err)
		assert.Nil(t, obj.Slug)
		return obj
	}
	// SQLite treats NULL != NULL, so multiple unnamed objects are allowed.
	a, b := mk(), mk()
	assert.NotEqual(t, a.ID, b.ID)
}

func TestUpdateSpecBumpsGeneration(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{"v":1}`),
	})
	require.NoError(t, err)

	updated, err := store.UpdateSpec(ctx, testGK, created.ID, []byte(`{"v":2}`))
	require.NoError(t, err)

	assert.EqualValues(t, 2, updated.Generation, "spec change bumps generation")
	assert.Greater(t, updated.ResourceVersion, created.ResourceVersion)
	assert.JSONEq(t, `{"v":2}`, string(updated.Spec))
}

// TestUpdateSpecIdenticalSpecIsNoOp verifies that re-writing the same spec bytes
// doesn't bump generation or resource_version: an idempotent update must not
// falsely unsettle a converged object or churn the watch cursor.
func TestUpdateSpecIdenticalSpecIsNoOp(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{"v":1}`),
	})
	require.NoError(t, err)

	// Settle the object so observed_generation == generation; an idempotent
	// update must leave it settled.
	settled, err := store.UpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{}`))
	require.NoError(t, err)

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()
	require.Equal(t, beehive.WatchEventAdded, recvEvent(t, w).Type) // snapshot

	again, err := store.UpdateSpec(ctx, testGK, created.ID, []byte(`{"v":1}`))
	require.NoError(t, err)

	assert.EqualValues(t, created.Generation, again.Generation, "identical spec must not bump generation")
	assert.Equal(t, settled.ResourceVersion, again.ResourceVersion, "identical spec must not bump resource_version")
	require.NotNil(t, again.ObservedGeneration)
	assert.EqualValues(t, again.Generation, *again.ObservedGeneration, "object stays settled after a no-op update")
	// No watcher churn: an idempotent update emits no Modified event.
	assertNoEvent(t, w, 100*time.Millisecond)
}

func TestUpdateStatusRecordsObservedGeneration(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	updated, err := store.UpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{"msg":"hi"}`))
	require.NoError(t, err)

	require.NotNil(t, updated.ObservedGeneration)
	assert.EqualValues(t, created.Generation, *updated.ObservedGeneration)
	assert.EqualValues(t, created.Generation, updated.Generation, "status write must not bump generation")
	require.NotNil(t, updated.ObservedAt)
	assert.Greater(t, updated.ResourceVersion, created.ResourceVersion)
	assert.JSONEq(t, `{"msg":"hi"}`, string(updated.Status))
}

func TestUpdateStatusRejectsFutureGeneration(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	// created.Generation is 1; reporting generation 5 is impossible to have seen.
	_, err = store.UpdateStatus(ctx, testGK, created.ID, created.Generation+4, []byte(`{"msg":"hi"}`))
	require.ErrorIs(t, err, beehive.ErrObservedGenerationFuture)

	// The rejected write must not have landed.
	reread, err := store.GetObject(ctx, created.ID)
	require.NoError(t, err)
	assert.Nil(t, reread.ObservedGeneration, "rejected status write must not record observed generation")
	assert.Empty(t, reread.Status, "rejected status write must not store status")
}

func TestUpdateStatusAcceptsStaleGeneration(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	bumped, err := store.UpdateSpec(ctx, testGK, created.ID, []byte(`{"x":1}`))
	require.NoError(t, err)
	require.EqualValues(t, 2, bumped.Generation)

	// Controller reports it reconciled the now-stale generation 1.
	updated, err := store.UpdateStatus(ctx, testGK, created.ID, created.Generation, []byte(`{}`))
	require.NoError(t, err)
	require.NotNil(t, updated.ObservedGeneration)
	assert.EqualValues(t, created.Generation, *updated.ObservedGeneration)
	assert.Less(t, *updated.ObservedGeneration, updated.Generation,
		"stale observed generation must leave the object unsettled")
}

func TestListObjects(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	for _, n := range []string{"a", "b", "c"} {
		_, err := store.CreateObject(ctx, &beehive.RawObject{
			Group: testGK.Group, Kind: testGK.Kind, Slug: new(n),
			Spec: []byte(`{}`),
		})
		require.NoError(t, err)
	}
	// A different kind must not leak into the list.
	_, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: "", Kind: "Other", Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	list, err := store.ListObjects(ctx, testGK)
	require.NoError(t, err)
	require.Len(t, list, 3)

	var names []string
	for _, o := range list {
		require.NotNil(t, o.Slug)
		names = append(names, *o.Slug)
	}
	assert.Equal(t, []string{"a", "b", "c"}, names, "ordered by id")
}

func TestResourceVersionIsMonotonic(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	a, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Slug: new("a"), Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	b, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Slug: new("b"), Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	assert.Greater(t, b.ResourceVersion, a.ResourceVersion, "each create takes the next cursor value")

	// A later mutation advances the global cursor past every prior write.
	updated, err := store.UpdateSpec(ctx, testGK, a.ID, []byte(`{"v":2}`))
	require.NoError(t, err)
	assert.Greater(t, updated.ResourceVersion, b.ResourceVersion)
}

func TestResourceVersionNotReusedAfterDelete(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	a, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Slug: new("a"), Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	b, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Slug: new("b"), Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	// Delete the highest-versioned row, then write again. The cursor must not
	// fall back to b's version — it only ever moves forward.
	require.NoError(t, store.DeleteObject(ctx, b.ID))

	updated, err := store.UpdateSpec(ctx, testGK, a.ID, []byte(`{"v":2}`))
	require.NoError(t, err)
	assert.Greater(t, updated.ResourceVersion, b.ResourceVersion,
		"a deleted row's resource_version must never be reused")
}

func TestRepeatRequestDeletionDoesNotBumpResourceVersion(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	first, changed, err := store.RequestDeletion(ctx, testGK, created.ID)
	require.NoError(t, err)
	assert.True(t, changed, "first call is a real change")
	assert.Greater(t, first.ResourceVersion, created.ResourceVersion,
		"the first request is a real change and bumps the cursor")

	// A repeat request changes no deletion state, so it must be a no-op: same
	// resource_version, same updated_at, no spurious watch/CAS churn.
	second, changed, err := store.RequestDeletion(ctx, testGK, created.ID)
	require.NoError(t, err)
	assert.False(t, changed, "repeat call is an idempotent no-op")
	assert.Equal(t, first.ResourceVersion, second.ResourceVersion,
		"an idempotent repeat must not bump resource_version")
	assert.Equal(t, first.UpdatedAt, second.UpdatedAt)
}

// GetObjectMeta returns the same row as GetObject but skips assembling conditions
// (the over-fetch the GC/ref metadata-only callers used to pay).
func TestGetObjectMetaSkipsConditions(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	_, err = store.SetCondition(ctx, testGK, created.ID,
		storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	full, err := store.GetObject(ctx, created.ID)
	require.NoError(t, err)
	require.Len(t, full.Conditions, 1, "GetObject assembles conditions")

	meta, err := store.GetObjectMeta(ctx, created.ID)
	require.NoError(t, err)
	assert.Nil(t, meta.Conditions, "GetObjectMeta must not assemble conditions")
	// Otherwise the same row: id and version match the conditions-laden read.
	assert.Equal(t, full.ID, meta.ID)
	assert.Equal(t, full.ResourceVersion, meta.ResourceVersion)
}

// MarkOwnedForDeletion marks every owned child for deletion and returns them all;
// a re-cascade over already-deleting children writes nothing (the O(1) steady
// state) yet still returns them for requeue.
func TestMarkOwnedForDeletionCascadesThenIsNoOp(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	mk := func() storeapi.ObjectID {
		o, err := store.CreateObject(ctx, &beehive.RawObject{
			Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
		})
		require.NoError(t, err)
		return o.ID
	}
	owner, childA, childB := mk(), mk(), mk()
	require.NoError(t, store.AddRef(ctx, childA, owner, beehive.RelationOwnedBy))
	require.NoError(t, store.AddRef(ctx, childB, owner, beehive.RelationOwnedBy))

	// Watch live changes only (no snapshot) so each cascade's events are isolated.
	w, err := store.WatchEvents(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()

	// First cascade marks both children (a Modified each) and returns both.
	got, err := store.MarkOwnedForDeletion(ctx, owner)
	require.NoError(t, err)
	require.Len(t, got, 2)
	for i := 0; i < 2; i++ {
		assert.Equal(t, beehive.WatchEventModified, recvEvent(t, w).Type)
	}
	a1, err := store.GetObjectMeta(ctx, childA)
	require.NoError(t, err)
	require.NotNil(t, a1.DeletionRequestedAt, "child A marked for deletion")
	b1, err := store.GetObjectMeta(ctx, childB)
	require.NoError(t, err)
	require.NotNil(t, b1.DeletionRequestedAt, "child B marked for deletion")

	// Second cascade over the now-deleting children: still returns both, but writes
	// nothing and emits nothing — no resource_version churn, no events.
	got2, err := store.MarkOwnedForDeletion(ctx, owner)
	require.NoError(t, err)
	require.Len(t, got2, 2)
	assertNoEvent(t, w, 100*time.Millisecond)
	a2, err := store.GetObjectMeta(ctx, childA)
	require.NoError(t, err)
	assert.Equal(t, a1.ResourceVersion, a2.ResourceVersion, "no re-mark, no rv churn")
	b2, err := store.GetObjectMeta(ctx, childB)
	require.NoError(t, err)
	assert.Equal(t, b1.ResourceVersion, b2.ResourceVersion)
}

// MarkOwnedForDeletion's child lookup must ride the idx_refs_to index, not scan
// the refs table — that index alignment is the point of the single-query cascade.
func TestMarkOwnedForDeletionUsesRefsIndex(t *testing.T) {
	store := newTestStore(t).(*sqliteStore)
	ctx := context.Background()

	rows, err := store.db.QueryContext(ctx, `
		EXPLAIN QUERY PLAN
		SELECT o.id, o."group", o.kind, o.deletion_requested_at
		FROM refs r JOIN objects o ON o.id = r.from_id
		WHERE r.to_id = ? AND r.relation = ?
		ORDER BY o.id`, int64(1), string(storeapi.RelationOwnedBy))
	require.NoError(t, err)
	defer rows.Close()

	var plan string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notused, &detail))
		plan += detail + "\n"
	}
	require.NoError(t, rows.Err())
	assert.Contains(t, plan, "idx_refs_to", "child lookup must use idx_refs_to:\n"+plan)
}

func TestDeleteFinalizerRemovesOneAndEmits(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
		Finalizers: []string{"a", "b"},
	})
	require.NoError(t, err)

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()
	require.Equal(t, beehive.WatchEventAdded, recvEvent(t, w).Type) // snapshot

	// Removing a present finalizer is a real change: only that finalizer drops,
	// resource_version bumps, and watchers see a Modified event.
	got, err := store.DeleteFinalizer(ctx, testGK, created.ID, "a")
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, got.Finalizers)
	assert.Greater(t, got.ResourceVersion, created.ResourceVersion)

	ev := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventModified, ev.Type)
	assert.Equal(t, []string{"b"}, ev.Object.Finalizers)

	// Persisted, not just reflected in the returned struct.
	reloaded, err := store.GetObject(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, reloaded.Finalizers)
}

func TestDeleteFinalizerAbsentIsNoOp(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
		Finalizers: []string{"a"},
	})
	require.NoError(t, err)

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()
	require.Equal(t, beehive.WatchEventAdded, recvEvent(t, w).Type) // snapshot

	// Removing a finalizer that isn't present changes nothing: the list is intact,
	// resource_version is unbumped, and no event fires (a watcher would otherwise
	// see a spurious diff).
	got, err := store.DeleteFinalizer(ctx, testGK, created.ID, "missing")
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, got.Finalizers)
	assert.Equal(t, created.ResourceVersion, got.ResourceVersion)
	assertNoEvent(t, w, 100*time.Millisecond)
}

func TestDeleteFinalizerMissingObject(t *testing.T) {
	store := newTestStore(t)
	_, err := store.DeleteFinalizer(context.Background(), testGK, 999, "a")
	assert.ErrorIs(t, err, beehive.ErrNotFound)
}

func TestListDeletionPendingIDs(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// alive: deletion never requested — must NOT appear.
	_, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	// pendingA and pendingB: deletion requested — must appear, ordered by id.
	pendingA, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	pendingB, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	_, _, err = store.RequestDeletion(ctx, testGK, pendingA.ID)
	require.NoError(t, err)
	_, _, err = store.RequestDeletion(ctx, testGK, pendingB.ID)
	require.NoError(t, err)

	// A deleting object of another kind must not leak into this kind's listing.
	otherGK := beehive.GroupKind{Group: "", Kind: "Other"}
	other, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: otherGK.Group, Kind: otherGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	_, _, err = store.RequestDeletion(ctx, otherGK, other.ID)
	require.NoError(t, err)

	ids, err := store.ListDeletionPendingIDs(ctx, testGK)
	require.NoError(t, err)
	assert.Equal(t, []beehive.ObjectID{pendingA.ID, pendingB.ID}, ids)
}

func TestListOutgoingRefs(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	from := newRefObject(t, store)
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	require.NoError(t, store.AddRef(ctx, from.ID, a.ID, beehive.RelationOwnedBy))
	require.NoError(t, store.AddRef(ctx, from.ID, b.ID, beehive.RelationDependsOn))
	// A second edge to the same target via another relation must not duplicate it.
	require.NoError(t, store.AddRef(ctx, from.ID, a.ID, beehive.RelationDependsOn))

	refs, err := store.ListOutgoingRefs(ctx, from.ID)
	require.NoError(t, err)
	var ids []beehive.ObjectID
	for _, r := range refs {
		ids = append(ids, r.ID)
	}
	assert.Equal(t, []beehive.ObjectID{a.ID, b.ID}, ids, "distinct targets, ordered by id")

	// An object that points at nothing has no referents.
	refs, err = store.ListOutgoingRefs(ctx, a.ID)
	require.NoError(t, err)
	assert.Empty(t, refs)
}

func TestDeleteFinalizingDependsOnRefs(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	target := newRefObject(t, store)
	deletingDep := newRefObject(t, store)
	liveDep := newRefObject(t, store)
	owned := newRefObject(t, store)

	require.NoError(t, store.AddRef(ctx, deletingDep.ID, target.ID, beehive.RelationDependsOn))
	require.NoError(t, store.AddRef(ctx, liveDep.ID, target.ID, beehive.RelationDependsOn))
	require.NoError(t, store.AddRef(ctx, owned.ID, target.ID, beehive.RelationOwnedBy))
	// A self-dependency the GC must also be able to clear.
	require.NoError(t, store.AddRef(ctx, target.ID, target.ID, beehive.RelationDependsOn))

	// The target and the finalizing dependent and the owned child are deleting;
	// the live dependent is not.
	for _, id := range []beehive.ObjectID{target.ID, deletingDep.ID, owned.ID} {
		_, _, err := store.RequestDeletion(ctx, testGK, id)
		require.NoError(t, err)
	}

	require.NoError(t, store.DeleteFinalizingDependsOnRefs(ctx, target.ID))

	// depends_on edges from finalizing sources (including the self-edge) are gone.
	assert.Equal(t, 0, countRefs(t, store, deletingDep.ID, target.ID, "depends_on"))
	assert.Equal(t, 0, countRefs(t, store, target.ID, target.ID, "depends_on"))
	// A live dependent's edge is preserved — it still legitimately blocks deletion.
	assert.Equal(t, 1, countRefs(t, store, liveDep.ID, target.ID, "depends_on"))
	// owned_by is never touched here; it clears only when the child is removed.
	assert.Equal(t, 1, countRefs(t, store, owned.ID, target.ID, "owned_by"))
}

func TestHasIncomingRefs(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	owner := newRefObject(t, store)
	child := newRefObject(t, store)

	has, err := store.HasIncomingRefs(ctx, owner.ID)
	require.NoError(t, err)
	assert.False(t, has, "no edges yet")

	require.NoError(t, store.AddRef(ctx, child.ID, owner.ID, beehive.RelationOwnedBy))

	has, err = store.HasIncomingRefs(ctx, owner.ID)
	require.NoError(t, err)
	assert.True(t, has, "owner is referenced by the child")

	has, err = store.HasIncomingRefs(ctx, child.ID)
	require.NoError(t, err)
	assert.False(t, has, "child is the source, not a target")
}

func TestHasIncomingRefsIgnoresFinalizingDependent(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	target := newRefObject(t, store)
	dep := newRefObject(t, store)
	require.NoError(t, store.AddRef(ctx, dep.ID, target.ID, beehive.RelationDependsOn))

	// A live dependent has a claim: it counts.
	has, err := store.HasIncomingRefs(ctx, target.ID)
	require.NoError(t, err)
	assert.True(t, has)

	// Once the dependent is itself finalizing, its claim is void — it's going away.
	_, _, err = store.RequestDeletion(ctx, testGK, dep.ID)
	require.NoError(t, err)
	has, err = store.HasIncomingRefs(ctx, target.ID)
	require.NoError(t, err)
	assert.False(t, has, "a finalizing dependent does not count as a referrer")

	// But a finalizing owned child still counts: the foreground cascade must wait
	// for it to be physically removed.
	child := newRefObject(t, store)
	require.NoError(t, store.AddRef(ctx, child.ID, target.ID, beehive.RelationOwnedBy))
	_, _, err = store.RequestDeletion(ctx, testGK, child.ID)
	require.NoError(t, err)
	has, err = store.HasIncomingRefs(ctx, target.ID)
	require.NoError(t, err)
	assert.True(t, has, "a finalizing owned child still blocks deletion")
}

func TestMutatorsReturnNotFoundForMissingID(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	const missing beehive.ObjectID = 999

	ops := map[string]func() error{
		"UpdateSpec": func() error {
			_, err := store.UpdateSpec(ctx, testGK, missing, []byte(`{}`))
			return err
		},
		"UpdateStatus": func() error {
			_, err := store.UpdateStatus(ctx, testGK, missing, 1, []byte(`{}`))
			return err
		},
		"RequestDeletion": func() error {
			_, _, err := store.RequestDeletion(ctx, testGK, missing)
			return err
		},
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			assert.ErrorIs(t, op(), beehive.ErrNotFound)
		})
	}
}

func TestRequestDeletionIsIdempotent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	first, _, err := store.RequestDeletion(ctx, testGK, created.ID)
	require.NoError(t, err)
	require.NotNil(t, first.DeletionRequestedAt)

	second, _, err := store.RequestDeletion(ctx, testGK, created.ID)
	require.NoError(t, err)
	require.NotNil(t, second.DeletionRequestedAt)
	assert.Equal(t, *first.DeletionRequestedAt, *second.DeletionRequestedAt,
		"deletion timestamp is stamped once and not moved by requeues")
}

func TestDeleteObject(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	require.NoError(t, store.DeleteObject(ctx, created.ID))

	_, err = store.GetObject(ctx, created.ID)
	assert.ErrorIs(t, err, beehive.ErrNotFound)

	assert.ErrorIs(t, store.DeleteObject(ctx, created.ID), beehive.ErrNotFound,
		"deleting a missing row reports not found")
}

func TestWithinCommitsAndRollsBack(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Commit: writes inside a successful Within are visible afterward.
	var committedID beehive.ObjectID
	require.NoError(t, store.Within(ctx, func(ctx context.Context) error {
		obj, err := store.CreateObject(ctx, &beehive.RawObject{
			Group: testGK.Group, Kind: testGK.Kind, Slug: new("committed"),
			Spec: []byte(`{}`),
		})
		if err != nil {
			return err
		}
		committedID = obj.ID
		return nil
	}))
	_, err := store.GetObject(ctx, committedID)
	assert.NoError(t, err)

	// Rollback: a non-nil error discards every write in the transaction.
	sentinel := errors.New("boom")
	err = store.Within(ctx, func(ctx context.Context) error {
		_, err := store.CreateObject(ctx, &beehive.RawObject{
			Group: testGK.Group, Kind: testGK.Kind, Slug: new("rolledback"),
			Spec: []byte(`{}`),
		})
		require.NoError(t, err)
		return sentinel
	})
	assert.ErrorIs(t, err, sentinel)
	_, err = store.GetObjectBySlug(ctx, testGK, "rolledback")
	assert.ErrorIs(t, err, beehive.ErrNotFound, "rolled-back write must not persist")
}

func TestListUnsettledIDs(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	otherGK := beehive.GroupKind{Group: "", Kind: "Other"}

	// settled: ObservedGeneration == Generation — must NOT appear
	settled, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	_, err = store.UpdateStatus(ctx, testGK, settled.ID, settled.Generation, []byte(`{}`))
	require.NoError(t, err)

	// unsettled: ObservedGeneration is nil — must appear
	nilObs, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	// unsettled: ObservedGeneration < Generation (spec changed after reconcile) — must appear
	stale, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	_, err = store.UpdateStatus(ctx, testGK, stale.ID, stale.Generation, []byte(`{}`))
	require.NoError(t, err)
	_, err = store.UpdateSpec(ctx, testGK, stale.ID, []byte(`{"updated":true}`))
	require.NoError(t, err)

	// different kind — must NOT appear
	_, err = store.CreateObject(ctx, &beehive.RawObject{
		Group: otherGK.Group, Kind: otherGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	ids, err := store.ListUnsettledIDs(ctx, testGK)
	require.NoError(t, err)
	assert.Equal(t, []beehive.ObjectID{nilObs.ID, stale.ID}, ids)

	// ListIDs returns every object of the kind, settled or not, ordered by id.
	all, err := store.ListIDs(ctx, testGK)
	require.NoError(t, err)
	assert.Equal(t, []beehive.ObjectID{settled.ID, nilObs.ID, stale.ID}, all)
}

func TestListIDsQueryError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	_, err := store.ListIDs(context.Background(), testGK)
	require.Error(t, err)
}

func TestNestedWithinJoinsOuterTransaction(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// The inner Within returns nil — if it opened and committed its own
	// transaction the write would survive the outer rollback below.
	sentinel := errors.New("outer boom")
	err := store.Within(ctx, func(ctx context.Context) error {
		if err := store.Within(ctx, func(ctx context.Context) error {
			_, err := store.CreateObject(ctx, &beehive.RawObject{
				Group: testGK.Group, Kind: testGK.Kind, Slug: new("nested"),
				Spec: []byte(`{}`),
			})
			return err
		}); err != nil {
			return err
		}
		return sentinel
	})
	assert.ErrorIs(t, err, sentinel)

	_, err = store.GetObjectBySlug(ctx, testGK, "nested")
	assert.ErrorIs(t, err, beehive.ErrNotFound,
		"nested Within joins the outer tx, so the outer rollback discards its write")
}

// newRawStore returns a *sqliteStore directly so tests can close store.db to
// force database errors on subsequent calls.
func newRawStore(t *testing.T) *sqliteStore {
	t.Helper()
	store, err := OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() }) // Close is a no-op after db.Close()
	return store
}

// insertBadFinalizersRow inserts a row with invalid finalizers JSON directly so
// scanObject's json.Unmarshal step fails when the row is read back.
func insertBadFinalizersRow(t *testing.T, store *sqliteStore, gk beehive.GroupKind) beehive.ObjectID {
	t.Helper()
	ctx := context.Background()
	res, err := store.db.ExecContext(ctx, `
		INSERT INTO objects ("group", kind, spec, finalizers, generation, resource_version, created_at, updated_at)
		VALUES (?, ?, '{}', 'not-valid-json', 1, 999999, 0, 0)`,
		gk.Group, gk.Kind)
	require.NoError(t, err)
	id, err := res.LastInsertId()
	require.NoError(t, err)
	return beehive.ObjectID(id)
}

func TestWithinBeginTxError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	err := store.Within(context.Background(), func(context.Context) error { return nil })
	require.Error(t, err)
}

func TestCreateObjectDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	_, err := store.CreateObject(context.Background(), &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.Error(t, err)
}

func TestListObjectsQueryError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	_, err := store.ListObjects(context.Background(), testGK)
	require.Error(t, err)
}

func TestListObjectsScanError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	// Create a valid object, then corrupt its finalizers so scanObject fails.
	created, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	_, err = store.db.ExecContext(ctx,
		`UPDATE objects SET finalizers = 'not-valid-json' WHERE id = ?`, created.ID)
	require.NoError(t, err)

	_, err = store.ListObjects(ctx, testGK)
	require.Error(t, err)
}

func TestListUnsettledIDsQueryError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	_, err := store.ListUnsettledIDs(context.Background(), testGK)
	require.Error(t, err)
}

func TestUpdateSpecDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	_, err := store.UpdateSpec(context.Background(), testGK, 1, []byte(`{}`))
	require.Error(t, err)
}

func TestUpdateStatusDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	_, err := store.UpdateStatus(context.Background(), testGK, 1, 1, []byte(`{}`))
	require.Error(t, err)
}

func TestRequestDeletionDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	_, _, err := store.RequestDeletion(context.Background(), testGK, 1)
	require.Error(t, err)
}

func TestRequestDeletionScanError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	// Insert a row with bad finalizers JSON and no deletion_requested_at.
	// RequestDeletion will UPDATE it (WHERE deletion_requested_at IS NULL matches),
	// the RETURNING clause gives us the row, and scanObject fails on bad finalizers.
	id := insertBadFinalizersRow(t, store, testGK)

	_, _, err := store.RequestDeletion(ctx, testGK, id)
	require.Error(t, err)
}

func TestDeleteObjectDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()

	err := store.DeleteObject(context.Background(), 1)
	require.Error(t, err)
}

func TestScanObjectBadFinalizersJSON(t *testing.T) {
	store := newRawStore(t)
	id := insertBadFinalizersRow(t, store, testGK)

	_, err := store.GetObject(context.Background(), id)
	require.Error(t, err)
}

func TestWithinNestedCommitError(t *testing.T) {
	// A nested Within with a non-nil error from fn propagates through the outer.
	store := newRawStore(t)
	ctx := context.Background()

	sentinel := errors.New("inner error")
	err := store.Within(ctx, func(ctx context.Context) error {
		return store.Within(ctx, func(context.Context) error {
			return sentinel
		})
	})
	assert.ErrorIs(t, err, sentinel)
}

// newRefObject creates a bare object of testGK and returns it. Refs only need
// ids, so no name/spec detail matters.
func newRefObject(t *testing.T, store beehive.Store) *beehive.RawObject {
	t.Helper()
	obj, err := store.CreateObject(context.Background(), &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	return obj
}

// countRefs reads the refs table directly to assert edge presence.
func countRefs(t *testing.T, store *sqliteStore, from, to beehive.ObjectID, relation string) int {
	t.Helper()
	var n int
	require.NoError(t, store.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM refs WHERE from_id = ? AND to_id = ? AND relation = ?`,
		from, to, relation).Scan(&n))
	return n
}

func TestAddRefInsertsRow(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	require.NoError(t, store.AddRef(ctx, a.ID, b.ID, "depends_on"))
	assert.Equal(t, 1, countRefs(t, store, a.ID, b.ID, "depends_on"))
}

func TestAddRefIdempotent(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	require.NoError(t, store.AddRef(ctx, a.ID, b.ID, "depends_on"))
	require.NoError(t, store.AddRef(ctx, a.ID, b.ID, "depends_on"))
	assert.Equal(t, 1, countRefs(t, store, a.ID, b.ID, "depends_on"), "re-adding an identical edge is a no-op")
}

func TestAddRefNonexistentEndpoint(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)

	err := store.AddRef(ctx, a.ID, 9999, "depends_on")
	assert.ErrorIs(t, err, beehive.ErrNotFound, "missing to_id yields ErrNotFound")
	assert.Equal(t, 0, countRefs(t, store, a.ID, 9999, "depends_on"))

	err = store.AddRef(ctx, 9999, a.ID, "depends_on")
	assert.ErrorIs(t, err, beehive.ErrNotFound, "missing from_id yields ErrNotFound")
	assert.Equal(t, 0, countRefs(t, store, 9999, a.ID, "depends_on"))
}

func TestAddRefNoVersionBumpNoEvent(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()
	// Drain the snapshot Added events for the two pre-existing objects.
	require.Equal(t, beehive.WatchEventAdded, recvEvent(t, w).Type)
	require.Equal(t, beehive.WatchEventAdded, recvEvent(t, w).Type)

	require.NoError(t, store.AddRef(ctx, a.ID, b.ID, "depends_on"))
	assertNoEvent(t, w, 200*time.Millisecond)

	gotA, err := store.GetObject(ctx, a.ID)
	require.NoError(t, err)
	assert.Equal(t, a.ResourceVersion, gotA.ResourceVersion, "a ref edge does not bump the from object")
	gotB, err := store.GetObject(ctx, b.ID)
	require.NoError(t, err)
	assert.Equal(t, b.ResourceVersion, gotB.ResourceVersion, "a ref edge does not bump the to object")
}

func TestDeleteRefRemovesRow(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	require.NoError(t, store.AddRef(ctx, a.ID, b.ID, "depends_on"))
	require.NoError(t, store.DeleteRef(ctx, a.ID, b.ID, "depends_on"))
	assert.Equal(t, 0, countRefs(t, store, a.ID, b.ID, "depends_on"))
}

func TestDeleteRefAbsentNoop(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	// No edge exists, and a nonexistent endpoint, are both silent no-ops.
	require.NoError(t, store.DeleteRef(ctx, a.ID, b.ID, "depends_on"))
	require.NoError(t, store.DeleteRef(ctx, a.ID, 9999, "depends_on"))

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()
	require.Equal(t, beehive.WatchEventAdded, recvEvent(t, w).Type)
	require.Equal(t, beehive.WatchEventAdded, recvEvent(t, w).Type)

	require.NoError(t, store.DeleteRef(ctx, a.ID, b.ID, "depends_on"))
	assertNoEvent(t, w, 200*time.Millisecond)
}

func TestAddRefJoinsTransaction(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	require.NoError(t, store.Within(ctx, func(ctx context.Context) error {
		return store.AddRef(ctx, a.ID, b.ID, "depends_on")
	}))
	assert.Equal(t, 1, countRefs(t, store, a.ID, b.ID, "depends_on"), "edge is committed with the transaction")
}

func TestAddRefRollback(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)

	sentinel := errors.New("rollback")
	err := store.Within(ctx, func(ctx context.Context) error {
		if err := store.AddRef(ctx, a.ID, b.ID, "depends_on"); err != nil {
			return err
		}
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)
	assert.Equal(t, 0, countRefs(t, store, a.ID, b.ID, "depends_on"), "the edge rolled back with the transaction")
}

func TestListIncomingRefs(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	a := newRefObject(t, store)
	b := newRefObject(t, store)
	c := newRefObject(t, store)

	require.NoError(t, store.AddRef(ctx, a.ID, c.ID, "depends_on"))
	require.NoError(t, store.AddRef(ctx, b.ID, c.ID, "depends_on"))
	// An owned_by edge to c must not show up under a depends_on query.
	require.NoError(t, store.AddRef(ctx, a.ID, c.ID, "owned_by"))

	deps, err := store.ListIncomingRefs(ctx, c.ID, "depends_on")
	require.NoError(t, err)
	require.Equal(t, []beehive.Referrer{
		{ID: a.ID, Group: testGK.Group, Kind: testGK.Kind},
		{ID: b.ID, Group: testGK.Group, Kind: testGK.Kind},
	}, deps)

	none, err := store.ListIncomingRefs(ctx, a.ID, "depends_on")
	require.NoError(t, err)
	assert.Empty(t, none, "a target with no dependents returns an empty slice, not an error")
}

func TestAddRefDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	require.Error(t, store.AddRef(context.Background(), 1, 2, "depends_on"))
}

func TestDeleteRefDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	require.Error(t, store.DeleteRef(context.Background(), 1, 2, "depends_on"))
}

func TestListIncomingRefsDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.ListIncomingRefs(context.Background(), 1, "depends_on")
	require.Error(t, err)
}

// newConditionObject creates a bare object to hang conditions on.
func newConditionObject(t *testing.T, store beehive.Store, name string) *beehive.RawObject {
	t.Helper()
	obj, err := store.CreateObject(context.Background(), &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Slug: new(name), Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	return obj
}

// findCondition returns the condition of the given type, or nil.
func findCondition(conds []storeapi.Condition, condType string) *storeapi.Condition {
	for i := range conds {
		if conds[i].Type == condType {
			return &conds[i]
		}
	}
	return nil
}

func TestSetConditionReadBack(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "ready-obj")

	got, err := store.SetCondition(ctx, testGK, obj.ID, storeapi.Condition{
		Type: "Ready", Status: "True", Reason: "Provisioned", Message: "all good",
	})
	require.NoError(t, err)

	cond := findCondition(got.Conditions, "Ready")
	require.NotNil(t, cond, "Ready condition must be present on the returned object")
	assert.Equal(t, "True", cond.Status)
	assert.Equal(t, "Provisioned", cond.Reason)
	assert.Equal(t, "all good", cond.Message)
}

func TestConditionsSurfaceOnReads(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "multi-read")

	_, err := store.SetCondition(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	// A second, independent type must coexist without clobbering the first.
	_, err = store.SetCondition(ctx, testGK, obj.ID, storeapi.Condition{Type: "Healthy", Status: "False", Reason: "Degraded"})
	require.NoError(t, err)

	assertBoth := func(t *testing.T, conds []storeapi.Condition) {
		t.Helper()
		ready := findCondition(conds, "Ready")
		healthy := findCondition(conds, "Healthy")
		require.NotNil(t, ready)
		require.NotNil(t, healthy)
		assert.Equal(t, "True", ready.Status)
		assert.Equal(t, "False", healthy.Status)
		assert.Equal(t, "Degraded", healthy.Reason)
	}

	byID, err := store.GetObject(ctx, obj.ID)
	require.NoError(t, err)
	assertBoth(t, byID.Conditions)

	byName, err := store.GetObjectBySlug(ctx, testGK, "multi-read")
	require.NoError(t, err)
	assertBoth(t, byName.Conditions)

	list, err := store.ListObjects(ctx, testGK)
	require.NoError(t, err)
	require.Len(t, list, 1)
	assertBoth(t, list[0].Conditions)
}

func TestSetConditionTransitionedAt(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "transition")

	_, err := store.SetCondition(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True", Reason: "A"})
	require.NoError(t, err)

	// Backdate transitioned_at to a known sentinel so we can prove preservation
	// (not a same-millisecond coincidence) and detect a fresh overwrite.
	const sentinel = int64(12345)
	backdate := func() {
		_, err := store.db.ExecContext(ctx,
			`UPDATE conditions SET transitioned_at = ? WHERE object_id = ? AND type = 'Ready'`, sentinel, obj.ID)
		require.NoError(t, err)
	}

	// Same status, different reason: transitioned_at is preserved at the sentinel.
	backdate()
	got, err := store.SetCondition(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True", Reason: "B"})
	require.NoError(t, err)
	assert.Equal(t, time.UnixMilli(sentinel).UTC(), findCondition(got.Conditions, "Ready").TransitionedAt,
		"same status keeps transitioned_at")

	// Status change: transitioned_at advances to the write's fresh stamp.
	backdate()
	got, err = store.SetCondition(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "False", Reason: "C"})
	require.NoError(t, err)
	changed := findCondition(got.Conditions, "Ready")
	assert.True(t, changed.TransitionedAt.After(time.UnixMilli(sentinel).UTC()),
		"status change advances transitioned_at past the sentinel")
	assert.Equal(t, changed.TransitionedAt, changed.UpdatedAt,
		"status change stamps transitioned_at = updated_at")
}

func TestSetConditionEmitsAndBumpsResourceVersion(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "watched")

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()
	// Drain the snapshot Added for the pre-existing object.
	require.Equal(t, beehive.WatchEventAdded, recvEvent(t, w).Type)

	got, err := store.SetCondition(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	assert.Greater(t, got.ResourceVersion, obj.ResourceVersion, "a condition change bumps resource_version")

	ev := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventModified, ev.Type)
	assert.Equal(t, got.ResourceVersion, ev.Object.ResourceVersion)
	require.NotNil(t, findCondition(ev.Object.Conditions, "Ready"), "emitted object carries the new condition")
}

func TestSetConditionNoOpSuppressed(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "noop")

	first, err := store.SetCondition(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True", Reason: "Up"})
	require.NoError(t, err)

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()
	require.Equal(t, beehive.WatchEventAdded, recvEvent(t, w).Type) // snapshot

	// An identical write changes nothing: no resource_version bump, no event.
	again, err := store.SetCondition(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True", Reason: "Up"})
	require.NoError(t, err)
	assert.Equal(t, first.ResourceVersion, again.ResourceVersion, "identical condition write is a no-op")
	assertNoEvent(t, w, 200*time.Millisecond)
}

func TestDeleteCondition(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "deletable")

	_, err := store.SetCondition(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	_, err = store.SetCondition(ctx, testGK, obj.ID, storeapi.Condition{Type: "Healthy", Status: "True"})
	require.NoError(t, err)

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()
	require.Equal(t, beehive.WatchEventAdded, recvEvent(t, w).Type) // snapshot

	got, err := store.DeleteCondition(ctx, testGK, obj.ID, "Ready")
	require.NoError(t, err)
	assert.Nil(t, findCondition(got.Conditions, "Ready"), "Ready removed")
	require.NotNil(t, findCondition(got.Conditions, "Healthy"), "Healthy untouched")

	ev := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventModified, ev.Type)
	assert.Equal(t, got.ResourceVersion, ev.Object.ResourceVersion)
}

func TestDeleteConditionAbsentIsNoOp(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "absent")

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()
	require.Equal(t, beehive.WatchEventAdded, recvEvent(t, w).Type) // snapshot

	got, err := store.DeleteCondition(ctx, testGK, obj.ID, "Ready")
	require.NoError(t, err)
	assert.Equal(t, obj.ResourceVersion, got.ResourceVersion, "deleting an absent condition is a no-op")
	assertNoEvent(t, w, 200*time.Millisecond)
}

// TestNonConditionWritesPreserveConditions verifies that mutators which don't
// touch conditions still return — and emit — the object with its existing
// conditions assembled, matching Get/List. Otherwise an Update result or a
// Modified watch event after a status/spec change would show Conditions == nil.
func TestNonConditionWritesPreserveConditions(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "preserve")
	_, err := store.SetCondition(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()
	require.Equal(t, beehive.WatchEventAdded, recvEvent(t, w).Type) // snapshot

	// UpdateStatus return + emitted event both carry the existing condition.
	updated, err := store.UpdateStatus(ctx, testGK, obj.ID, obj.Generation, []byte(`{"v":1}`))
	require.NoError(t, err)
	require.NotNil(t, findCondition(updated.Conditions, "Ready"), "UpdateStatus result carries conditions")
	require.NotNil(t, findCondition(recvEvent(t, w).Object.Conditions, "Ready"), "UpdateStatus event carries conditions")

	// UpdateSpec too.
	spec, err := store.UpdateSpec(ctx, testGK, obj.ID, []byte(`{"s":1}`))
	require.NoError(t, err)
	require.NotNil(t, findCondition(spec.Conditions, "Ready"), "UpdateSpec result carries conditions")
	require.NotNil(t, findCondition(recvEvent(t, w).Object.Conditions, "Ready"), "UpdateSpec event carries conditions")

	// RequestDeletion (the row persists; conditions still exist).
	del, _, err := store.RequestDeletion(ctx, testGK, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, findCondition(del.Conditions, "Ready"), "RequestDeletion result carries conditions")
	require.NotNil(t, findCondition(recvEvent(t, w).Object.Conditions, "Ready"), "RequestDeletion event carries conditions")
}

// TestNonConditionWriteAssemblyError drops the conditions table so the
// post-write condition assembly fails, covering that error branch in the shared
// scanAndEmit (UpdateStatus/UpdateSpec) and in RequestDeletion.
func TestNonConditionWriteAssemblyError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "assembly-error")

	_, err := store.db.ExecContext(ctx, `DROP TABLE conditions`)
	require.NoError(t, err)

	_, err = store.UpdateStatus(ctx, testGK, obj.ID, obj.Generation, []byte(`{}`))
	require.Error(t, err)
	_, err = store.UpdateSpec(ctx, testGK, obj.ID, []byte(`{}`))
	require.Error(t, err)
	_, _, err = store.RequestDeletion(ctx, testGK, obj.ID)
	require.Error(t, err)
}

func TestDeleteObjectCascadesConditions(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "cascade")

	_, err := store.SetCondition(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	require.NoError(t, store.DeleteObject(ctx, obj.ID))

	var count int
	require.NoError(t, store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM conditions WHERE object_id = ?`, obj.ID).Scan(&count))
	assert.Zero(t, count, "ON DELETE CASCADE removes the object's condition rows")
}

func TestLivenessDowngradedToUnknownBeforeProcessStart(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "liveness")

	// A liveness condition and a store-truth condition, both written "now".
	_, err := store.SetCondition(ctx, testGK, obj.ID,
		storeapi.Condition{Type: "Connected", Status: "True", Liveness: true})
	require.NoError(t, err)
	_, err = store.SetCondition(ctx, testGK, obj.ID,
		storeapi.Condition{Type: "Provisioned", Status: "True"})
	require.NoError(t, err)

	// Simulate a process that started AFTER both writes: the liveness condition
	// is no longer re-confirmed by this process, so it reads as "verifying".
	store.processStart = time.Now().Add(time.Hour)

	got, err := store.GetObject(ctx, obj.ID)
	require.NoError(t, err)
	live := findCondition(got.Conditions, "Connected")
	truth := findCondition(got.Conditions, "Provisioned")
	require.NotNil(t, live)
	require.NotNil(t, truth)
	assert.Equal(t, "Unknown", live.Status, "stale liveness condition downgrades to Unknown")
	assert.Equal(t, "True", truth.Status, "store-truth condition is unaffected")
}

func TestStaleLivenessReConfirmRefreshes(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "reconfirm")

	_, err := store.SetCondition(ctx, testGK, obj.ID,
		storeapi.Condition{Type: "Connected", Status: "True", Reason: "Dialed", Liveness: true})
	require.NoError(t, err)

	// Backdate the write to before processStart so it reads as stale "verifying".
	_, err = store.db.ExecContext(ctx,
		`UPDATE conditions SET updated_at = 0 WHERE object_id = ? AND type = 'Connected'`, obj.ID)
	require.NoError(t, err)
	got, err := store.GetObject(ctx, obj.ID)
	require.NoError(t, err)
	require.Equal(t, "Unknown", findCondition(got.Conditions, "Connected").Status, "precondition: reads as verifying")

	// Re-confirming the identical condition must NOT be suppressed as a no-op: the
	// write has to refresh updated_at so the condition is valid in this process
	// again, otherwise it stays downgraded to Unknown forever.
	_, err = store.SetCondition(ctx, testGK, obj.ID,
		storeapi.Condition{Type: "Connected", Status: "True", Reason: "Dialed", Liveness: true})
	require.NoError(t, err)

	got, err = store.GetObject(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, "True", findCondition(got.Conditions, "Connected").Status,
		"re-confirmed liveness condition is no longer downgraded")
}

func TestSetConditionObjectNotFound(t *testing.T) {
	store := newTestStore(t)
	_, err := store.SetCondition(context.Background(), testGK, 999999, storeapi.Condition{
		Type: "Ready", Status: "True",
	})
	assert.ErrorIs(t, err, beehive.ErrNotFound)
}

func TestSetConditionDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.SetCondition(context.Background(), testGK, 1, storeapi.Condition{Type: "Ready", Status: "True"})
	require.Error(t, err)
}

func TestSetConditionInvalidStatusRejected(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "bad-status")

	// The conditions.status CHECK constraint rejects anything outside the enum,
	// surfacing as an error from the upsert.
	_, err := store.SetCondition(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "Bogus"})
	require.Error(t, err)
}

func TestDeleteConditionDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.DeleteCondition(context.Background(), testGK, 1, "Ready")
	require.Error(t, err)
}

// TestConditionAssemblyError corrupts a condition row so the read-path scan
// fails, exercising the conditions-assembly error branches in GetObject (via
// loadConditions) and ListObjects (via loadConditionsForKind).
func TestConditionAssemblyError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "corrupt")

	// transitioned_at is an INTEGER column; storing text makes the int64 scan fail.
	_, err := store.db.ExecContext(ctx, `
		INSERT INTO conditions (object_id, type, status, transitioned_at, updated_at)
		VALUES (?, 'Ready', 'True', 'not-an-int', 0)`, obj.ID)
	require.NoError(t, err)

	_, err = store.GetObject(ctx, obj.ID)
	require.Error(t, err, "GetObject surfaces a conditions scan error")

	_, err = store.ListObjects(ctx, testGK)
	require.Error(t, err, "ListObjects surfaces a conditions scan error")
}

// TestConditionResourceVersionError drops the resource_version sequence so the
// post-write version bump fails. It covers that error branch in both
// SetCondition and DeleteCondition, and asserts the write is atomic with the
// bump: when the bump fails, the condition change is rolled back rather than
// left applied without a version bump or watch event.
func TestConditionResourceVersionError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "rv-error")

	_, err := store.SetCondition(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	_, err = store.db.ExecContext(ctx, `DROP TABLE resource_version_seq`)
	require.NoError(t, err)

	// A real change whose version bump fails: the whole call rolls back.
	_, err = store.SetCondition(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "False"})
	require.Error(t, err)
	got, err := store.GetObject(ctx, obj.ID)
	require.NoError(t, err)
	ready := findCondition(got.Conditions, "Ready")
	require.NotNil(t, ready, "rolled-back SetCondition must not delete the prior condition")
	assert.Equal(t, "True", ready.Status, "rolled-back SetCondition must not apply the changed status")

	// A delete whose version bump fails likewise rolls back, leaving the row.
	_, err = store.DeleteCondition(ctx, testGK, obj.ID, "Ready")
	require.Error(t, err)
	got, err = store.GetObject(ctx, obj.ID)
	require.NoError(t, err)
	assert.NotNil(t, findCondition(got.Conditions, "Ready"), "rolled-back DeleteCondition must leave the condition in place")
}

func TestGetConditionScanError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "getcond-corrupt")

	_, err := store.db.ExecContext(ctx, `
		INSERT INTO conditions (object_id, type, status, transitioned_at, updated_at)
		VALUES (?, 'Ready', 'True', 'not-an-int', 0)`, obj.ID)
	require.NoError(t, err)

	// The object row reads fine, but SetCondition's getCondition pre-read hits the
	// corrupt row and fails before any write.
	_, err = store.SetCondition(ctx, testGK, obj.ID, storeapi.Condition{Type: "Ready", Status: "False"})
	require.Error(t, err)
}

// dropSeq removes the resource_version sequence so the next nextResourceVersion
// call (an UPDATE ... RETURNING) fails, while ordinary object reads still work —
// isolating each mutator's version-bump error branch.
func dropSeq(t *testing.T, store *sqliteStore) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(), `DROP TABLE resource_version_seq`)
	require.NoError(t, err)
}

// dropConditions removes the conditions table while the connection stays open, so
// a DELETE/INSERT against it fails inside an already-open transaction.
func dropConditions(t *testing.T, store *sqliteStore) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(), `DROP TABLE conditions`)
	require.NoError(t, err)
}

// dropObjects removes the objects table mid-connection so a scoped read inside an
// open transaction fails (BeginTx still succeeds, unlike closing the db).
func dropObjects(t *testing.T, store *sqliteStore) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(), `DROP TABLE objects`)
	require.NoError(t, err)
}

func TestLoadConditionsForKindQueryError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.loadConditionsForKind(context.Background(), testGK)
	require.Error(t, err)
}

// TestUpdateSpecResourceVersionError covers UpdateSpec's nextResourceVersion
// branch: the scoped read succeeds and the spec differs, then the version bump
// fails because the sequence table is gone.
func TestUpdateSpecResourceVersionError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	dropSeq(t, store)

	_, err := store.UpdateSpec(ctx, testGK, obj.ID, []byte(`{"changed":true}`))
	require.Error(t, err)
}

// TestUpdateStatusResourceVersionError covers UpdateStatus's nextResourceVersion
// branch (its first statement inside Within).
func TestUpdateStatusResourceVersionError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	dropSeq(t, store)

	_, err := store.UpdateStatus(ctx, testGK, obj.ID, obj.Generation, []byte(`{}`))
	require.Error(t, err)
}

// TestDeleteConditionScopedReadError covers DeleteCondition's scoped-read error
// branch: BeginTx succeeds, but the objects table is gone so the read fails.
func TestDeleteConditionScopedReadError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "del-cond-read")
	dropObjects(t, store)

	_, err := store.DeleteCondition(ctx, testGK, obj.ID, "Ready")
	require.Error(t, err)
}

// TestDeleteConditionDeleteExecError covers DeleteCondition's DELETE-exec error
// branch: the object read succeeds but the conditions table is gone.
func TestDeleteConditionDeleteExecError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "del-cond-exec")
	dropConditions(t, store)

	_, err := store.DeleteCondition(ctx, testGK, obj.ID, "Ready")
	require.Error(t, err)
}

// TestDeleteFinalizerResourceVersionError covers DeleteFinalizer's
// nextResourceVersion branch: a present finalizer is removed (a real change),
// then the version bump fails.
func TestDeleteFinalizerResourceVersionError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
		Finalizers: []string{"f"},
	})
	require.NoError(t, err)
	dropSeq(t, store)

	_, err = store.DeleteFinalizer(ctx, testGK, obj.ID, "f")
	require.Error(t, err)
}

// TestRequestDeletionResourceVersionError covers markForDeletion's
// nextResourceVersion branch, reached via RequestDeletion on a live object whose
// version bump fails.
func TestRequestDeletionResourceVersionError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newRefObject(t, store)
	dropSeq(t, store)

	_, _, err := store.RequestDeletion(ctx, testGK, obj.ID)
	require.Error(t, err)
}

// TestMarkOwnedForDeletionQueryError covers the child-lookup query error.
func TestMarkOwnedForDeletionQueryError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.MarkOwnedForDeletion(context.Background(), 1)
	require.Error(t, err)
}

// TestMarkOwnedForDeletionChildMarkError covers the per-child markForDeletion
// error branch: an owned, not-yet-deleting child exists, but the version bump in
// markForDeletion fails (sequence dropped) with a non-ErrNotFound error.
func TestMarkOwnedForDeletionChildMarkError(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	owner := newRefObject(t, store)
	child := newRefObject(t, store)
	require.NoError(t, store.AddRef(ctx, child.ID, owner.ID, storeapi.RelationOwnedBy))
	dropSeq(t, store)

	_, err := store.MarkOwnedForDeletion(ctx, owner.ID)
	require.Error(t, err)
}

func TestListOutgoingRefsDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.ListOutgoingRefs(context.Background(), 1)
	require.Error(t, err)
}

func TestHasIncomingRefsDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.HasIncomingRefs(context.Background(), 1)
	require.Error(t, err)
}
