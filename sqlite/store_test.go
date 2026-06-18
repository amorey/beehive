package sqlite

import (
	"context"
	"errors"
	"testing"

	"github.com/amorey/beehive"
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
		Name:  new("world"),
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
	require.NotNil(t, obj.Name)
	assert.Equal(t, "world", *obj.Name)
}

func TestCreateObjectPersistsFinalizers(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	want := []string{"kstack.sh/cluster", "kstack.sh/dns"}
	created, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Name: new("guarded"),
		Spec: []byte(`{}`), Finalizers: want,
	})
	require.NoError(t, err)
	assert.Equal(t, want, created.Finalizers)

	// Round-trips through the JSON column, not just the returned struct.
	reloaded, err := store.GetObject(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, want, reloaded.Finalizers)
}

func TestGetByIdAndName(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Name: new("world"),
		Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	byID, err := store.GetObject(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, created.ID, byID.ID)

	byName, err := store.GetObjectByName(ctx, testGK, "world")
	require.NoError(t, err)
	assert.Equal(t, created.ID, byName.ID)
}

func TestGetNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.GetObject(ctx, 999)
	assert.ErrorIs(t, err, beehive.ErrNotFound)

	_, err = store.GetObjectByName(ctx, testGK, "nope")
	assert.ErrorIs(t, err, beehive.ErrNotFound)
}

func TestDuplicateNameRejected(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	mk := func() error {
		_, err := store.CreateObject(ctx, &beehive.RawObject{
			Group: testGK.Group, Kind: testGK.Kind, Name: new("dup"),
			Spec: []byte(`{}`),
		})
		return err
	}
	require.NoError(t, mk())
	assert.Error(t, mk(), "second create with same name should violate UNIQUE")
}

func TestUnnamedObjectsCoexist(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	mk := func() *beehive.RawObject {
		obj, err := store.CreateObject(ctx, &beehive.RawObject{
			Group: testGK.Group, Kind: testGK.Kind, // Name nil
			Spec: []byte(`{}`),
		})
		require.NoError(t, err)
		assert.Nil(t, obj.Name)
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

	updated, err := store.UpdateSpec(ctx, created.ID, []byte(`{"v":2}`))
	require.NoError(t, err)

	assert.EqualValues(t, 2, updated.Generation, "spec change bumps generation")
	assert.Greater(t, updated.ResourceVersion, created.ResourceVersion)
	assert.JSONEq(t, `{"v":2}`, string(updated.Spec))
}

func TestUpdateStatusRecordsObservedGeneration(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	created, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	updated, err := store.UpdateStatus(ctx, created.ID, created.Generation, []byte(`{"msg":"hi"}`))
	require.NoError(t, err)

	require.NotNil(t, updated.ObservedGeneration)
	assert.EqualValues(t, created.Generation, *updated.ObservedGeneration)
	assert.EqualValues(t, created.Generation, updated.Generation, "status write must not bump generation")
	require.NotNil(t, updated.ObservedAt)
	assert.Greater(t, updated.ResourceVersion, created.ResourceVersion)
	assert.JSONEq(t, `{"msg":"hi"}`, string(updated.Status))
}

func TestListObjects(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	for _, n := range []string{"a", "b", "c"} {
		_, err := store.CreateObject(ctx, &beehive.RawObject{
			Group: testGK.Group, Kind: testGK.Kind, Name: new(n),
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
		require.NotNil(t, o.Name)
		names = append(names, *o.Name)
	}
	assert.Equal(t, []string{"a", "b", "c"}, names, "ordered by id")
}

func TestResourceVersionIsMonotonic(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	a, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Name: new("a"), Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	b, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Name: new("b"), Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	assert.Greater(t, b.ResourceVersion, a.ResourceVersion, "each create takes the next cursor value")

	// A later mutation advances the global cursor past every prior write.
	updated, err := store.UpdateSpec(ctx, a.ID, []byte(`{"v":2}`))
	require.NoError(t, err)
	assert.Greater(t, updated.ResourceVersion, b.ResourceVersion)
}

func TestResourceVersionNotReusedAfterDelete(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	a, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Name: new("a"), Spec: []byte(`{}`),
	})
	require.NoError(t, err)
	b, err := store.CreateObject(ctx, &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Name: new("b"), Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	// Delete the highest-versioned row, then write again. The cursor must not
	// fall back to b's version — it only ever moves forward.
	require.NoError(t, store.DeleteObject(ctx, b.ID))

	updated, err := store.UpdateSpec(ctx, a.ID, []byte(`{"v":2}`))
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

	first, err := store.RequestDeletion(ctx, created.ID)
	require.NoError(t, err)
	assert.Greater(t, first.ResourceVersion, created.ResourceVersion,
		"the first request is a real change and bumps the cursor")

	// A repeat request changes no deletion state, so it must be a no-op: same
	// resource_version, same updated_at, no spurious watch/CAS churn.
	second, err := store.RequestDeletion(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, first.ResourceVersion, second.ResourceVersion,
		"an idempotent repeat must not bump resource_version")
	assert.Equal(t, first.UpdatedAt, second.UpdatedAt)
}

func TestMutatorsReturnNotFoundForMissingID(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	const missing beehive.ObjectID = 999

	ops := map[string]func() error{
		"UpdateSpec": func() error {
			_, err := store.UpdateSpec(ctx, missing, []byte(`{}`))
			return err
		},
		"UpdateStatus": func() error {
			_, err := store.UpdateStatus(ctx, missing, 1, []byte(`{}`))
			return err
		},
		"RequestDeletion": func() error {
			_, err := store.RequestDeletion(ctx, missing)
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

	first, err := store.RequestDeletion(ctx, created.ID)
	require.NoError(t, err)
	require.NotNil(t, first.DeletionRequestedAt)

	second, err := store.RequestDeletion(ctx, created.ID)
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
			Group: testGK.Group, Kind: testGK.Kind, Name: new("committed"),
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
			Group: testGK.Group, Kind: testGK.Kind, Name: new("rolledback"),
			Spec: []byte(`{}`),
		})
		require.NoError(t, err)
		return sentinel
	})
	assert.ErrorIs(t, err, sentinel)
	_, err = store.GetObjectByName(ctx, testGK, "rolledback")
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
	_, err = store.UpdateStatus(ctx, settled.ID, settled.Generation, []byte(`{}`))
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
	_, err = store.UpdateStatus(ctx, stale.ID, stale.Generation, []byte(`{}`))
	require.NoError(t, err)
	_, err = store.UpdateSpec(ctx, stale.ID, []byte(`{"updated":true}`))
	require.NoError(t, err)

	// different kind — must NOT appear
	_, err = store.CreateObject(ctx, &beehive.RawObject{
		Group: otherGK.Group, Kind: otherGK.Kind, Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	ids, err := store.ListUnsettledIDs(ctx, testGK)
	require.NoError(t, err)
	assert.Equal(t, []beehive.ObjectID{nilObs.ID, stale.ID}, ids)
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
				Group: testGK.Group, Kind: testGK.Kind, Name: new("nested"),
				Spec: []byte(`{}`),
			})
			return err
		}); err != nil {
			return err
		}
		return sentinel
	})
	assert.ErrorIs(t, err, sentinel)

	_, err = store.GetObjectByName(ctx, testGK, "nested")
	assert.ErrorIs(t, err, beehive.ErrNotFound,
		"nested Within joins the outer tx, so the outer rollback discards its write")
}
