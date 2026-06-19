package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/internal/storeapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newConditionObject creates a bare object to hang conditions on.
func newConditionObject(t *testing.T, store beehive.Store, name string) *beehive.RawObject {
	t.Helper()
	obj, err := store.CreateObject(context.Background(), &beehive.RawObject{
		Group: testGK.Group, Kind: testGK.Kind, Name: new(name), Spec: []byte(`{}`),
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

	got, err := store.SetCondition(ctx, obj.ID, storeapi.Condition{
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

	_, err := store.SetCondition(ctx, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	// A second, independent type must coexist without clobbering the first.
	_, err = store.SetCondition(ctx, obj.ID, storeapi.Condition{Type: "Healthy", Status: "False", Reason: "Degraded"})
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

	byName, err := store.GetObjectByName(ctx, testGK, "multi-read")
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

	_, err := store.SetCondition(ctx, obj.ID, storeapi.Condition{Type: "Ready", Status: "True", Reason: "A"})
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
	got, err := store.SetCondition(ctx, obj.ID, storeapi.Condition{Type: "Ready", Status: "True", Reason: "B"})
	require.NoError(t, err)
	assert.Equal(t, time.UnixMilli(sentinel).UTC(), findCondition(got.Conditions, "Ready").TransitionedAt,
		"same status keeps transitioned_at")

	// Status change: transitioned_at advances to the write's fresh stamp.
	backdate()
	got, err = store.SetCondition(ctx, obj.ID, storeapi.Condition{Type: "Ready", Status: "False", Reason: "C"})
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

	got, err := store.SetCondition(ctx, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
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

	first, err := store.SetCondition(ctx, obj.ID, storeapi.Condition{Type: "Ready", Status: "True", Reason: "Up"})
	require.NoError(t, err)

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()
	require.Equal(t, beehive.WatchEventAdded, recvEvent(t, w).Type) // snapshot

	// An identical write changes nothing: no resource_version bump, no event.
	again, err := store.SetCondition(ctx, obj.ID, storeapi.Condition{Type: "Ready", Status: "True", Reason: "Up"})
	require.NoError(t, err)
	assert.Equal(t, first.ResourceVersion, again.ResourceVersion, "identical condition write is a no-op")
	assertNoEvent(t, w, 200*time.Millisecond)
}

func TestDeleteCondition(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "deletable")

	_, err := store.SetCondition(ctx, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)
	_, err = store.SetCondition(ctx, obj.ID, storeapi.Condition{Type: "Healthy", Status: "True"})
	require.NoError(t, err)

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()
	require.Equal(t, beehive.WatchEventAdded, recvEvent(t, w).Type) // snapshot

	got, err := store.DeleteCondition(ctx, obj.ID, "Ready")
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

	got, err := store.DeleteCondition(ctx, obj.ID, "Ready")
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
	_, err := store.SetCondition(ctx, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()
	require.Equal(t, beehive.WatchEventAdded, recvEvent(t, w).Type) // snapshot

	// UpdateStatus return + emitted event both carry the existing condition.
	updated, err := store.UpdateStatus(ctx, obj.ID, obj.Generation, []byte(`{"v":1}`))
	require.NoError(t, err)
	require.NotNil(t, findCondition(updated.Conditions, "Ready"), "UpdateStatus result carries conditions")
	require.NotNil(t, findCondition(recvEvent(t, w).Object.Conditions, "Ready"), "UpdateStatus event carries conditions")

	// UpdateSpec too.
	spec, err := store.UpdateSpec(ctx, obj.ID, []byte(`{"s":1}`))
	require.NoError(t, err)
	require.NotNil(t, findCondition(spec.Conditions, "Ready"), "UpdateSpec result carries conditions")
	require.NotNil(t, findCondition(recvEvent(t, w).Object.Conditions, "Ready"), "UpdateSpec event carries conditions")

	// RequestDeletion (the row persists; conditions still exist).
	del, _, err := store.RequestDeletion(ctx, obj.ID)
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

	_, err = store.UpdateStatus(ctx, obj.ID, obj.Generation, []byte(`{}`))
	require.Error(t, err)
	_, err = store.UpdateSpec(ctx, obj.ID, []byte(`{}`))
	require.Error(t, err)
	_, _, err = store.RequestDeletion(ctx, obj.ID)
	require.Error(t, err)
}

func TestDeleteObjectCascadesConditions(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "cascade")

	_, err := store.SetCondition(ctx, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
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
	_, err := store.SetCondition(ctx, obj.ID,
		storeapi.Condition{Type: "Connected", Status: "True", Liveness: true})
	require.NoError(t, err)
	_, err = store.SetCondition(ctx, obj.ID,
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

	_, err := store.SetCondition(ctx, obj.ID,
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
	_, err = store.SetCondition(ctx, obj.ID,
		storeapi.Condition{Type: "Connected", Status: "True", Reason: "Dialed", Liveness: true})
	require.NoError(t, err)

	got, err = store.GetObject(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, "True", findCondition(got.Conditions, "Connected").Status,
		"re-confirmed liveness condition is no longer downgraded")
}

func TestSetConditionObjectNotFound(t *testing.T) {
	store := newTestStore(t)
	_, err := store.SetCondition(context.Background(), 999999, storeapi.Condition{
		Type: "Ready", Status: "True",
	})
	assert.ErrorIs(t, err, beehive.ErrNotFound)
}

func TestSetConditionDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.SetCondition(context.Background(), 1, storeapi.Condition{Type: "Ready", Status: "True"})
	require.Error(t, err)
}

func TestSetConditionInvalidStatusRejected(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	obj := newConditionObject(t, store, "bad-status")

	// The conditions.status CHECK constraint rejects anything outside the enum,
	// surfacing as an error from the upsert.
	_, err := store.SetCondition(ctx, obj.ID, storeapi.Condition{Type: "Ready", Status: "Bogus"})
	require.Error(t, err)
}

func TestDeleteConditionDBError(t *testing.T) {
	store := newRawStore(t)
	store.db.Close()
	_, err := store.DeleteCondition(context.Background(), 1, "Ready")
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

	_, err := store.SetCondition(ctx, obj.ID, storeapi.Condition{Type: "Ready", Status: "True"})
	require.NoError(t, err)

	_, err = store.db.ExecContext(ctx, `DROP TABLE resource_version_seq`)
	require.NoError(t, err)

	// A real change whose version bump fails: the whole call rolls back.
	_, err = store.SetCondition(ctx, obj.ID, storeapi.Condition{Type: "Ready", Status: "False"})
	require.Error(t, err)
	got, err := store.GetObject(ctx, obj.ID)
	require.NoError(t, err)
	ready := findCondition(got.Conditions, "Ready")
	require.NotNil(t, ready, "rolled-back SetCondition must not delete the prior condition")
	assert.Equal(t, "True", ready.Status, "rolled-back SetCondition must not apply the changed status")

	// A delete whose version bump fails likewise rolls back, leaving the row.
	_, err = store.DeleteCondition(ctx, obj.ID, "Ready")
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
	_, err = store.SetCondition(ctx, obj.ID, storeapi.Condition{Type: "Ready", Status: "False"})
	require.Error(t, err)
}
