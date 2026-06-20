package sqlite

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/internal/storeapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errWatchBoom = errors.New("boom")

// recvEvent waits for the next event on w, failing the test if none arrives or
// the channel closes within the failsafe timeout.
func recvEvent(t *testing.T, w beehive.Watcher) storeapi.RawWatchEvent {
	t.Helper()
	select {
	case ev, ok := <-w.Events():
		if !ok {
			t.Fatal("watcher channel closed unexpectedly")
		}
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
		panic("unreachable")
	}
}

// assertNoEvent fails if any event arrives on w within d.
func assertNoEvent(t *testing.T, w beehive.Watcher, d time.Duration) {
	t.Helper()
	select {
	case ev, ok := <-w.Events():
		if ok {
			t.Fatalf("unexpected event: %+v", ev)
		}
		t.Fatal("watcher channel closed unexpectedly")
	case <-time.After(d):
	}
}

// assertWatcherClosed fails if w's channel does not close within the timeout.
func assertWatcherClosed(t *testing.T, w beehive.Watcher) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-w.Events():
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for watcher channel to close")
		}
	}
}

func newWatchObject() *beehive.RawObject {
	return &beehive.RawObject{Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`)}
}

// TestWithinFlushesAfterCommit verifies events emitted inside a transaction are
// published once it commits.
func TestWithinFlushesAfterCommit(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()

	require.NoError(t, store.Within(ctx, func(ctx context.Context) error {
		_, err := store.CreateObject(ctx, newWatchObject())
		return err
	}))

	ev := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventAdded, ev.Type)
}

// TestWithinRollbackDiscardsEvents verifies a rolled-back transaction publishes
// nothing.
func TestWithinRollbackDiscardsEvents(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()

	err = store.Within(ctx, func(ctx context.Context) error {
		if _, err := store.CreateObject(ctx, newWatchObject()); err != nil {
			return err
		}
		return errWatchBoom
	})
	require.ErrorIs(t, err, errWatchBoom)

	assertNoEvent(t, w, 200*time.Millisecond)
}

// TestNestedWithinSingleFlush verifies a write in a nested Within produces
// exactly one event, flushed at the outermost commit.
func TestNestedWithinSingleFlush(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()

	require.NoError(t, store.Within(ctx, func(ctx context.Context) error {
		return store.Within(ctx, func(ctx context.Context) error {
			_, err := store.CreateObject(ctx, newWatchObject())
			return err
		})
	}))

	ev := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventAdded, ev.Type)
	assertNoEvent(t, w, 200*time.Millisecond) // only one flush
}

// TestRequestDeletionIdempotentNoEvent verifies the second (idempotent) Delete
// emits no event.
func TestRequestDeletionIdempotentNoEvent(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	obj, err := store.CreateObject(ctx, newWatchObject())
	require.NoError(t, err)

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()

	// Drain the snapshot Added for the pre-existing object.
	snap := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventAdded, snap.Type)

	_, changed, err := store.RequestDeletion(ctx, testGK, obj.ID)
	require.NoError(t, err)
	require.True(t, changed)
	ev := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventModified, ev.Type)

	_, changed, err = store.RequestDeletion(ctx, testGK, obj.ID)
	require.NoError(t, err)
	require.False(t, changed)
	assertNoEvent(t, w, 200*time.Millisecond)
}

// TestWatchDedupesSnapshotResourceVersion verifies a live event already covered
// by the snapshot (resource version ≤ the snapshot's) is dropped. The
// beforeSnapshot hook publishes the duplicate into the subscribe→snapshot window.
func TestWatchDedupesSnapshotResourceVersion(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	obj, err := store.CreateObject(ctx, newWatchObject())
	require.NoError(t, err)

	store.beforeSnapshot = func() {
		// Same resource version as the snapshot will carry → must be deduped.
		store.publish(testGK, storeapi.RawWatchEvent{Type: beehive.WatchEventModified, Object: obj})
	}

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()

	ev := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventAdded, ev.Type)
	assert.Equal(t, obj.ID, ev.Object.ID)
	assertNoEvent(t, w, 200*time.Millisecond) // the duplicate live event was dropped
}

// TestWatchStreamsLiveAfterSnapshot verifies a live event with a newer resource
// version is delivered after the snapshot.
func TestWatchStreamsLiveAfterSnapshot(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	obj, err := store.CreateObject(ctx, newWatchObject())
	require.NoError(t, err)

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()
	recvEvent(t, w) // drain snapshot Added

	_, err = store.UpdateSpec(ctx, testGK, obj.ID, []byte(`{"v":2}`))
	require.NoError(t, err)

	ev := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventModified, ev.Type)
	assert.Equal(t, obj.ID, ev.Object.ID)
}

// TestWatchFiltersByID verifies Watch(id) drops live events for other objects.
func TestWatchFiltersByID(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	obj1, err := store.CreateObject(ctx, newWatchObject())
	require.NoError(t, err)
	obj2, err := store.CreateObject(ctx, newWatchObject())
	require.NoError(t, err)

	w, err := store.Watch(ctx, testGK, obj1.ID)
	require.NoError(t, err)
	defer w.Close()
	snap := recvEvent(t, w) // snapshot Added for obj1 only
	assert.Equal(t, obj1.ID, snap.Object.ID)

	_, err = store.UpdateSpec(ctx, testGK, obj2.ID, []byte(`{"v":2}`)) // filtered out
	require.NoError(t, err)
	_, err = store.UpdateSpec(ctx, testGK, obj1.ID, []byte(`{"v":2}`)) // delivered
	require.NoError(t, err)

	ev := recvEvent(t, w)
	assert.Equal(t, obj1.ID, ev.Object.ID)
}

// TestWatchAfterCloseErrors verifies Watch/WatchList fail once the store closed.
func TestWatchAfterCloseErrors(t *testing.T) {
	store, err := OpenMemory()
	require.NoError(t, err)
	require.NoError(t, store.Close())

	_, err = store.WatchList(context.Background(), testGK)
	require.ErrorIs(t, err, errStoreClosed)
	_, err = store.Watch(context.Background(), testGK, 1)
	require.ErrorIs(t, err, errStoreClosed)
	_, err = store.WatchEvents(context.Background(), testGK)
	require.ErrorIs(t, err, errStoreClosed)
}

// TestWatchEventsSkipsSnapshot verifies WatchEvents delivers no initial snapshot
// for pre-existing objects, but does stream subsequent live changes.
func TestWatchEventsSkipsSnapshot(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// A pre-existing object: WatchList would replay it as Added; WatchEvents must not.
	pre, err := store.CreateObject(ctx, newWatchObject())
	require.NoError(t, err)

	w, err := store.WatchEvents(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()
	assertNoEvent(t, w, 200*time.Millisecond)

	// A live change to that object streams through.
	_, err = store.UpdateSpec(ctx, testGK, pre.ID, []byte(`{"x":1}`))
	require.NoError(t, err)
	ev := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventModified, ev.Type)
	assert.Equal(t, pre.ID, ev.Object.ID)
}

// TestWatchEventsStreamsLiveAdded verifies a newly created object reaches a
// WatchEvents subscriber as an Added event (only the initial snapshot is skipped).
func TestWatchEventsStreamsLiveAdded(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	w, err := store.WatchEvents(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()

	created, err := store.CreateObject(ctx, newWatchObject())
	require.NoError(t, err)
	ev := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventAdded, ev.Type)
	assert.Equal(t, created.ID, ev.Object.ID)
}

// TestWatchSnapshotLoadError verifies a snapshot-load failure surfaces as an
// error (and the receiver is released). beforeSnapshot closes the db so the
// ListObjects snapshot query fails.
func TestWatchSnapshotLoadError(t *testing.T) {
	store, err := OpenMemory()
	require.NoError(t, err)
	store.beforeSnapshot = func() { store.db.Close() }

	_, err = store.WatchList(context.Background(), testGK)
	require.Error(t, err)
}

// TestWatchClosesOnStoreClose verifies closing the store closes active watchers
// (here the goroutine is parked on a receive).
func TestWatchClosesOnStoreClose(t *testing.T) {
	store, err := OpenMemory()
	require.NoError(t, err)

	w, err := store.WatchList(context.Background(), testGK)
	require.NoError(t, err)

	require.NoError(t, store.Close())
	assertWatcherClosed(t, w)
}

// TestWatchClosesOnStoreCloseWhileParkedOnSend verifies a watcher parked on a
// send (a snapshot item, no reader) is woken and torn down by Close — the case
// closing the hub alone would miss. Exit is awaited via afterStream to avoid
// racing the send/close selection with a reader.
func TestWatchClosesOnStoreCloseWhileParkedOnSend(t *testing.T) {
	store := newRawStore(t)
	exited := make(chan struct{})
	store.afterStream = func() { close(exited) }
	ctx := context.Background()

	_, err := store.CreateObject(ctx, newWatchObject()) // snapshot has one item
	require.NoError(t, err)

	w, err := store.WatchList(ctx, testGK) // goroutine parks on the snapshot send
	require.NoError(t, err)

	require.NoError(t, store.Close())
	<-exited
	_, ok := <-w.Events()
	assert.False(t, ok, "channel must close when the store closes mid-send")
}

// TestDeleteObjectEmitsDeleted verifies the physical delete publishes a Deleted
// event so watch-stream caches learn the object is gone.
func TestDeleteObjectEmitsDeleted(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	obj, err := store.CreateObject(ctx, newWatchObject())
	require.NoError(t, err)

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()
	recvEvent(t, w) // drain snapshot Added

	require.NoError(t, store.DeleteObject(ctx, obj.ID))

	ev := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventDeleted, ev.Type)
	assert.Equal(t, obj.ID, ev.Object.ID)
}

// TestWatchNotFoundEmptySnapshot verifies Watch on an id that doesn't exist
// starts with an empty snapshot (ErrNotFound is treated as no snapshot) rather
// than erroring.
func TestWatchNotFoundEmptySnapshot(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	w, err := store.Watch(ctx, testGK, 999)
	require.NoError(t, err)
	defer w.Close()
	assertNoEvent(t, w, 100*time.Millisecond) // empty snapshot, no events
}

// TestWatchSnapshotGetObjectError verifies a non-ErrNotFound snapshot failure on
// Watch surfaces as an error.
func TestWatchSnapshotGetObjectError(t *testing.T) {
	store, err := OpenMemory()
	require.NoError(t, err)
	store.beforeSnapshot = func() { store.db.Close() }

	_, err = store.Watch(context.Background(), testGK, 1)
	require.Error(t, err)
}

// TestWatchSnapshotSendCtxDone covers the snapshot-send path exiting on context
// cancellation: a snapshot is pending but the context is cancelled with no
// reader, so the goroutine takes the ctx.Done arm instead of sending. The test
// awaits exit via afterStream rather than reading the channel, which would race
// the goroutine's send/cancel selection.
func TestWatchSnapshotSendCtxDone(t *testing.T) {
	store := newRawStore(t)
	exited := make(chan struct{})
	store.afterStream = func() { close(exited) }
	ctx, cancel := context.WithCancel(context.Background())

	_, err := store.CreateObject(ctx, newWatchObject()) // snapshot has one item
	require.NoError(t, err)

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)

	cancel() // goroutine parks on the snapshot send (no reader) → takes ctx.Done
	<-exited
	_, ok := <-w.Events()
	assert.False(t, ok, "channel must be closed after the goroutine exits")
}

// TestWatchCoalescesRapidUpdates verifies the conflating hub collapses several
// rapid updates to one object — published while the watcher is still parked
// delivering the snapshot Added — into a single Modified carrying the latest
// body. A ring would have delivered every intermediate version.
func TestWatchCoalescesRapidUpdates(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	obj, err := store.CreateObject(ctx, newWatchObject()) // rv1
	require.NoError(t, err)

	w, err := store.Watch(ctx, testGK, obj.ID) // snapshot high-water = rv1
	require.NoError(t, err)
	defer w.Close()

	// Two live updates with rv > high-water, published before the snapshot Added
	// is drained, so the goroutine is parked and both land in the receiver's slot.
	mod := func(rv int64, spec string) {
		store.publish(testGK, storeapi.RawWatchEvent{Type: beehive.WatchEventModified,
			Object: &beehive.RawObject{ID: obj.ID, Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(spec), ResourceVersion: rv}})
	}
	mod(2, `{"v":2}`)
	mod(3, `{"v":3}`)

	first := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventAdded, first.Type)

	rec := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventModified, rec.Type)
	assert.Equal(t, obj.ID, rec.Object.ID)
	assert.JSONEq(t, `{"v":3}`, string(rec.Object.Spec))

	// The rv2 update was coalesced away — no second Modified.
	assertNoEvent(t, w, 100*time.Millisecond)
}

// TestWatchDeliversRealDeleteBodyWhenSlow verifies that when a lagging watcher's
// pending update coalesces into a delete, it receives a single Deleted carrying
// the object's real final body — not the null-spec tombstone the old relist
// synthesized.
func TestWatchDeliversRealDeleteBodyWhenSlow(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	obj, err := store.CreateObject(ctx, newWatchObject()) // rv1
	require.NoError(t, err)

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()

	// Update then delete while parked on the snapshot Added: the Modified
	// coalesces into the Deleted, which carries the real last row.
	store.publish(testGK, storeapi.RawWatchEvent{Type: beehive.WatchEventModified,
		Object: &beehive.RawObject{ID: obj.ID, Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{"hello":"mutated"}`), ResourceVersion: 2}})
	store.publish(testGK, storeapi.RawWatchEvent{Type: beehive.WatchEventDeleted,
		Object: &beehive.RawObject{ID: obj.ID, Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{"hello":"final"}`), ResourceVersion: 3}})

	first := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventAdded, first.Type)

	rec := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventDeleted, rec.Type)
	assert.Equal(t, obj.ID, rec.Object.ID)
	assert.JSONEq(t, `{"hello":"final"}`, string(rec.Object.Spec))
}

// TestWatchSnapshotRaceDeleteNotLost verifies the P1 correctness property: an
// object created in the subscribe→snapshot race window (its Added is buffered
// before the snapshot is taken, and the snapshot includes it) must not lose a
// subsequent delete. The old annihilation in mergeWatchEvent would coalesce the
// buffered Added+Deleted into nothing, leaving the consumer with a stale object.
func TestWatchSnapshotRaceDeleteNotLost(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	// Create the object inside beforeSnapshot so its Added is buffered in the
	// receiver before the snapshot transaction starts. The snapshot includes the
	// object (it exists in the DB), so the consumer learns about it via the
	// snapshot Added. Then a Deleted is published while the goroutine is parked
	// delivering that snapshot Added — it coalesces with the buffered race-window
	// Added. The consumer must still receive the Deleted.
	var created *storeapi.RawObject
	store.beforeSnapshot = func() {
		var err error
		created, err = store.CreateObject(ctx, newWatchObject())
		require.NoError(t, err)
	}

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()

	// Publish Deleted before reading any event; goroutine is parked on the
	// snapshot send, so this lands in the buffer and merges with the buffered
	// race-window Added.
	store.publish(testGK, storeapi.RawWatchEvent{
		Type: beehive.WatchEventDeleted,
		Object: &storeapi.RawObject{
			ID: created.ID, Group: testGK.Group, Kind: testGK.Kind,
			ResourceVersion: created.ResourceVersion + 1,
		},
	})

	snap := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventAdded, snap.Type)
	assert.Equal(t, created.ID, snap.Object.ID)

	del := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventDeleted, del.Type)
	assert.Equal(t, created.ID, del.Object.ID)
}

// TestAnnihilatingMergeForList verifies the WatchList memory bound: a coalesced
// Added→Deleted for an object the snapshot never contained is dropped
// (keep=false) so a slow consumer never accumulates a tombstone per transient id
// — while a snapshot-covered object's delete is still preserved.
func TestAnnihilatingMergeForList(t *testing.T) {
	var seed atomic.Pointer[snapshotIDs]
	merge := annihilatingMerge(snapshotPreserve(&seed))
	const id storeapi.ObjectID = 42
	added := storeapi.RawWatchEvent{Type: beehive.WatchEventAdded,
		Object: &storeapi.RawObject{ID: id, ResourceVersion: 5}}
	deleted := storeapi.RawWatchEvent{Type: beehive.WatchEventDeleted,
		Object: &storeapi.RawObject{ID: id, ResourceVersion: 6}}

	// Snapshot not yet loaded: membership unknown, so keep conservatively.
	got, keep := merge(added, deleted)
	require.True(t, keep)
	assert.Equal(t, beehive.WatchEventDeleted, got.Type)

	// Snapshot loaded without the id: transient object the consumer never saw —
	// annihilate the pair.
	empty := snapshotIDs{}
	seed.Store(&empty)
	_, keep = merge(added, deleted)
	assert.False(t, keep)

	// Snapshot contains the id (race-window object): its delete must survive.
	withID := snapshotIDs{id: {}}
	seed.Store(&withID)
	got, keep = merge(added, deleted)
	require.True(t, keep)
	assert.Equal(t, beehive.WatchEventDeleted, got.Type)
}

// TestAnnihilatingMergeForEvents verifies the WatchEvents memory bound: with no
// snapshot to preserve (preserve == nil), every unobserved Added→Deleted pair is
// annihilated, while a non-delete coalescence still survives.
func TestAnnihilatingMergeForEvents(t *testing.T) {
	merge := annihilatingMerge(nil)
	const id storeapi.ObjectID = 7
	added := storeapi.RawWatchEvent{Type: beehive.WatchEventAdded,
		Object: &storeapi.RawObject{ID: id, ResourceVersion: 1}}
	deleted := storeapi.RawWatchEvent{Type: beehive.WatchEventDeleted,
		Object: &storeapi.RawObject{ID: id, ResourceVersion: 2}}
	modified := storeapi.RawWatchEvent{Type: beehive.WatchEventModified,
		Object: &storeapi.RawObject{ID: id, ResourceVersion: 2}}

	// Unobserved create→delete: dropped entirely.
	_, keep := merge(added, deleted)
	assert.False(t, keep)

	// Create→modify still coalesces and survives (kept as Added, latest body).
	got, keep := merge(added, modified)
	require.True(t, keep)
	assert.Equal(t, beehive.WatchEventAdded, got.Type)
	assert.EqualValues(t, 2, got.Object.ResourceVersion)
}

// TestWatchSnapshotRaceModifiedNotAdded verifies that when a race-window Added
// and a post-snapshot Modified coalesce in the buffer (mergeWatchEvent preserves
// Added type since prev was Added), the consumer — which already received the
// object via the snapshot — sees Modified, not a spurious second Added.
func TestWatchSnapshotRaceModifiedNotAdded(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	var created *storeapi.RawObject
	store.beforeSnapshot = func() {
		var err error
		created, err = store.CreateObject(ctx, newWatchObject())
		require.NoError(t, err)
	}

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()

	// Publish Modified while the goroutine is parked on the snapshot send.
	// The merge of (buffered Added, incoming Modified) yields Added — but the
	// consumer already has this object from the snapshot, so it must arrive
	// as Modified.
	store.publish(testGK, storeapi.RawWatchEvent{
		Type: beehive.WatchEventModified,
		Object: &storeapi.RawObject{
			ID: created.ID, Group: testGK.Group, Kind: testGK.Kind,
			Spec:            []byte(`{"v":2}`),
			ResourceVersion: created.ResourceVersion + 1,
		},
	})

	snap := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventAdded, snap.Type)
	assert.Equal(t, created.ID, snap.Object.ID)

	mod := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventModified, mod.Type) // not a second Added
	assert.Equal(t, created.ID, mod.Object.ID)
}

// TestWatchBornAndDiedBeforeSnapshotUnobserved verifies that an object created
// and deleted entirely within the subscribe→snapshot race window leaves no trace
// in the stream. Both events' resource versions are ≤ the snapshot's high-water,
// so the RV filter drops the coalesced tombstone.
func TestWatchBornAndDiedBeforeSnapshotUnobserved(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	store.beforeSnapshot = func() {
		obj, err := store.CreateObject(ctx, newWatchObject())
		require.NoError(t, err)
		// Delete without going through RequestDeletion; the freshly created
		// object has no finalizers or referrers, so it can be removed directly.
		require.NoError(t, store.DeleteObject(ctx, obj.ID))
	}

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()

	// Both the Added and the coalesced Deleted have RV ≤ the snapshot's
	// high-water mark and are filtered; the consumer sees nothing.
	assertNoEvent(t, w, 200*time.Millisecond)
}

// TestWatchBornAndDiedAfterSnapshotUnobserved verifies that an object born and
// deleted entirely in the live stream — after the snapshot, while the consumer
// never received its Added — does not produce a spurious Deleted. Without the
// seenIDs guard the coalesced tombstone would pass the RV filter and reach the
// consumer as a Deleted for an object it never knew about.
func TestWatchBornAndDiedAfterSnapshotUnobserved(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()

	// Inject Added then Deleted before any read — goroutine is alive but the
	// empty snapshot means it has no snapshot items to send, so it's parked
	// waiting for a live event. The Added coalesces with the Deleted in the
	// conflating buffer; the resulting Deleted must be silently dropped.
	store.publish(testGK, storeapi.RawWatchEvent{
		Type:   beehive.WatchEventAdded,
		Object: &storeapi.RawObject{ID: 99, Group: testGK.Group, Kind: testGK.Kind, ResourceVersion: 1},
	})
	store.publish(testGK, storeapi.RawWatchEvent{
		Type:   beehive.WatchEventDeleted,
		Object: &storeapi.RawObject{ID: 99, Group: testGK.Group, Kind: testGK.Kind, ResourceVersion: 2},
	})

	assertNoEvent(t, w, 200*time.Millisecond)
}

// TestWatchLiveSendCtxDone covers the live-send path exiting on context
// cancellation. beforeSnapshot buffers a live event (RecvContext prefers a
// ready value over a cancelled context), so with an empty snapshot the goroutine
// reaches the live send and takes the ctx.Done arm.
func TestWatchLiveSendCtxDone(t *testing.T) {
	store := newRawStore(t)
	exited := make(chan struct{})
	store.afterStream = func() { close(exited) }
	ctx, cancel := context.WithCancel(context.Background())

	store.beforeSnapshot = func() {
		store.publish(testGK, storeapi.RawWatchEvent{
			Type:   beehive.WatchEventModified,
			Object: &beehive.RawObject{ID: 1, Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`), ResourceVersion: 1},
		})
	}

	w, err := store.WatchList(ctx, testGK) // empty snapshot, one buffered live event
	require.NoError(t, err)

	cancel() // goroutine parks on the live send (no reader) → takes ctx.Done
	<-exited
	_, ok := <-w.Events()
	assert.False(t, ok, "channel must be closed after the goroutine exits")
}
