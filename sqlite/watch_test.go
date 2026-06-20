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

// TestWatchRecoversFromLag verifies that when a watcher's receiver falls behind
// the broadcast ring and drops events, the watcher does NOT terminate. It
// re-lists current state and re-emits it as Modified so consumers (and the
// dependency waker, which acts only on Modified) re-converge instead of silently
// losing the stream.
//
// The goroutine parks delivering its snapshot Added (no reader), which keeps it
// out of the receive loop while we overflow the ring; draining the snapshot then
// drops it into RecvContext, where it sees ErrLagged.
func TestWatchRecoversFromLag(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	obj, err := store.CreateObject(ctx, newWatchObject())
	require.NoError(t, err)

	w, err := store.Watch(ctx, testGK, obj.ID)
	require.NoError(t, err)
	defer w.Close()

	// Burst > ring capacity of events for an unrelated id; filterID drops them,
	// so they serve only to lag the receiver while the goroutine is parked.
	filler := &beehive.RawObject{ID: 999999, Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`), ResourceVersion: 1}
	for range hubBufferSize + 50 {
		store.publish(testGK, storeapi.RawWatchEvent{Type: beehive.WatchEventModified, Object: filler})
	}

	// Draining the snapshot Added lets the goroutine enter the receive loop and
	// hit ErrLagged.
	first := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventAdded, first.Type)
	assert.Equal(t, obj.ID, first.Object.ID)

	// Recovery: current state re-emitted as Modified, channel still open.
	rec := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventModified, rec.Type)
	assert.Equal(t, obj.ID, rec.Object.ID)
}

// TestWatchRelistEmitsDeleteForVanishedObject verifies that when an object is
// deleted during a lag gap (its Deleted event dropped from the ring), the
// recovery relist reports a Deleted tombstone for it so a cache-maintaining
// consumer does not retain the stale object forever. The row is gone, so the
// tombstone carries id + kind and a null (universally decodable) spec, not the
// last row.
func TestWatchRelistEmitsDeleteForVanishedObject(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()

	obj, err := store.CreateObject(ctx, newWatchObject())
	require.NoError(t, err)

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()

	// Delete the object: snapshot (captured at watch() time) still holds it, but
	// the live store no longer does. The Deleted event lands in the ring...
	require.NoError(t, store.DeleteObject(ctx, obj.ID))
	// ...then a burst > ring capacity drops it, forcing ErrLagged on recovery.
	filler := &beehive.RawObject{ID: 999999, Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`), ResourceVersion: 1}
	for range hubBufferSize + 50 {
		store.publish(testGK, storeapi.RawWatchEvent{Type: beehive.WatchEventModified, Object: filler})
	}

	// Drain the snapshot Added so the goroutine enters the receive loop, hits
	// ErrLagged, and relists.
	first := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventAdded, first.Type)
	assert.Equal(t, obj.ID, first.Object.ID)

	// Recovery relist finds the object gone and reports its Deleted tombstone:
	// id + kind, with a null spec that decodes into any Go type.
	rec := recvEvent(t, w)
	assert.Equal(t, beehive.WatchEventDeleted, rec.Type)
	assert.Equal(t, obj.ID, rec.Object.ID)
	assert.Equal(t, testGK.Kind, rec.Object.Kind)
	assert.JSONEq(t, "null", string(rec.Object.Spec))
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
