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
	"sync/atomic"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/internal/storeapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errWatchBoom = errors.New("boom")

// recvLogEvent waits for the next event-log run on w, failing on timeout/close.
func recvLogEvent(t *testing.T, w storeapi.EventWatcher) storeapi.Event {
	t.Helper()
	select {
	case ev, ok := <-w.Events():
		if !ok {
			t.Fatal("event watcher channel closed unexpectedly")
		}
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for log event")
		panic("unreachable")
	}
}

// assertNoLogEvent fails if any event-log run arrives on w within d.
func assertNoLogEvent(t *testing.T, w storeapi.EventWatcher, d time.Duration) {
	t.Helper()
	select {
	case ev, ok := <-w.Events():
		if ok {
			t.Fatalf("unexpected log event: %+v", ev)
		}
		t.Fatal("event watcher channel closed unexpectedly")
	case <-time.After(d):
	}
}

// mergeEvent keeps the higher-resource-version run, so a slow subscriber
// converges to a run's latest count/window rather than seeing every bump.
func TestMergeEvent(t *testing.T) {
	older := storeapi.Event{ID: 1, ResourceVersion: 5, Count: 1}
	newer := storeapi.Event{ID: 1, ResourceVersion: 7, Count: 3}

	got, keep := mergeEvent(older, newer)
	assert.True(t, keep)
	assert.EqualValues(t, 7, got.ResourceVersion)
	assert.Equal(t, 3, got.Count, "latest count wins")

	got, keep = mergeEvent(newer, older) // prev already newer
	assert.True(t, keep)
	assert.EqualValues(t, 7, got.ResourceVersion)
	assert.Equal(t, 3, got.Count)
}

// eventMatchesQuery bounds Since at stored (millisecond) precision, matching
// ListEvents' toMillis(Since) SQL bound, so a sub-millisecond Since doesn't drop a
// live run in that same millisecond that the snapshot would keep.
func TestEventMatchesQuerySincePrecision(t *testing.T) {
	const ms = int64(1_700_000_000_123)
	// Since carries a sub-millisecond remainder within the run's millisecond.
	q := storeapi.EventQuery{Since: fromMillis(ms).Add(700 * time.Microsecond)}

	atBoundary := storeapi.Event{LastAt: fromMillis(ms)}
	assert.True(t, eventMatchesQuery(atBoundary, q),
		"a run at the truncated-ms bound must pass, matching the snapshot query")

	earlier := storeapi.Event{LastAt: fromMillis(ms - 1)}
	assert.False(t, eventMatchesQuery(earlier, q), "a run a full millisecond earlier is filtered")
}

// WatchEvents delivers the object's current runs as a snapshot (oldest-first),
// then streams live runs.
func TestWatchEventsSnapshotThenLive(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	_, err := store.RecordEvent(ctx, testGK, id, storeapi.Event{Category: "c", Type: "Warning", Reason: "ProbeFailed"})
	require.NoError(t, err)
	_, err = store.RecordEvent(ctx, testGK, id, storeapi.Event{Category: "c", Type: "Normal", Reason: "Connected"})
	require.NoError(t, err)

	w, err := store.WatchEvents(ctx, testGK, id, storeapi.EventQuery{})
	require.NoError(t, err)
	defer w.Close()

	assert.Equal(t, "ProbeFailed", recvLogEvent(t, w).Reason, "snapshot oldest-first")
	assert.Equal(t, "Connected", recvLogEvent(t, w).Reason)

	_, err = store.RecordEvent(ctx, testGK, id, storeapi.Event{Category: "c", Type: "Warning", Reason: "TLSHandshake"})
	require.NoError(t, err)
	assert.Equal(t, "TLSHandshake", recvLogEvent(t, w).Reason, "streamed live after the snapshot")
}

// A run buffered in the race window whose object is deleted before the snapshot
// is stale (the snapshot is empty by deletion, not Limit truncation) and must not
// be delivered — there are no tombstones in this stream to clear a phantom.
func TestWatchEventsDropsRaceWindowRunsForDeletedObject(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	// Record a run then delete the object, both in the subscribe→snapshot window:
	// the run is buffered in the receiver, but the FK cascade removes it before the
	// snapshot reads, so ListEvents (and the object scope-check) see it gone.
	store.beforeSnapshot = func() {
		_, err := store.RecordEvent(ctx, testGK, id, storeapi.Event{Category: "c", Type: "Warning", Reason: "ProbeFailed"})
		require.NoError(t, err)
		require.NoError(t, store.DeleteObject(ctx, id))
	}

	w, err := store.WatchEvents(ctx, testGK, id, storeapi.EventQuery{})
	require.NoError(t, err)
	defer w.Close()

	assertNoLogEvent(t, w, 200*time.Millisecond)
}

// WatchEvents scopes its snapshot to gk: a foreign id's existing log must not
// leak through the snapshot, keeping it consistent with the gk-scoped live stream.
func TestWatchEventsScopesSnapshotToKind(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store) // belongs to testGK

	_, err := store.RecordEvent(ctx, testGK, id, storeapi.Event{Category: "c", Type: "Warning", Reason: "ProbeFailed"})
	require.NoError(t, err)

	// Watch the same id under a different kind: the snapshot must be empty, not the
	// testGK object's log.
	w, err := store.WatchEvents(ctx, beehive.GroupKind{Kind: "Other"}, id, storeapi.EventQuery{})
	require.NoError(t, err)
	defer w.Close()

	assertNoLogEvent(t, w, 200*time.Millisecond)
}

// A Limit bounds only the snapshot: a matching run committed in the
// subscribe→snapshot window but truncated from the (limited) snapshot must still
// stream live, not be dropped by the resource-version dedup.
func TestWatchEventsLimitDoesNotDropRaceWindowRuns(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	// Two distinct runs commit in the race window; both carry rv ≤ the snapshot's
	// high-water. With Limit 1 the snapshot carries only the newest (Second); the
	// older run (First) is excluded by the limit and must arrive live.
	store.beforeSnapshot = func() {
		_, err := store.RecordEvent(ctx, testGK, id, storeapi.Event{Category: "c", Type: "Warning", Reason: "First"})
		require.NoError(t, err)
		_, err = store.RecordEvent(ctx, testGK, id, storeapi.Event{Category: "c", Type: "Normal", Reason: "Second"})
		require.NoError(t, err)
	}

	w, err := store.WatchEvents(ctx, testGK, id, storeapi.EventQuery{Limit: 1})
	require.NoError(t, err)
	defer w.Close()

	got := map[string]bool{}
	got[recvLogEvent(t, w).Reason] = true
	got[recvLogEvent(t, w).Reason] = true
	assert.True(t, got["Second"], "snapshot run delivered")
	assert.True(t, got["First"], "limit-excluded race-window run must still stream live")
}

// The query filters the live stream too, not just the snapshot: an emission in
// another category never reaches a category-scoped subscriber.
func TestWatchEventsFiltersLiveByCategory(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)

	conn := "connection"
	w, err := store.WatchEvents(ctx, testGK, id, storeapi.EventQuery{Category: &conn})
	require.NoError(t, err)
	defer w.Close()

	// An out-of-category emission must be skipped, so the first delivered run is
	// the connection one that follows it.
	_, err = store.RecordEvent(ctx, testGK, id, storeapi.Event{Category: "sync", Type: "Normal", Reason: "Synced"})
	require.NoError(t, err)
	_, err = store.RecordEvent(ctx, testGK, id, storeapi.Event{Category: "connection", Type: "Warning", Reason: "ProbeFailed"})
	require.NoError(t, err)

	got := recvLogEvent(t, w)
	assert.Equal(t, "connection", got.Category)
	assert.Equal(t, "ProbeFailed", got.Reason)
}

// recvEvent waits for the next event on w, failing the test if none arrives or
// the channel closes within the failsafe timeout.
func recvEvent(t *testing.T, w beehive.Watcher) storeapi.RawChange {
	t.Helper()
	select {
	case ev, ok := <-w.Changes():
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
	case ev, ok := <-w.Changes():
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
		case _, ok := <-w.Changes():
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
	assert.Equal(t, beehive.Added, ev.Type)
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
	assert.Equal(t, beehive.Added, ev.Type)
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
	assert.Equal(t, beehive.Added, snap.Type)

	_, changed, err := store.RequestDeletion(ctx, testGK, obj.ID)
	require.NoError(t, err)
	require.True(t, changed)
	ev := recvEvent(t, w)
	assert.Equal(t, beehive.Modified, ev.Type)

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
		store.publish(testGK, storeapi.RawChange{Type: beehive.Modified, Object: obj})
	}

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()

	ev := recvEvent(t, w)
	assert.Equal(t, beehive.Added, ev.Type)
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

	_, err = store.UpdateSpec(ctx, testGK, obj.ID, []byte(`{"v":2}`), 0)
	require.NoError(t, err)

	ev := recvEvent(t, w)
	assert.Equal(t, beehive.Modified, ev.Type)
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

	_, err = store.UpdateSpec(ctx, testGK, obj2.ID, []byte(`{"v":2}`), 0) // filtered out
	require.NoError(t, err)
	_, err = store.UpdateSpec(ctx, testGK, obj1.ID, []byte(`{"v":2}`), 0) // delivered
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
	_, err = store.WatchChanges(context.Background(), testGK)
	require.ErrorIs(t, err, errStoreClosed)
}

// TestWatchChangesSkipsSnapshot verifies WatchChanges delivers no initial snapshot
// for pre-existing objects, but does stream subsequent live changes.
func TestWatchChangesSkipsSnapshot(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// A pre-existing object: WatchList would replay it as Added; WatchChanges must not.
	pre, err := store.CreateObject(ctx, newWatchObject())
	require.NoError(t, err)

	w, err := store.WatchChanges(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()
	assertNoEvent(t, w, 200*time.Millisecond)

	// A live change to that object streams through.
	_, err = store.UpdateSpec(ctx, testGK, pre.ID, []byte(`{"x":1}`), 0)
	require.NoError(t, err)
	ev := recvEvent(t, w)
	assert.Equal(t, beehive.Modified, ev.Type)
	assert.Equal(t, pre.ID, ev.Object.ID)
}

// TestWatchChangesStreamsLiveAdded verifies a newly created object reaches a
// WatchChanges subscriber as an Added event (only the initial snapshot is skipped).
func TestWatchChangesStreamsLiveAdded(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	w, err := store.WatchChanges(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()

	created, err := store.CreateObject(ctx, newWatchObject())
	require.NoError(t, err)
	ev := recvEvent(t, w)
	assert.Equal(t, beehive.Added, ev.Type)
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
	_, ok := <-w.Changes()
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
	assert.Equal(t, beehive.Deleted, ev.Type)
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
	_, ok := <-w.Changes()
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
		store.publish(testGK, storeapi.RawChange{Type: beehive.Modified,
			Object: &beehive.RawObject{ID: obj.ID, Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(spec), ResourceVersion: rv}})
	}
	mod(2, `{"v":2}`)
	mod(3, `{"v":3}`)

	first := recvEvent(t, w)
	assert.Equal(t, beehive.Added, first.Type)

	rec := recvEvent(t, w)
	assert.Equal(t, beehive.Modified, rec.Type)
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
	store.publish(testGK, storeapi.RawChange{Type: beehive.Modified,
		Object: &beehive.RawObject{ID: obj.ID, Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{"hello":"mutated"}`), ResourceVersion: 2}})
	store.publish(testGK, storeapi.RawChange{Type: beehive.Deleted,
		Object: &beehive.RawObject{ID: obj.ID, Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{"hello":"final"}`), ResourceVersion: 3}})

	first := recvEvent(t, w)
	assert.Equal(t, beehive.Added, first.Type)

	rec := recvEvent(t, w)
	assert.Equal(t, beehive.Deleted, rec.Type)
	assert.Equal(t, obj.ID, rec.Object.ID)
	assert.JSONEq(t, `{"hello":"final"}`, string(rec.Object.Spec))
}

// TestWatchSnapshotRaceDeleteNotLost verifies the P1 correctness property: an
// object created in the subscribe→snapshot race window (its Added is buffered
// before the snapshot is taken, and the snapshot includes it) must not lose a
// subsequent delete. The old annihilation in mergeChange would coalesce the
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
	store.publish(testGK, storeapi.RawChange{
		Type: beehive.Deleted,
		Object: &storeapi.RawObject{
			ID: created.ID, Group: testGK.Group, Kind: testGK.Kind,
			ResourceVersion: created.ResourceVersion + 1,
		},
	})

	snap := recvEvent(t, w)
	assert.Equal(t, beehive.Added, snap.Type)
	assert.Equal(t, created.ID, snap.Object.ID)

	del := recvEvent(t, w)
	assert.Equal(t, beehive.Deleted, del.Type)
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
	added := storeapi.RawChange{Type: beehive.Added,
		Object: &storeapi.RawObject{ID: id, ResourceVersion: 5}}
	deleted := storeapi.RawChange{Type: beehive.Deleted,
		Object: &storeapi.RawObject{ID: id, ResourceVersion: 6}}

	// Snapshot not yet loaded: membership unknown, so keep conservatively.
	got, keep := merge(added, deleted)
	require.True(t, keep)
	assert.Equal(t, beehive.Deleted, got.Type)

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
	assert.Equal(t, beehive.Deleted, got.Type)
}

// TestAnnihilatingMergeForEvents verifies the WatchChanges memory bound: with no
// snapshot to preserve (preserve == nil), every unobserved Added→Deleted pair is
// annihilated, while a non-delete coalescence still survives.
func TestAnnihilatingMergeForEvents(t *testing.T) {
	merge := annihilatingMerge(nil)
	const id storeapi.ObjectID = 7
	added := storeapi.RawChange{Type: beehive.Added,
		Object: &storeapi.RawObject{ID: id, ResourceVersion: 1}}
	deleted := storeapi.RawChange{Type: beehive.Deleted,
		Object: &storeapi.RawObject{ID: id, ResourceVersion: 2}}
	modified := storeapi.RawChange{Type: beehive.Modified,
		Object: &storeapi.RawObject{ID: id, ResourceVersion: 2}}

	// Unobserved create→delete: dropped entirely.
	_, keep := merge(added, deleted)
	assert.False(t, keep)

	// Create→modify still coalesces and survives (kept as Added, latest body).
	got, keep := merge(added, modified)
	require.True(t, keep)
	assert.Equal(t, beehive.Added, got.Type)
	assert.EqualValues(t, 2, got.Object.ResourceVersion)
}

// TestWatchSnapshotRaceModifiedNotAdded verifies that when a race-window Added
// and a post-snapshot Modified coalesce in the buffer (mergeChange preserves
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
	store.publish(testGK, storeapi.RawChange{
		Type: beehive.Modified,
		Object: &storeapi.RawObject{
			ID: created.ID, Group: testGK.Group, Kind: testGK.Kind,
			Spec:            []byte(`{"v":2}`),
			ResourceVersion: created.ResourceVersion + 1,
		},
	})

	snap := recvEvent(t, w)
	assert.Equal(t, beehive.Added, snap.Type)
	assert.Equal(t, created.ID, snap.Object.ID)

	mod := recvEvent(t, w)
	assert.Equal(t, beehive.Modified, mod.Type) // not a second Added
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

	// Inject Added then Deleted into the subscribe→snapshot window: the receiver is
	// registered but the stream goroutine hasn't started draining yet, so both land
	// in the conflating buffer and coalesce deterministically (publishing after
	// WatchList returns would race the goroutine draining the Added first). The
	// empty snapshot means id 99 was never observed, so the resulting lone Deleted
	// must be silently dropped rather than delivered.
	store.beforeSnapshot = func() {
		store.publish(testGK, storeapi.RawChange{
			Type:   beehive.Added,
			Object: &storeapi.RawObject{ID: 99, Group: testGK.Group, Kind: testGK.Kind, ResourceVersion: 1},
		})
		store.publish(testGK, storeapi.RawChange{
			Type:   beehive.Deleted,
			Object: &storeapi.RawObject{ID: 99, Group: testGK.Group, Kind: testGK.Kind, ResourceVersion: 2},
		})
	}

	w, err := store.WatchList(ctx, testGK)
	require.NoError(t, err)
	defer w.Close()

	assertNoEvent(t, w, 200*time.Millisecond)
}

// TestMergeChangeKeepsHigherResourceVersion verifies an out-of-order merge
// (prev's resource version exceeds next's) keeps the higher-versioned event as
// the newer lifecycle state.
func TestMergeChangeKeepsHigherResourceVersion(t *testing.T) {
	const id storeapi.ObjectID = 3
	prev := storeapi.RawChange{Type: beehive.Modified,
		Object: &storeapi.RawObject{ID: id, ResourceVersion: 9, Spec: []byte(`{"v":"new"}`)}}
	next := storeapi.RawChange{Type: beehive.Modified,
		Object: &storeapi.RawObject{ID: id, ResourceVersion: 4, Spec: []byte(`{"v":"old"}`)}}

	got, keep := mergeChange(prev, next)
	require.True(t, keep)
	assert.EqualValues(t, 9, got.Object.ResourceVersion, "higher-RV (prev) body wins")
	assert.Equal(t, []byte(`{"v":"new"}`), got.Object.Spec)
}

// dropObjectsTable removes the objects table while the connection stays open, so
// a later read inside an already-open transaction fails (BeginTx still succeeds,
// unlike closing the db) — exercising the snapshot load's inner error branches.
func dropObjectsTable(t *testing.T, store *sqliteStore) {
	t.Helper()
	_, err := store.db.ExecContext(context.Background(), `DROP TABLE objects`)
	require.NoError(t, err)
}

// TestSnapshotAtLoadError covers snapshotAt's load-error branch: the transaction
// opens (BeginTx succeeds) but the snapshot's ListObjects fails because the
// objects table is gone, so the error surfaces from load, not from BeginTx.
func TestSnapshotAtLoadError(t *testing.T) {
	store := newRawStore(t)
	store.beforeSnapshot = func() { dropObjectsTable(t, store) }

	_, err := store.WatchList(context.Background(), testGK)
	require.Error(t, err)
}

// TestWatchGetObjectInnerError covers Watch's snapshot GetObject error branch
// (the non-ErrNotFound path): the transaction opens, then GetObject fails on the
// missing objects table — distinct from a closed-db failure that aborts at BeginTx.
func TestWatchGetObjectInnerError(t *testing.T) {
	store := newRawStore(t)
	store.beforeSnapshot = func() { dropObjectsTable(t, store) }

	_, err := store.Watch(context.Background(), testGK, 1)
	require.Error(t, err)
}

// TestWatchOrphanTombstoneDropped covers the goroutine's orphan-tombstone guard:
// a single-object watch uses an id-scoped (non-annihilating) receiver, so a bare
// Deleted for the watched id reaches the stream loop. The id was never in the
// snapshot and no Added was delivered, so seenIDs lacks it and the Deleted is
// dropped rather than surfacing a spurious tombstone.
func TestWatchOrphanTombstoneDropped(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	const id storeapi.ObjectID = 99

	w, err := store.Watch(ctx, testGK, id) // empty snapshot (id absent), seenIDs empty
	require.NoError(t, err)
	defer w.Close()

	// RV 1 clears the empty snapshot's high-water (0) so the event reaches the
	// seenIDs switch, where the orphan tombstone is dropped.
	store.publish(testGK, storeapi.RawChange{
		Type:   beehive.Deleted,
		Object: &storeapi.RawObject{ID: id, Group: testGK.Group, Kind: testGK.Kind, ResourceVersion: 1},
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
		store.publish(testGK, storeapi.RawChange{
			Type:   beehive.Modified,
			Object: &beehive.RawObject{ID: 1, Group: testGK.Group, Kind: testGK.Kind, Spec: []byte(`{}`), ResourceVersion: 1},
		})
	}

	w, err := store.WatchList(ctx, testGK) // empty snapshot, one buffered live event
	require.NoError(t, err)

	cancel() // goroutine parks on the live send (no reader) → takes ctx.Done
	<-exited
	_, ok := <-w.Changes()
	assert.False(t, ok, "channel must be closed after the goroutine exits")
}

// WatchEvents on a closed store returns errStoreClosed (nil event hub).
func TestWatchEventsAfterCloseErrors(t *testing.T) {
	store := newRawStore(t)
	require.NoError(t, store.Close())
	_, err := store.WatchEvents(context.Background(), testGK, 1, storeapi.EventQuery{})
	require.ErrorIs(t, err, errStoreClosed)
}

// emitEvent outside a transaction publishes immediately (no collector on ctx).
func TestEmitEventOutsideTransaction(t *testing.T) {
	store := newRawStore(t)
	// No collector in ctx → the publishEvent path; no watcher, so the send drops.
	store.emitEvent(context.Background(), testGK, &storeapi.Event{ID: 1, ObjectID: 1})
}

// eventMatchesQuery filters a live run by type and by reason.
func TestEventMatchesQueryTypeAndReason(t *testing.T) {
	run := storeapi.Event{Category: "c", Type: "Warning", Reason: "ProbeFailed", LastAt: fromMillis(1)}
	assert.False(t, eventMatchesQuery(run, storeapi.EventQuery{Type: "Normal"}))
	assert.False(t, eventMatchesQuery(run, storeapi.EventQuery{Reason: "Other"}))
	assert.True(t, eventMatchesQuery(run, storeapi.EventQuery{Type: "Warning", Reason: "ProbeFailed"}))
}

// WatchEvents surfaces snapshot faults: the object scope-check and the list query.
func TestWatchEventsSnapshotErrors(t *testing.T) {
	t.Run("object scope check fails", func(t *testing.T) {
		store := newRawStore(t)
		id := newEventObject(t, store)
		store.beforeSnapshot = func() { dropObjectsTable(t, store) }
		_, err := store.WatchEvents(context.Background(), testGK, id, storeapi.EventQuery{})
		require.Error(t, err)
	})
	t.Run("snapshot list fails", func(t *testing.T) {
		store := newRawStore(t)
		id := newEventObject(t, store)
		store.beforeSnapshot = func() { dropEventsTable(t, store) }
		_, err := store.WatchEvents(context.Background(), testGK, id, storeapi.EventQuery{})
		require.Error(t, err)
	})
}

// The snapshot send exits on context cancellation (afterStream confirms exit).
func TestWatchEventsSnapshotSendCtxDone(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)
	_, err := store.RecordEvent(ctx, testGK, id, storeapi.Event{Category: "c", Type: "Normal", Reason: "R"})
	require.NoError(t, err)

	exited := make(chan struct{})
	store.afterStream = func() { close(exited) }
	wctx, cancel := context.WithCancel(ctx)
	w, err := store.WatchEvents(wctx, testGK, id, storeapi.EventQuery{})
	require.NoError(t, err)

	cancel() // goroutine parks on the snapshot send (no reader) → ctx.Done
	<-exited
	_, ok := <-w.Events()
	assert.False(t, ok)
}

// The live send exits on context cancellation. beforeSnapshot buffers a live run
// (RecvContext prefers a ready value over a cancelled ctx), so with an empty
// snapshot the goroutine reaches the live send and takes the ctx.Done arm.
func TestWatchEventsLiveSendCtxDone(t *testing.T) {
	store := newRawStore(t)
	id := newEventObject(t, store)
	exited := make(chan struct{})
	store.afterStream = func() { close(exited) }
	ctx, cancel := context.WithCancel(context.Background())

	store.beforeSnapshot = func() {
		store.publishEvent(testGK, storeapi.Event{
			ID: 1, ObjectID: id, Category: "c", Type: "Normal", Reason: "R",
			FirstAt: fromMillis(1), LastAt: fromMillis(1), ResourceVersion: 1 << 30,
		})
	}

	w, err := store.WatchEvents(ctx, testGK, id, storeapi.EventQuery{}) // empty snapshot, one buffered run
	require.NoError(t, err)

	cancel() // goroutine parks on the live send → ctx.Done
	<-exited
	_, ok := <-w.Events()
	assert.False(t, ok)
}

// The send exits when the store closes while parked (the s.done arm, distinct
// from ctx cancellation — closing the hub only wakes a receive, not a send).
func TestWatchEventsSendStoreClose(t *testing.T) {
	store := newRawStore(t)
	ctx := context.Background()
	id := newEventObject(t, store)
	_, err := store.RecordEvent(ctx, testGK, id, storeapi.Event{Category: "c", Type: "Normal", Reason: "R"})
	require.NoError(t, err)

	exited := make(chan struct{})
	store.afterStream = func() { close(exited) }
	w, err := store.WatchEvents(ctx, testGK, id, storeapi.EventQuery{})
	require.NoError(t, err)

	require.NoError(t, store.Close()) // goroutine parked on the snapshot send → s.done
	<-exited
	_, ok := <-w.Events()
	assert.False(t, ok)
}
