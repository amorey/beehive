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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amorey/gobus"
	"github.com/amorey/gobus/conflate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// flakyListStore fails the first n tail listings and then succeeds, so a test can
// prove a poll failure costs one tick rather than the whole stream.
type flakyListStore struct {
	Store
	failures atomic.Int64
}

func (s *flakyListStore) ObjectWritesListSince(ctx context.Context, gk GroupKind, afterRV int64, limit int) ([]ObjectWrite, int64, error) {
	if s.failures.Add(-1) >= 0 {
		return nil, 0, errBoom
	}
	return s.Store.ObjectWritesListSince(ctx, gk, afterRV, limit)
}

// A poll that fails is skipped, not fatal. Tearing the stream down would turn a
// transient store error into a subscriber that silently stops receiving — the
// failure mode a level-triggered surface exists to avoid, since there is no
// resubscribe for a consumer to notice it needs.
func TestWatchPollFailureCostsOneTickNotTheStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &flakyListStore{Store: newClientTestStore(t)}
	logger, buf := captureLogger(slog.LevelWarn)
	bh := newTestBeehive(t, store, fast(WithLogger(logger))...)
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	snap, ch, err := client.WatchList(ctx)
	require.NoError(t, err)
	require.Len(t, snap.Objects, 1, "the snapshot reports the object")
	require.Equal(t, obj.ID, snap.Objects[0].ID)

	// The next two listings fail. Only a tick that has something to list reaches
	// them, so give it a change to find.
	store.failures.Store(2)
	_, err = client.Update(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)

	ev := recv(t, ch)
	assert.Equal(t, Modified, ev.Type, "the stream survives the failed polls and reports the change")
	assert.Equal(t, "b", ev.Object.Spec.Val)
	assert.Contains(t, buf.String(), "watch tail step failed", "the skipped reads are reported")
}

// An object that has not changed since it was reported emits nothing. This is
// what makes the steady state silent: without the version comparison every tick
// would re-send the whole kind, and a subscriber could not tell a change from a
// heartbeat.
func TestWatchEmitsNothingWhileNothingChanges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newClientTestStore(t)
	bh := newTestBeehive(t, store, fast()...)
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	snap, ch, err := client.WatchList(ctx)
	require.NoError(t, err)
	require.Len(t, snap.Objects, 1, "the object is in the snapshot, not the stream")

	// A real change is the barrier. Many ticks pass while the object is untouched;
	// if any of them re-sent it, that Modified would arrive carrying the old spec
	// and this assertion would see it instead of the new one.
	_, err = client.Update(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)

	ev := recv(t, ch)
	assert.Equal(t, Modified, ev.Type)
	assert.Equal(t, "b", ev.Object.Spec.Val, "the only delivery after the snapshot is the real change")
}

// A physical removal is derived from the row's absence, not from a tombstone the
// store kept: the delete draws no version, so nothing in the write log records it.
// The change carries the object's last known state, which is its final state —
// nothing was written after it.
func TestWatchDerivesDeletedFromAbsence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newClientTestStore(t)
	bh := newTestBeehive(t, store, fast()...)
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "gone"})

	snap, ch, err := client.WatchList(ctx)
	require.NoError(t, err)
	require.Len(t, snap.Objects, 1, "the object is in the snapshot, not the stream")

	require.NoError(t, store.ObjectsDelete(ctx, obj.ID))

	ev := recv(t, ch)
	assert.Equal(t, Deleted, ev.Type)
	require.NotNil(t, ev.Object)
	assert.Equal(t, obj.ID, ev.Object.ID)
	assert.Equal(t, "gone", ev.Object.Spec.Val, "the tombstone carries the row's last known state")
}

// A single-object watch is kind-scoped like Get: another kind's id reads as
// absent rather than streaming that kind's rows through this client, where they
// would be decoded as this Spec.
func TestWatchSingleObjectIsKindScoped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newClientTestStore(t)
	bh := newTestBeehive(t, store, fast()...)
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	other := GroupKind{Kind: "Other"}
	_, err = Register(bh, other, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	foreign := mustCreate(t, ctx, NewClient[cSpec, cStatus](bh, other), "foreign", cSpec{Val: "foreign"})

	_, ch, err := NewClient[cSpec, cStatus](bh, clientTestGK).Watch(ctx, foreign.ID)
	require.NoError(t, err)

	// The barrier is this client's own object: it is created after the foreign one,
	// so anything the foreign id produced would have to arrive first.
	mine := mustCreate(t, ctx, NewClient[cSpec, cStatus](bh, clientTestGK), "mine", cSpec{Val: "mine"})
	mineSnap, _, err := NewClient[cSpec, cStatus](bh, clientTestGK).WatchList(ctx)
	require.NoError(t, err)
	require.Len(t, mineSnap.Objects, 1)
	require.Equal(t, mine.ID, mineSnap.Objects[0].ID)

	select {
	case ev := <-ch:
		t.Fatalf("a foreign id must stream nothing, got %+v", ev)
	default:
	}
}

// A single-object watch survives a store error the same way the list watch does.
// The read is per-tick, so a failure costs that tick; ending the stream would leave
// the subscriber with no way to learn it should resubscribe.
func TestWatchSingleObjectSurvivesAReadFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, bh, client, _ := watchFixture(t)
	logger, buf := captureLogger(slog.LevelWarn)
	bh.logger = logger

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	snap, ch, err := client.Watch(ctx, obj.ID)
	require.NoError(t, err)
	require.NotNil(t, snap.Object, "the object is in the snapshot, not the stream")

	// A change to find, and a read that fails while it tries. Wait for ticks that
	// come *after* the failure is armed, so the recovery below is the stream
	// outliving a failure rather than never meeting one.
	store.getErr.Store(true) // the tail's batched read of what changed
	_, err = client.Update(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)
	drainProbe(store.polled)
	waitClosed(t, chanAfter(store.polled, 2), "polls while the read fails")
	store.getErr.Store(false)

	ev := recv(t, ch)
	assert.Equal(t, Modified, ev.Type)
	assert.Equal(t, "b", ev.Object.Spec.Val)
	assert.Contains(t, buf.String(), "watch tail step failed", "the skipped read is reported")
}

// The cheap half of the poll can fail too. It runs only when the write cursor has
// not moved, so a failure there is the same one-tick cost as a failed listing —
// not a reason to end the stream.
func TestWatchSurvivesADeleteCheckFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, bh, client, _ := watchFixture(t)
	logger, buf := captureLogger(slog.LevelWarn)
	bh.logger = logger

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	snap, ch, err := client.WatchList(ctx)
	require.NoError(t, err)
	require.Len(t, snap.Objects, 1, "the object is in the snapshot, not the stream")

	// A write moves the position, so every tick from here reaches the tail's
	// listing — which now fails.
	store.listErr.Store(true)
	_, err = client.Update(ctx, obj.ID, cSpec{Val: "a2"})
	require.NoError(t, err)
	drainProbe(store.polled)
	waitClosed(t, chanAfter(store.polled, 2), "polls while the tail listing fails")
	store.listErr.Store(false)

	// A real change proves the stream is still live and still tailing.
	_, err = client.Update(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)
	assert.Equal(t, "b", recv(t, ch).Object.Spec.Val)
	assert.Contains(t, buf.String(), "watch tail step failed")
}

// A watch over an empty kind reports nothing and reads almost nothing: with no
// object ever reported there is no id set to compare, so the delete check answers
// without touching the store at all.
func TestWatchOverAnEmptyKindStaysQuiet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, _ := watchFixture(t)
	_, ch, err := client.WatchList(ctx)
	require.NoError(t, err)
	waitClosed(t, chanAfter(store.polled, 3), "three polls over the empty kind")

	select {
	case ev := <-ch:
		t.Fatalf("an empty kind must stream nothing, got %+v", ev)
	default:
	}
}

// A subscriber that stops reading must not wedge its poll goroutine. Cancelling is
// the only way out for a send nobody is receiving, so the stream has to abandon it
// and close rather than hold the value forever.
func TestWatchAbandonsASendWhenTheSubscriberGoesAway(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store, _, client, _ := watchFixture(t)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	_, ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	// A change after subscribing, which only the stream can carry. Nobody reads
	// ch, so the poll goroutine parks in the send. Waiting for the tail that
	// produced it is what puts the cancellation after the read and before the
	// send, which is the only place left that can observe it.
	drainProbe(store.tailed)
	_, err = client.Update(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)
	waitClosed(t, chanAfter(store.tailed, 1), "the tail that found the change")
	cancel()

	waitClosed(t, closedWhenDrained(ch), "the stream to close on cancellation")
}

// The same holds for a tombstone: a Deleted change is a send like any other, so a
// subscriber that has gone away must not strand the goroutine that derived it.
func TestWatchAbandonsATombstoneSendOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store, _, client, _ := watchFixture(t)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	snap, ch, err := client.WatchList(ctx)
	require.NoError(t, err)
	require.Len(t, snap.Objects, 1, "the object is in the snapshot, not the stream")

	// Remove the row outright and stop reading: the next poll tails the delete
	// entry, builds the tombstone from its row image, and parks in the send.
	drainProbe(store.tailed)
	require.NoError(t, store.ObjectsDelete(ctx, obj.ID))
	waitClosed(t, chanAfter(store.tailed, 1), "the tail that observes the removal")
	cancel()

	waitClosed(t, closedWhenDrained(ch), "the stream to close on cancellation")
}

// A removal is reported even when the row image will not decode, with a nil
// Object and the id.
//
// Two cases reach here and the tailer cannot tell them apart. A row that never
// decoded was never shown to the subscriber, so its removal is news to nobody.
// A row that decoded for a while and then stopped — a peer writes it at a schema
// version this binary cannot read, or the blob is corrupted — is one the
// subscriber holds. A physical delete is the last thing the log will ever say
// about an id, and ids are never reused, so withholding the Deleted strands that
// object in every mirror for good, where a redundant one is a no-op for any
// consumer keyed by id. Redundant beats missing, as everywhere else here.
func TestWatchReportsRemovalOfARowItCouldNeverDecode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, _ := watchFixture(t)
	poison, err := store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`not json`),
	})
	require.NoError(t, err)

	_, ch, err := client.WatchList(ctx)
	require.NoError(t, err)
	waitClosed(t, chanAfter(store.polled, 2), "the poll that quarantines the poison row")

	require.NoError(t, store.ObjectsDelete(ctx, poison.ID))

	// A good object created after the removal is the barrier: it can only arrive
	// after the poll that dropped the poison row from the stream's own bookkeeping.
	good := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "good"})

	ev := recv(t, ch)
	require.Equal(t, Deleted, ev.Type, "the removal went unreported")
	assert.Equal(t, poison.ID, ev.ID, "the id is what a mirror needs to drop it")
	assert.Nil(t, ev.Object, "there is no decodable state to carry")

	ev = recv(t, ch)
	assert.Equal(t, Added, ev.Type)
	assert.Equal(t, good.ID, ev.Object.ID)
}

// TestWatchStaysQuietThroughEventWrites pins what the quiet-tick gate asks. The
// event log draws its resource_version from the same sequence the objects do, so a
// gate that compared the sequence itself would be defeated by a single
// EventsAdd anywhere in the store — and a controller that records an event per
// reconcile, the shape examples/events encourages, would defeat it permanently,
// turning every subscriber into a full blob-bearing listing per tick.
func TestWatchStaysQuietThroughEventWrites(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	snap, ch, err := client.WatchList(ctx)
	require.NoError(t, err)
	require.Len(t, snap.Objects, 1, "the create is in the snapshot")
	drainProbe(store.listed)

	// An event write bumps the shared sequence and no objects row.
	require.NoError(t, cc.EventsAdd(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Probed"}))

	waitClosed(t, chanAfter(store.polled, 3), "three polls after the event write")
	select {
	case <-store.listed:
		t.Fatal("an event write must not make the object watch pay for a listing")
	default:
	}
	select {
	case ev := <-ch:
		t.Fatalf("an event write is not an object change, got %+v", ev)
	default:
	}
}

// TestWatchSingleObjectFindsADeleteWithoutListingTheKind pins the cheap half of a
// single-object poll. The watch's liveness probe is scoped to the one id it
// tracks, not to the kind: asking for every id of the kind would make the cheap
// half the expensive half and scale it with the kind rather than with the watch.
//
// The removal under test draws no version and does not move the write log's
// high-water mark — another object was written after the deletion mark — so the
// quiet path is the only thing that can notice it. The kind-wide listing is wired
// A single-object watch reports the whole delete lifecycle: the deletion request
// is an ordinary write and arrives as a Modified, and only the collection is a
// Deleted. The tail is the kind's, so another object's write in between must not
// disturb it.
func TestWatchSingleObjectReportsTheDeleteLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The fixture starts nothing, so every step below is the test's own: no sweeper
	// and no reconcile loop can collect the row out from under the ordering.
	store, bh, client, _ := watchFixture(t)
	watched := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "watched"})
	newer := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "newer"})

	snap, ch, err := client.Watch(ctx, watched.ID)
	require.NoError(t, err)
	require.NotNil(t, snap.Object)
	drainProbe(store.byIDs)

	require.NoError(t, client.Delete(ctx, watched.ID))
	pending := recv(t, ch)
	require.Equal(t, Modified, pending.Type, "the deletion request is an ordinary write")
	assert.NotNil(t, pending.Object.DeletionRequestedAt)

	// Another object's write, which this watch must not report.
	_, err = client.Update(ctx, newer.ID, cSpec{Val: "newest"})
	require.NoError(t, err)

	gone, err := bh.gcCollect(ctx, watched.ID)
	require.NoError(t, err)
	require.True(t, gone, "the row is finalizer-free, so the collect removes it")

	ev := recv(t, ch)
	assert.Equal(t, Deleted, ev.Type)
	assert.Equal(t, watched.ID, ev.Object.ID)
	assert.Equal(t, "watched", ev.Object.Spec.Val, "the row image carries the final state")

	// The other object's write is read by the kind's shared tailer — the filter
	// is on the fan-out, not on the read — but it is never delivered here.
	select {
	case ev := <-ch:
		t.Fatalf("a single-object watch delivered another object: %+v", ev)
	default:
	}
}

// TestWatchTakesItsSnapshotBeforeReturning pins the guarantee that makes
// "subscribe, then act" safe: the stream reads current state before the
// subscribing call returns, so a change the caller makes next is measured against
// a snapshot that already exists.
//
// Without it the snapshot is taken by the first tick, and everything between
// subscribing and that tick is invisible — an object created and collected in the
// window is never reported at all, so a caller waiting for its Deleted waits
// forever. The poll interval here is an hour: nothing but the synchronous first
// read can deliver anything.
func TestWatchTakesItsSnapshotBeforeReturning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t), withWatchFloorInterval(time.Hour))
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	t.Run("list watch", func(t *testing.T) {
		snap, _, err := client.WatchList(ctx)
		require.NoError(t, err)
		require.Len(t, snap.Objects, 1)
		assert.Equal(t, obj.ID, snap.Objects[0].ID)
	})

	t.Run("single-object watch", func(t *testing.T) {
		snap, _, err := client.Watch(ctx, obj.ID)
		require.NoError(t, err)
		require.NotNil(t, snap.Object)
		assert.Equal(t, obj.ID, snap.Object.ID)
	})
}

// TestWatchReportsAFailedFirstRead pins the other half of the subscribe-then-act
// guarantee: a stream is handed back only when it holds a snapshot. A failed first
// read returns an error instead, because the alternative is a watch whose guarantee
// is quietly void — it would hold no state to compare against, so an object deleted
// next would never be reported, and the caller would wait for a tombstone that
// cannot come. Every *later* failure costs one tick, since the last good poll's
// state is still there.
func TestWatchReportsAFailedFirstRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, _ := watchFixture(t)
	mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	store.listErr.Store(true)
	_, ch, err := client.WatchList(ctx)
	require.ErrorIs(t, err, errBoom, "the caller learns the snapshot failed")
	assert.Nil(t, ch, "and gets no stream to wait on")
	assert.Contains(t, err.Error(), "initial read failed")

	// The single-object watch reads its own one-row snapshot, so it answers the
	// same way.
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "b"})
	store.getErr.Store(true)
	_, ch, err = client.Watch(ctx, obj.ID)
	require.ErrorIs(t, err, errBoom)
	assert.Nil(t, ch)

	// It is the read that failed, not the subscription: with the store answering
	// again, subscribing works.
	store.listErr.Store(false)
	store.getErr.Store(false)
	snap, _, err := client.WatchList(ctx)
	require.NoError(t, err)
	assert.Len(t, snap.Objects, 2)
}

// A context already cancelled at subscribe is the same story told by the store:
// the snapshot read fails, so there is no stream to hand back.
func TestWatchOnACancelledContextDoesNotSubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, client, _ := watchFixture(t)
	_, _, err := client.WatchList(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

// The tail's floor falls back the same way, with more at stake: a zero there
// makes the timer fire in a busy loop, not just a stream that never emits.
func TestWatchFloorFallsBackToTheDefault(t *testing.T) {
	assert.Equal(t, defaultWatchFloorInterval, (&Beehive{}).watchFloor(),
		"an unset floor reads as the default rather than as no wait at all")

	bh := newTestBeehive(t, &fakeStore{}, withWatchFloorInterval(fastTick))
	assert.Equal(t, fastTick, bh.watchFloor(), "a configured floor is used as given")
}

// The snapshot leaves the stream. A subscriber holds current state before it
// reads the first change, so "am I synced?" is a value rather than a guess about
// indistinguishable Added changes.
func TestWatchListReturnsASnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bh := newTestBeehive(t, newClientTestStore(t), withWatchFloorInterval(time.Millisecond))
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	defer func() { assert.NoError(t, stop(ctx)) }()
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	before := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	snap, ch, err := client.WatchList(ctx)
	require.NoError(t, err)
	require.Len(t, snap.Objects, 1, "current state, in hand before the first change")
	assert.Equal(t, before.ID, snap.Objects[0].ID)
	assert.GreaterOrEqual(t, snap.ResourceVersion, before.ResourceVersion)

	after := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "b"})

	ev := recv(t, ch)
	assert.Equal(t, Added, ev.Type)
	assert.Equal(t, after.ID, ev.Object.ID, "the stream carries only what the snapshot missed")
}

// An owner-scoped snapshot holds that owner's children and nothing else — not a
// sibling owner's, and not an unowned object.
func TestOwnedObjectsListWatchSnapshotsOnlyTheOwnersChildren(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bh := newTestBeehive(t, newClientTestStore(t), withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	owner := mustCreate(t, ctx, client, "owner", cSpec{})
	other := mustCreate(t, ctx, client, "other", cSpec{})
	mine := mustCreate(t, ctx, client, "mine", cSpec{}, WithOwner(owner.ID))
	mustCreate(t, ctx, client, "theirs", cSpec{}, WithOwner(other.ID))
	mustCreate(t, ctx, client, "orphan", cSpec{})

	snap, _, err := client.OwnedObjectsListWatch(ctx, owner.ID)
	require.NoError(t, err)

	require.Len(t, snap.Objects, 1)
	assert.Equal(t, mine.ID, snap.Objects[0].ID)
	assert.GreaterOrEqual(t, snap.ResourceVersion, mine.ResourceVersion)
}

// A child created after the snapshot arrives as Added. This is the case a
// denormalised owner column gets wrong: the create's log entry is appended
// before its owner edge exists.
func TestOwnedObjectsListWatchDeliversALaterChild(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bh := newTestBeehive(t, newClientTestStore(t), fast()...)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	owner := mustCreate(t, ctx, client, "owner", cSpec{})

	_, ch, err := client.OwnedObjectsListWatch(ctx, owner.ID)
	require.NoError(t, err)

	child := mustCreate(t, ctx, client, "child", cSpec{Val: "a"}, WithOwner(owner.ID))

	ev := recv(t, ch)
	assert.Equal(t, Added, ev.Type)
	assert.Equal(t, child.ID, ev.Object.ID)
}

// Everything outside the scope is silent: another owner's child, and an object
// with no owner at all.
func TestOwnedObjectsListWatchIgnoresWhatItDoesNotOwn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bh := newTestBeehive(t, newClientTestStore(t), fast()...)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	owner := mustCreate(t, ctx, client, "owner", cSpec{})
	other := mustCreate(t, ctx, client, "other", cSpec{})

	_, ch, err := client.OwnedObjectsListWatch(ctx, owner.ID)
	require.NoError(t, err)

	mustCreate(t, ctx, client, "theirs", cSpec{}, WithOwner(other.ID))
	mustCreate(t, ctx, client, "orphan", cSpec{})
	// Written last, so anything ahead of it in the stream is a leak.
	mine := mustCreate(t, ctx, client, "mine", cSpec{}, WithOwner(owner.ID))

	ev := recv(t, ch)
	assert.Equal(t, mine.ID, ev.Object.ID, "only the owner's own child is delivered")
}

// A collected child is still the owner's. Its owned_by edge cascades away with
// the row, so the scope is decided from the log entry's image — the only place
// the owner survives.
func TestOwnedObjectsListWatchReportsACollectedChild(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, bh, client, _ := watchFixture(t)
	owner := mustCreate(t, ctx, client, "owner", cSpec{})
	child := mustCreate(t, ctx, client, "child", cSpec{Val: "final"}, WithOwner(owner.ID))

	_, ch, err := client.OwnedObjectsListWatch(ctx, owner.ID)
	require.NoError(t, err)

	require.NoError(t, client.Delete(ctx, child.ID))
	pending := recv(t, ch)
	require.Equal(t, Modified, pending.Type, "the deletion request is an ordinary write")

	gone, err := bh.gcCollect(ctx, child.ID)
	require.NoError(t, err)
	require.True(t, gone)

	ev := recv(t, ch)
	assert.Equal(t, Deleted, ev.Type)
	assert.Equal(t, child.ID, ev.Object.ID)
	assert.Equal(t, "final", ev.Object.Spec.Val, "the row image carries the final state")
}

// A collected child whose spec cannot be decoded is still reported: the scope
// comes off the resolved owner, not off the object that failed to decode, so
// quarantining the body must not also drop the removal.
func TestOwnedObjectsListWatchReportsAnUndecodableCollectedChild(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, _, client, _ := watchFixture(t)
	owner := mustCreate(t, ctx, client, "owner", cSpec{})
	poison, err := store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`not json`),
	})
	require.NoError(t, err)
	_, err = store.EdgesAdd(ctx, poison.ID, owner.ID, RelationOwnedBy)
	require.NoError(t, err)

	_, ch, err := client.OwnedObjectsListWatch(ctx, owner.ID)
	require.NoError(t, err)

	require.NoError(t, store.ObjectsDelete(ctx, poison.ID))

	ev := recv(t, ch)
	require.Equal(t, Deleted, ev.Type, "the removal went unreported")
	assert.Equal(t, poison.ID, ev.ID)
	assert.Nil(t, ev.Object, "the body is quarantined, the removal is not")
}

// A resume replays the gap scoped the same way a live stream is, collected
// children included.
func TestOwnedObjectsListWatchResumesFromAPosition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, bh, client, _ := watchFixture(t)
	owner := mustCreate(t, ctx, client, "owner", cSpec{})
	other := mustCreate(t, ctx, client, "other", cSpec{})

	snap, _, err := client.OwnedObjectsListWatch(ctx, owner.ID)
	require.NoError(t, err)

	// All of this lands in the gap the resume below has to replay.
	mustCreate(t, ctx, client, "theirs", cSpec{}, WithOwner(other.ID))
	mustCreate(t, ctx, client, "orphan", cSpec{})
	mine := mustCreate(t, ctx, client, "mine", cSpec{Val: "a"}, WithOwner(owner.ID))
	doomed := mustCreate(t, ctx, client, "doomed", cSpec{Val: "b"}, WithOwner(owner.ID))
	require.NoError(t, client.Delete(ctx, doomed.ID))
	gone, err := bh.gcCollect(ctx, doomed.ID)
	require.NoError(t, err)
	require.True(t, gone)

	_, ch, err := client.OwnedObjectsListWatch(ctx, owner.ID, WithResumeFrom(snap.ResourceVersion))
	require.NoError(t, err)

	first := recv(t, ch)
	assert.Equal(t, Added, first.Type)
	assert.Equal(t, mine.ID, first.Object.ID, "the siblings outside the scope are not replayed")

	second := recv(t, ch)
	assert.Equal(t, Deleted, second.Type)
	assert.Equal(t, doomed.ID, second.ID)
}

// A nil owner means "unowned" and "not resolved" alike, and only the first is
// legitimate. The second silently drops the change forever, so it announces
// itself: this is the sticky owner gate's continuous assertion, in place of one
// test of one interleaving.
func TestAScopedWatchAnnouncesAnUnresolvedOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger, buf := captureLogger(slog.LevelWarn)
	store := &edgelessStore{Store: newClientTestStore(t), failed: make(chan struct{}, 256)}
	bh := newTestBeehive(t, store, fast()...)
	bh.logger = logger
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	owner := mustCreate(t, ctx, client, "owner", cSpec{})

	_, ch, err := client.OwnedObjectsListWatch(ctx, owner.ID)
	require.NoError(t, err)

	store.blind.Store(true)
	dropped := mustCreate(t, ctx, client, "dropped", cSpec{}, WithOwner(owner.ID))
	// The blinded lookup is the drain that carried the create, so by the time it
	// fires that change has been resolved to nothing.
	waitClosed(t, chanAfter(store.failed, 1), "the drain that cannot resolve an owner")

	// A second child, resolvable, is the barrier: receiving it proves the batch
	// holding the first has been through decodeChanges.
	store.blind.Store(false)
	delivered := mustCreate(t, ctx, client, "delivered", cSpec{}, WithOwner(owner.ID))

	ev := recv(t, ch)
	assert.Equal(t, delivered.ID, ev.Object.ID)
	assert.NotEqual(t, dropped.ID, ev.Object.ID, "an unresolved change is not delivered")
	assert.Contains(t, buf.String(), "unresolved owner")
}

// A scoped watch already knows every delivered object's owner — matching on it
// is how the object got here — so LoadOwner costs it no second query.
func TestOwnedObjectsListWatchLoadsTheOwnerItAlreadyResolved(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &countingLoadStore{Store: newClientTestStore(t)}
	bh := newTestBeehive(t, store, fast()...)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	owner := mustCreate(t, ctx, client, "owner", cSpec{})

	snap, ch, err := client.OwnedObjectsListWatch(ctx, owner.ID, WithLoads(LoadOwner()))
	require.NoError(t, err)
	require.Empty(t, snap.Objects)
	before := store.relationReads.Load()

	child := mustCreate(t, ctx, client, "child", cSpec{}, WithOwner(owner.ID))

	ev := recv(t, ch)
	require.Equal(t, child.ID, ev.Object.ID)
	got, ok, err := ev.Object.Owner()
	require.NoError(t, err, "the relation the watch asked for is loaded")
	require.True(t, ok)
	assert.Equal(t, owner.ID, got.ID)
	assert.Equal(t, before+1, store.relationReads.Load(), "the tailer's own read, and no second one")
}

// The owner read is part of the page, so a failed one costs a retry rather than
// the stream — like any other poll failure. The cursor has not moved, so the
// retry delivers exactly what the failed drain could not.
func TestAScopedWatchSurvivesAFailedOwnerRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &edgelessStore{Store: newClientTestStore(t), failed: make(chan struct{}, 256)}
	bh := newTestBeehive(t, store, fast()...)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	owner := mustCreate(t, ctx, client, "owner", cSpec{})

	_, ch, err := client.OwnedObjectsListWatch(ctx, owner.ID)
	require.NoError(t, err)

	store.broken.Store(true)
	child := mustCreate(t, ctx, client, "child", cSpec{Val: "a"}, WithOwner(owner.ID))
	waitClosed(t, chanAfter(store.failed, 1), "the drain that cannot read owners")

	store.broken.Store(false)
	ev := recv(t, ch)
	assert.Equal(t, child.ID, ev.Object.ID)
}

// An owner with no children yet is not an error: a watch is opened before the
// children it is waiting for exist.
func TestOwnedObjectsListWatchOverAChildlessOwnerStaysQuiet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bh := newTestBeehive(t, newClientTestStore(t), withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	owner := mustCreate(t, ctx, client, "owner", cSpec{})

	snap, ch, err := client.OwnedObjectsListWatch(ctx, owner.ID)
	require.NoError(t, err)
	assert.Empty(t, snap.Objects)
	assert.NotZero(t, snap.ResourceVersion)
	assert.NotNil(t, ch)
}

// The tail reads what the log says changed, not the whole kind. That is the
// whole point of the log: a tick costs what moved, not what exists.
func TestObjectStreamTailsTheWriteLog(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, _, client, _ := watchFixture(t)
	quiet := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "quiet"})
	busy := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "busy"})

	snap, ch, err := client.WatchList(ctx)
	require.NoError(t, err)
	require.Len(t, snap.Objects, 2)
	drainProbe(store.listed)
	drainProbe(store.byIDs)

	_, err = client.Update(ctx, busy.ID, cSpec{Val: "busy2"})
	require.NoError(t, err)

	ev := recv(t, ch)
	require.Equal(t, Modified, ev.Type)
	require.Equal(t, busy.ID, ev.Object.ID)

	select {
	case ids := <-store.byIDs:
		assert.Equal(t, []ObjectID{busy.ID}, ids, "only the changed object is read")
	case <-time.After(testTimeout):
		t.Fatal("the tail never read the changed object")
	}
	select {
	case <-store.listed:
		t.Fatal("a tick must not list the kind")
	default:
	}
	assert.NotEqual(t, quiet.ID, ev.Object.ID)
}

// A Deleted change is built from the log entry's row image. Nothing else can
// supply it: the row is gone and its conditions cascaded with it.
func TestDeletedChangeComesFromTheLogImage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "doomed"})
	require.NoError(t, cc.ConditionsSet(ctx, obj.ID, Condition{
		Type: "Ready", Status: ConditionTrue, Reason: "Settled",
	}))

	_, ch, err := client.WatchList(ctx)
	require.NoError(t, err)
	store.listIDsErr.Store(true) // the liveness probe is gone; using it would fail here

	require.NoError(t, store.ObjectsDelete(ctx, obj.ID))

	ev := recv(t, ch)
	require.Equal(t, Deleted, ev.Type)
	require.NotNil(t, ev.Object)
	assert.Equal(t, obj.ID, ev.Object.ID)
	assert.Equal(t, "doomed", ev.Object.Spec.Val)
	assert.Len(t, ev.Object.Conditions, 1, "the image carries what cascaded away")
}

// Retention can trim past a live stream's cursor between ticks, and a tail that
// just read an empty page would skip those changes silently — the exact failure
// ErrWatchTooOld exists to prevent. The check rides the same read as the page.
func TestTrimUnderALiveStreamEndsIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, _, client, _ := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	snap, ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	// The horizon moves above where this stream is parked.
	store.forceTrimmed.Store(snap.ResourceVersion + 1)
	_, err = client.Update(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)

	ev := recv(t, ch)
	assert.Equal(t, Failed, ev.Type)
	assert.ErrorIs(t, ev.Err, ErrWatchTooOld)
	assert.Nil(t, ev.Object)
	waitClosed(t, closedWhenDrained(ch), "the stream to close behind the failure")
}

// The boundary, and it is the common case rather than an edge one: a kind that
// stops writing has its whole log age out, and the horizon converges onto exactly
// the position every live tail is parked at. Testing <= there would tear down
// every established watcher on every idle kind.
func TestAQuietKindIsNotTornDownByATrim(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, _, client, _ := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	snap, ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	// Trimmed through exactly where the tail sits: nothing it had not read is gone.
	store.forceTrimmed.Store(snap.ResourceVersion)
	_, err = client.Update(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)

	ev := recv(t, ch)
	assert.Equal(t, Modified, ev.Type)
	assert.Equal(t, "b", ev.Object.Spec.Val)
}

// A resumed stream takes no snapshot: it starts from the position the caller
// already holds and carries only what happened above it.
func TestWithResumeFromSkipsTheSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, _, client, _ := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	first, _, err := client.WatchList(ctx)
	require.NoError(t, err)
	drainProbe(store.listed)

	_, err = client.Update(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)

	snap, ch, err := client.WatchList(ctx, WithResumeFrom(first.ResourceVersion))
	require.NoError(t, err)
	assert.Empty(t, snap.Objects, "a resume reads no state")
	assert.Equal(t, first.ResourceVersion, snap.ResourceVersion)

	ev := recv(t, ch)
	assert.Equal(t, Modified, ev.Type)
	assert.Equal(t, "b", ev.Object.Spec.Val, "the change the caller missed")

	select {
	case <-store.listed:
		t.Fatal("a resume must not list the kind")
	default:
	}
}

// A resume below the horizon ends the stream rather than the call. It is the
// same failure a live stream reports for the same reason, so a caller handles
// ErrWatchTooOld in one place instead of two.
func TestResumeBelowTheHorizonEndsTheStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, _, client, _ := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	store.forceTrimmed.Store(obj.ResourceVersion + 10)

	_, ch, err := client.WatchList(ctx, WithResumeFrom(obj.ResourceVersion))
	require.NoError(t, err, "the position is the caller's; the call reads nothing to check it")

	ev := recv(t, ch)
	assert.Equal(t, Failed, ev.Type)
	assert.ErrorIs(t, ev.Err, ErrWatchTooOld)
	_, ok := <-ch
	assert.False(t, ok, "a Failed change is the last value before the channel closes")
}

// A watch can carry the same eager relations List does, batched per delivery so
// a stream does not become an N+1 of relation reads.
func TestWatchAppliesLoadOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _, client, _ := watchFixture(t)
	owner := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "owner"})
	child := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "child"}, WithOwner(owner.ID))

	snap, ch, err := client.WatchList(ctx, WithLoads(LoadOwner()))
	require.NoError(t, err)

	for _, obj := range snap.Objects {
		if obj.ID == child.ID {
			got, ok, err := obj.Owner()
			require.NoError(t, err, "the snapshot's objects carry the requested relation")
			require.True(t, ok)
			assert.Equal(t, owner.ID, got.ID)
		}
	}

	_, err = client.Update(ctx, child.ID, cSpec{Val: "child2"})
	require.NoError(t, err)

	ev := recv(t, ch)
	require.Equal(t, child.ID, ev.Object.ID)
	got, ok, err := ev.Object.Owner()
	require.NoError(t, err, "so do the stream's")
	require.True(t, ok)
	assert.Equal(t, owner.ID, got.ID)
}

// The tail needs no reconciler, so a kind nobody registered a controller for can
// still be watched. SchedulesWatch keeps its ErrNoController: a schedule is a
// reconciler's state, and a client-only kind has none.
func TestWatchNeedsNoController(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bh := newTestBeehive(t, newClientTestStore(t), fast()...)
	clientOnly := GroupKind{Group: "acme.com", Kind: "Unregistered"}
	client := NewClient[cSpec, cStatus](bh, clientOnly)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	snap, ch, err := client.WatchList(ctx)
	require.NoError(t, err)
	require.Len(t, snap.Objects, 1)

	_, err = client.Update(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)
	assert.Equal(t, "b", recv(t, ch).Object.Spec.Val)

	_, err = client.SchedulesWatch(ctx, obj.ID)
	assert.ErrorIs(t, err, ErrNoController, "a schedule still needs a reconciler")
}

// A create followed by an update inside one interval is still a create. The
// coalesced entry is the update, but the object was absent from the snapshot, so
// reporting Modified would hand a cache a change for an id it does not hold —
// and a controller writing status right after a create makes this the common
// case, not a rare one.
func TestCoalescedCreateThenUpdateStaysAdded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _, client, _ := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	_, err := client.Update(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)

	// Resuming from 0 puts both entries in one page, as one interval would.
	_, ch, err := client.WatchList(ctx, WithResumeFrom(0))
	require.NoError(t, err)

	ev := recv(t, ch)
	assert.Equal(t, Added, ev.Type, "the unread run began with a create")
	assert.Equal(t, "b", ev.Object.Spec.Val, "carrying current state, as every change does")
}

// A create and a delete in one interval still report Deleted: the row is gone,
// and the entry carries the state to report.
func TestCoalescedCreateThenDeleteReportsDeleted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, _, client, _ := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	require.NoError(t, store.ObjectsDelete(ctx, obj.ID))

	_, ch, err := client.WatchList(ctx, WithResumeFrom(0))
	require.NoError(t, err)

	ev := recv(t, ch)
	assert.Equal(t, Deleted, ev.Type)
	assert.Equal(t, obj.ID, ev.Object.ID)
}

// imagelessStore strips the row image off every delete entry, standing in for a
// backend that breaks ObjectWritesListSince's atomicity contract.
type imagelessStore struct {
	Store
}

func (s *imagelessStore) ObjectWritesListSince(ctx context.Context, gk GroupKind, afterRV int64, limit int) ([]ObjectWrite, int64, error) {
	page, trimmed, err := s.Store.ObjectWritesListSince(ctx, gk, afterRV, limit)
	for i := range page {
		page[i].Final = nil
	}
	return page, trimmed, err
}

// A delete entry with no row image is quarantined like any other undecodable
// row, not dereferenced. Store is a public extension point, so a backend that
// breaks the atomicity contract must cost one change, never the process.
func TestADeleteWithNoImageIsQuarantined(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bh := newTestBeehive(t, &imagelessStore{newClientTestStore(t)}, fast()...)
	logger, buf := captureLogger(slog.LevelWarn)
	bh.logger = logger
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	doomed := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "doomed"})
	survivor := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "survivor"})

	_, ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	require.NoError(t, bh.store.ObjectsDelete(ctx, doomed.ID))
	_, err = client.Update(ctx, survivor.ID, cSpec{Val: "still here"})
	require.NoError(t, err)

	ev := recv(t, ch)
	assert.Equal(t, Modified, ev.Type, "the imageless delete is dropped, the next change is not")
	assert.Equal(t, survivor.ID, ev.Object.ID)
	assert.Contains(t, buf.String(), "Watch")
}

// edgelessStore refuses the batched relation read the eager loaders and the
// owner-scoped tailer share: broken errors it, so a watch that asked for
// relations cannot quietly deliver objects without them, and blind answers it
// empty, which is what an owner gate that failed to arm looks like from a
// subscriber's side.
type edgelessStore struct {
	Store
	broken atomic.Bool
	blind  atomic.Bool
	// failed fires after a refused load, so a test can wait for the tail to have
	// met the failure instead of watching a log buffer race.
	failed chan struct{}
}

func (s *edgelessStore) EdgesGroupOutgoingByID(ctx context.Context, ids []ObjectID, r Relation) (map[ObjectID][]ObjectRef, error) {
	switch {
	case s.broken.Load():
		probeSignal(s.failed)
		return nil, errBoom
	case s.blind.Load() && r == RelationOwnedBy:
		probeSignal(s.failed)
		return nil, nil
	}
	return s.Store.EdgesGroupOutgoingByID(ctx, ids, r)
}

// A watch that asked for relations fails rather than delivering objects whose
// accessors would report ErrNotLoaded. On the snapshot that is the call's own
// error; on a later batch it costs one tick, like any other poll failure.
func TestWatchSurfacesAFailedRelationLoad(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &edgelessStore{Store: newClientTestStore(t), failed: make(chan struct{}, 256)}
	store.broken.Store(true)
	bh := newTestBeehive(t, store, fast()...)
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	_, _, err = client.WatchList(ctx, WithLoads(LoadOwner()))
	require.ErrorIs(t, err, errBoom, "the snapshot's loads are part of its read")
	assert.Contains(t, err.Error(), "initial read failed")

	// Drain the snapshot's own signal, or the wait below would be satisfied by it
	// and the tail would never be observed failing at all.
	drainProbe(store.failed)

	// Resuming skips the snapshot, so only the tail can fail here.
	_, ch, err := client.WatchList(ctx, WithResumeFrom(0), WithLoads(LoadOwner()))
	require.NoError(t, err)
	waitClosed(t, chanAfter(store.failed, 1), "the tail to meet the failed load")
	select {
	case ev := <-ch:
		t.Fatalf("a batch whose loads failed must deliver nothing, got %+v", ev)
	default:
	}

	// One tick, not the stream: with the relation read answering again, the same
	// batch comes through.
	store.broken.Store(false)
	ev := recv(t, ch)
	assert.Equal(t, obj.ID, ev.Object.ID)
	_, ok, err := ev.Object.Owner()
	require.NoError(t, err, "and it carries the relation that was asked for")
	assert.False(t, ok, "this object has no owner")
}

// emptyPageStore reports a position above the cursor but hands back no entries,
// which a correct store cannot do. The tail must return quietly rather than
// indexing the last element of an empty page.
type emptyPageStore struct {
	Store
	gate   atomic.Int64
	listed chan struct{}
}

func (s *emptyPageStore) ObjectWritesMaxVersion(context.Context, GroupKind) (int64, error) {
	// Rises on every call, so it stays above the cursor the tailer seeds from
	// this same read. A constant would equal that cursor and gate every drain
	// out, leaving the test asserting quiet while exercising nothing.
	return s.gate.Add(1), nil
}

func (s *emptyPageStore) ObjectWritesListSince(context.Context, GroupKind, int64, int) ([]ObjectWrite, int64, error) {
	probeSignal(s.listed)
	return nil, 0, nil
}

func TestAnEmptyPageAboveTheCursorIsQuiet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &emptyPageStore{Store: newClientTestStore(t), listed: make(chan struct{}, 8)}

	bh := newTestBeehive(t, store, fast()...)
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	_, ch, err := client.WatchList(ctx)
	require.NoError(t, err)
	require.NoError(t, bh.kindWriteHub.Send(clientTestGK))

	// Gate on the read this test is about: asserting before the tail has read a
	// page above its cursor would pass against any implementation at all.
	select {
	case <-store.listed:
	case <-time.After(testTimeout):
		t.Fatal("the tail never read a page above its cursor")
	}

	select {
	case ev, ok := <-ch:
		t.Fatalf("nothing to report, got %+v (open=%v)", ev, ok)
	default:
	}
}

// vanishingStore answers the batched read with nothing, standing in for an
// object collected between the log read and the read of what it named.
type vanishingStore struct {
	Store
	read chan struct{}
}

func (s *vanishingStore) ObjectsListByIDs(context.Context, GroupKind, []ObjectID) ([]*RawObject, error) {
	probeSignal(s.read)
	return nil, nil
}

// An object the batched read no longer returns is skipped, not reported half
// built: its delete appended an entry of its own above this page, so it arrives
// as a Deleted on a later tick.
func TestAVanishedObjectIsSkipped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &vanishingStore{Store: newClientTestStore(t), read: make(chan struct{}, 256)}
	bh := newTestBeehive(t, store, fast()...)
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	_, ch, err := client.WatchList(ctx, WithResumeFrom(0))
	require.NoError(t, err)

	waitClosed(t, chanAfter(store.read, 1), "the tail to read what the page named")
	select {
	case ev := <-ch:
		t.Fatalf("nothing to report for a row that is gone, got %+v", ev)
	default:
	}
}

// A create publishes its kind's new log position, so a tailer never has to poll
// to learn that the kind moved.
func TestKindWriteHubPublishesOnCreate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t))
	rx, _ := bh.kindWriteHub.Watch(clientTestGK)
	defer rx.Close()

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	mustCreate(t, ctx, client, "w1", cSpec{})

	ev, err := rx.RecvContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, clientTestGK, ev.Key)
}

// The waker watches across every kind, because a depends_on edge may point at
// a kind it cannot name.
func TestKindWriteHubWatchAcrossTakesAnyKind(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t))
	rx, ok := bh.kindWriteHub.WatchAcross()
	require.True(t, ok)
	defer rx.Close()

	other := GroupKind{Group: "test", Kind: "Unwatched"}
	require.NoError(t, bh.kindWriteHub.Send(other))

	ev, err := rx.RecvContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, other, ev.Key, "the event names the kind that moved")
}

// One slot, so a burst across kinds is one wake. The waker re-reads its
// position from the store, so N wakes for N kinds would be N passes finding
// what one pass already found.
func TestKindWriteHubWatchAcrossCollapsesABurst(t *testing.T) {
	bh := newTestBeehive(t, newClientTestStore(t))
	rx, ok := bh.kindWriteHub.WatchAcross()
	require.True(t, ok)
	defer rx.Close()

	for _, kind := range []string{"A", "B", "C"} {
		require.NoError(t, bh.kindWriteHub.Send(GroupKind{Kind: kind}))
	}

	ev, err := rx.TryRecv()
	require.NoError(t, err)
	assert.Equal(t, GroupKind{Kind: "C"}, ev.Key, "the slot names the last kind to land")

	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty, "and the burst left nothing behind it")
}

// A Beehive built field by field has no hub, and the waker falls back to its
// floor tick there rather than dereferencing nil.
func TestKindWriteHubWatchAcrossReportsAZeroHub(t *testing.T) {
	var zero kindWriteHub

	rx, ok := zero.WatchAcross()
	assert.False(t, ok)
	assert.Nil(t, rx)
}

// Every path that appends an object_writes entry wakes the kind. The rows are
// the store's write-log call sites, not the public verbs: ConditionsSet and
// ConditionsDelete reach the log through bumpObject, which a verb-derived
// table misses. A write path missing from this table is one watchers see only
// on the floor tick.
func TestKindWriteHubPublishesOnEveryWrite(t *testing.T) {
	type writeCase struct {
		name string
		// setup's own wakes are drained before write runs, so a write that
		// forgets to publish fails even when its setup published.
		setup func(t *testing.T, ctx context.Context, w *writeWorld) ObjectID
		write func(t *testing.T, ctx context.Context, w *writeWorld, id ObjectID)
	}
	create := func(name string, opts ...Option) func(*testing.T, context.Context, *writeWorld) ObjectID {
		return func(t *testing.T, ctx context.Context, w *writeWorld) ObjectID {
			return mustCreate(t, ctx, w.client, name, cSpec{Val: "a"}, opts...).ID
		}
	}
	cases := []writeCase{
		{
			name: "create",
			write: func(t *testing.T, ctx context.Context, w *writeWorld, _ ObjectID) {
				mustCreate(t, ctx, w.client, "create", cSpec{})
			},
		},
		{
			name: "get-or-create creating",
			write: func(t *testing.T, ctx context.Context, w *writeWorld, _ ObjectID) {
				_, created, err := w.client.GetOrCreate(ctx, "goc", cSpec{})
				require.NoError(t, err)
				require.True(t, created)
			},
		},
		{
			name:  "update spec",
			setup: create("update"),
			write: func(t *testing.T, ctx context.Context, w *writeWorld, id ObjectID) {
				_, err := w.client.Update(ctx, id, cSpec{Val: "b"})
				require.NoError(t, err)
			},
		},
		{
			name:  "update spec by name",
			setup: create("update-by-name"),
			write: func(t *testing.T, ctx context.Context, w *writeWorld, _ ObjectID) {
				_, err := w.client.UpdateByName(ctx, "update-by-name", cSpec{Val: "b"})
				require.NoError(t, err)
			},
		},
		{
			name:  "update status",
			setup: create("status"),
			write: func(t *testing.T, ctx context.Context, w *writeWorld, id ObjectID) {
				require.NoError(t, w.ctrl.UpdateStatus(ctx, id, 1, cStatus{Val: "ok"}))
			},
		},
		{
			name:  "conditions set",
			setup: create("cond-set"),
			write: func(t *testing.T, ctx context.Context, w *writeWorld, id ObjectID) {
				require.NoError(t, w.ctrl.ConditionsSet(ctx, id, Condition{Type: "Ready", Status: ConditionTrue}))
			},
		},
		{
			name: "conditions delete",
			setup: func(t *testing.T, ctx context.Context, w *writeWorld) ObjectID {
				id := create("cond-del")(t, ctx, w)
				require.NoError(t, w.ctrl.ConditionsSet(ctx, id, Condition{Type: "Ready", Status: ConditionTrue}))
				return id
			},
			write: func(t *testing.T, ctx context.Context, w *writeWorld, id ObjectID) {
				require.NoError(t, w.ctrl.ConditionsDelete(ctx, id, "Ready"))
			},
		},
		{
			name:  "finalizer clear",
			setup: create("finalizer", WithFinalizers("f")),
			write: func(t *testing.T, ctx context.Context, w *writeWorld, id ObjectID) {
				require.NoError(t, w.ctrl.FinalizersDelete(ctx, id, "f"))
			},
		},
		{
			name:  "soft delete",
			setup: create("delete"),
			write: func(t *testing.T, ctx context.Context, w *writeWorld, id ObjectID) {
				require.NoError(t, w.client.Delete(ctx, id))
			},
		},
		{
			name:  "soft delete by name",
			setup: create("delete-by-name"),
			write: func(t *testing.T, ctx context.Context, w *writeWorld, _ ObjectID) {
				require.NoError(t, w.client.DeleteByName(ctx, "delete-by-name"))
			},
		},
		{
			name: "physical delete",
			setup: func(t *testing.T, ctx context.Context, w *writeWorld) ObjectID {
				id := create("collect")(t, ctx, w)
				require.NoError(t, w.client.Delete(ctx, id))
				return id
			},
			write: func(t *testing.T, ctx context.Context, w *writeWorld, id ObjectID) {
				deleted, err := w.bh.gcCollect(ctx, id)
				require.NoError(t, err)
				require.True(t, deleted)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()

			w := newWriteWorld(t)
			rx, _ := w.bh.kindWriteHub.Watch(clientTestGK)
			defer rx.Close()

			var id ObjectID
			if tc.setup != nil {
				id = tc.setup(t, ctx, w)
			}
			drainRecv(rx)

			tc.write(t, ctx, w, id)
			ev, err := rx.RecvContext(ctx)
			require.NoError(t, err, "write published no wake")
			assert.Equal(t, clientTestGK, ev.Key)
		})
	}
}

// The owner cascade marks children of several kinds in one call, so one wake on
// the caller's kind is not enough: it is routed by the refs the store returns.
// The collection is driven directly — DeletionRequestsCreateFromOwner has one
// caller, gcCollect, and Delete on the owner only marks the owner.
func TestKindWriteHubPublishesPerCascadedKind(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	gkChildA := GroupKind{Kind: "ChildA"}
	gkChildB := GroupKind{Kind: "ChildB"}

	w := newWriteWorld(t)
	owner := mustCreate(t, ctx, w.client, "owner", cSpec{})
	mustCreate(t, ctx, NewClient[cSpec, cStatus](w.bh, gkChildA), "a", cSpec{}, WithOwner(owner.ID))
	mustCreate(t, ctx, NewClient[cSpec, cStatus](w.bh, gkChildB), "b", cSpec{}, WithOwner(owner.ID))
	require.NoError(t, w.client.Delete(ctx, owner.ID))

	rxA, _ := w.bh.kindWriteHub.Watch(gkChildA)
	defer rxA.Close()
	rxB, _ := w.bh.kindWriteHub.Watch(gkChildB)
	defer rxB.Close()

	_, err := w.bh.gcCollect(ctx, owner.ID)
	require.NoError(t, err)

	_, err = rxA.RecvContext(ctx)
	assert.NoError(t, err, "cascade published no wake for ChildA")
	_, err = rxB.RecvContext(ctx)
	assert.NoError(t, err, "cascade published no wake for ChildB")
}

// A write that never commits wakes nobody: the publish rides AfterCommit, so a
// rollback discards it with the row it would have announced.
func TestKindWriteHubSilentOnRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	w := newWriteWorld(t)
	rx, _ := w.bh.kindWriteHub.Watch(clientTestGK)
	defer rx.Close()

	err := w.bh.store.Within(ctx, func(ctx context.Context) error {
		_, err := w.client.Create(ctx, "rolled-back", cSpec{})
		require.NoError(t, err)
		return errBoom
	})
	require.ErrorIs(t, err, errBoom)

	_, err = rx.TryRecv()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}

// Stop closes the wake sender so a blocked tailer ends instead of hanging. The
// watch machinery comes up in New, not Start, so it goes down either way.
func TestKindWriteHubClosesOnStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	t.Run("after start", func(t *testing.T) {
		bh, err := New(newClientTestStore(t), fast()...)
		require.NoError(t, err)
		stop, err := bh.Start(ctx)
		require.NoError(t, err)
		rx, _ := bh.kindWriteHub.Watch(clientTestGK)
		defer rx.Close()

		require.NoError(t, stop(ctx))
		_, err = rx.RecvContext(ctx)
		assert.ErrorIs(t, err, gobus.ErrClosed)
	})

	t.Run("never started", func(t *testing.T) {
		bh, err := New(newClientTestStore(t))
		require.NoError(t, err)
		rx, _ := bh.kindWriteHub.Watch(clientTestGK)
		defer rx.Close()

		require.NoError(t, bh.stop(ctx))
		_, err = rx.RecvContext(ctx)
		assert.ErrorIs(t, err, gobus.ErrClosed)
	})
}

// A commit wakes the tailer, which delivers without any interval elapsing.
func TestTailerDeliversOnWake(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	w := newWriteWorld(t)
	tailer, rx := startTailer(t, w.bh, clientTestGK)

	obj := mustCreate(t, ctx, w.client, "tailed", cSpec{Val: "a"})

	ev, err := rx.RecvContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, obj.ID, ev.Key)
	assert.Equal(t, WriteCreate, ev.Value.Op)
	require.NotNil(t, ev.Value.Object)
	assert.Equal(t, obj.ResourceVersion, ev.Value.Object.ResourceVersion)
	assert.Equal(t, tailer.gk, clientTestGK)
}

// writeDuringMaxVersionStore commits one write inside the first position read,
// after the read has taken its value — the ordering that loses a wake if the
// tailer registers its receiver second.
type writeDuringMaxVersionStore struct {
	Store
	once   sync.Once
	onRead func()
}

func (s *writeDuringMaxVersionStore) ObjectWritesMaxVersion(ctx context.Context, gk GroupKind) (int64, error) {
	at, err := s.Store.ObjectWritesMaxVersion(ctx, gk)
	s.once.Do(func() {
		if s.onRead != nil {
			s.onRead()
		}
	})
	return at, err
}

// The tailer registers its wake receiver before it reads its starting cursor.
// A write that commits in between is above the cursor and its wake is in the
// slot; in the other order the wake is lost and the object never delivered.
func TestTailerLosesNoWriteAtStartup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	store := &writeDuringMaxVersionStore{Store: newClientTestStore(t)}
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	var raced ObjectID
	store.onRead = func() { raced = mustCreate(t, ctx, client, "raced", cSpec{}).ID }

	_, rx := startTailer(t, bh, clientTestGK)

	ev, err := rx.RecvContext(ctx)
	require.NoError(t, err, "the write that raced startup was never delivered")
	assert.Equal(t, raced, ev.Key)
}

// countingTailStore counts the reads a tail step makes.
type countingTailStore struct {
	Store
	positionReads atomic.Int64
}

func (s *countingTailStore) ObjectWritesMaxVersion(ctx context.Context, gk GroupKind) (int64, error) {
	s.positionReads.Add(1)
	return s.Store.ObjectWritesMaxVersion(ctx, gk)
}

// A burst larger than one page drains on its own. The wakes collapse into one
// slot, so a tailer that read a single page per wake would strand the
// remainder until some unrelated later write. The drain loop stops on a short
// page rather than on a second position read, so a step costs one position
// read.
func TestTailerDrainsBurstAbovePageCap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	const burst = tailPageCap + 88

	store := &countingTailStore{Store: newClientTestStore(t)}
	bh := newTestBeehive(t, store)

	_, rx := startTailer(t, bh, clientTestGK)
	positionReadsAtStart := store.positionReads.Load()

	// The burst is written straight to the store, so it publishes no wakes of
	// its own; one manual wake follows. A burst collapses to one wake anyway,
	// and this keeps the read count deterministic.
	spec, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	for i := range burst {
		_, err := store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{
			Name: fmt.Sprintf("burst-%d", i),
			Spec: spec,
		})
		require.NoError(t, err)
	}
	require.NoError(t, bh.kindWriteHub.Send(clientTestGK))

	seen := make(map[ObjectID]bool, burst)
	for len(seen) < burst {
		ev, err := rx.RecvContext(ctx)
		require.NoError(t, err, "burst stalled after %d of %d", len(seen), burst)
		seen[ev.Key] = true
	}
	// One drain, so one position read, however many pages it takes: the gate
	// answers "is there anything?", and a full page answers "is there more?".
	assert.Equal(t, int64(1), store.positionReads.Load()-positionReadsAtStart)
}

// Two unread changes for one object coalesce by the merge table. Nothing is
// dropped: the pending slot is shared by all subscribers but each snapshot is
// its own, so dropping a create/delete pair would leave a subscriber that
// snapshotted between them holding a deleted object forever.
func TestTailerMergeTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	update := func(val string) func(*testing.T, context.Context, *writeWorld, ObjectID) {
		return func(t *testing.T, ctx context.Context, w *writeWorld, id ObjectID) {
			_, err := w.client.Update(ctx, id, cSpec{Val: val})
			require.NoError(t, err)
		}
	}
	softDelete := func(t *testing.T, ctx context.Context, w *writeWorld, id ObjectID) {
		require.NoError(t, w.client.Delete(ctx, id))
	}
	collect := func(t *testing.T, ctx context.Context, w *writeWorld, id ObjectID) {
		deleted, err := w.bh.gcCollect(ctx, id)
		require.NoError(t, err)
		require.True(t, deleted)
	}

	cases := []struct {
		name string
		// observed drains the create, so the pending slot starts empty and the
		// object is one the subscriber already holds.
		observed bool
		// Each write is one log entry, and the tailer publishes between them, so
		// the pair merges in the pending slot rather than inside one page.
		first, second func(*testing.T, context.Context, *writeWorld, ObjectID)
		wantOp        WriteOp
		wantVal       string
	}{
		{
			name:    "unobserved create then update reports the create with the newest state",
			second:  update("b"),
			wantOp:  WriteCreate,
			wantVal: "b",
		},
		{
			name:     "update then update reports the last",
			observed: true,
			first:    update("b"),
			second:   update("c"),
			wantOp:   WriteUpdate,
			wantVal:  "c",
		},
		{
			name:     "update then delete reports the delete",
			observed: true,
			first:    softDelete,
			second:   collect,
			wantOp:   WriteDelete,
		},
		{
			name:   "unobserved create then delete still reports the delete",
			first:  softDelete,
			second: collect,
			wantOp: WriteDelete,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWriteWorld(t)
			tailer, pending := startTailer(t, w.bh, clientTestGK)
			// A second receiver, read after every write, is how the test knows
			// the tailer published — pending must stay unread to merge.
			stepped := tailer.hub.Receiver()
			defer stepped.Close()

			id := mustCreate(t, ctx, w.client, "merged", cSpec{Val: "a"}).ID
			_, err := stepped.RecvContext(ctx)
			require.NoError(t, err)
			if tc.observed {
				drainRecv(pending)
			}

			for _, write := range []func(*testing.T, context.Context, *writeWorld, ObjectID){tc.first, tc.second} {
				if write == nil {
					continue
				}
				write(t, ctx, w, id)
				_, err = stepped.RecvContext(ctx)
				require.NoError(t, err)
			}

			ev, err := pending.RecvContext(ctx)
			require.NoError(t, err)
			assert.Equal(t, id, ev.Key)
			assert.Equal(t, tc.wantOp, ev.Value.Op)
			if tc.wantVal != "" {
				assert.Contains(t, string(ev.Value.Object.Spec), tc.wantVal)
			}
			// One slot per object: the merge left no second delivery behind.
			_, err = pending.TryRecv()
			assert.ErrorIs(t, err, gobus.ErrEmpty)
		})
	}
}

// A stale send — one the pending slot already covers — keeps the pending value
// and its queue position. Driven through the merge directly: the tailer itself
// cannot send a key out of order (one goroutine, a strictly rising cursor), so
// this guard is defense in depth against a second producer that does not exist
// yet, and only the merge can show it.
func TestMergeRawChangeKeepsThePendingValueOnAStaleSend(t *testing.T) {
	pending := rawChange{ID: 7, Op: WriteUpdate, ResourceVersion: 9}

	for _, rv := range []int64{8, 9} {
		t.Run(fmt.Sprintf("rv=%d", rv), func(t *testing.T) {
			got, keep := mergeRawChange(pending, rawChange{ID: 7, Op: WriteDelete, ResourceVersion: rv})
			assert.True(t, keep, "nothing is dropped")
			assert.Equal(t, pending, got, "a stale send must not overwrite a newer pending value")
		})
	}
}

// A failed step costs a retry, not the stream: the cursor did not advance, so
// the entries are still there to read.
func TestTailerRetriesAfterAFailedStep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	store := &flakyListStore{Store: newClientTestStore(t)}
	bh := newTestBeehive(t, store, withWatchFloorInterval(fastTick))
	_, rx := startTailer(t, bh, clientTestGK)

	store.failures.Store(1)
	obj := mustCreate(t, ctx, NewClient[cSpec, cStatus](bh, clientTestGK), "retried", cSpec{})

	ev, err := rx.RecvContext(ctx)
	require.NoError(t, err, "the tailer never retried the failed step")
	assert.Equal(t, obj.ID, ev.Key)
}

// The cursor is shared, so a cursor below the retention horizon is not one
// subscriber's problem: every subscriber ends and resubscribes onto a fresh
// snapshot. Advancing the cursor instead would silently drop changes for all of
// them.
func TestTailerResetsWhenItsCursorIsTrimmed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	store := &flakyListStore{Store: newClientTestStore(t)}
	bh := newTestBeehive(t, store, withWatchFloorInterval(fastTick))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	// Every step fails while the log grows, so the cursor stays where it began.
	store.failures.Store(math.MaxInt64)
	tailer, rx := startTailer(t, bh, clientTestGK)
	for i := range 3 {
		mustCreate(t, ctx, client, fmt.Sprintf("trimmed-%d", i), cSpec{})
	}
	// Retention overtakes the cursor, then the store recovers.
	_, err := store.ObjectWritesSweep(ctx, 1, 0)
	require.NoError(t, err)
	store.failures.Store(0)

	for {
		_, err = rx.RecvContext(ctx)
		if err != nil {
			break
		}
	}
	assert.ErrorIs(t, err, gobus.ErrClosed)
	assert.ErrorIs(t, tailer.failure(), ErrWatchTooOld)

	// A tailer started now is above the horizon and works.
	_, fresh := startTailer(t, bh, clientTestGK)
	obj := mustCreate(t, ctx, client, "after-reset", cSpec{})
	ev, err := fresh.RecvContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, obj.ID, ev.Key)
}

// pausedTailer builds a tailer and never starts it, so a test drives pass by
// hand.
func pausedTailer(t *testing.T, bh *Beehive, gk GroupKind) *objectTailer {
	t.Helper()
	tailer, err := newObjectTailer(context.Background(), bh, gk)
	require.NoError(t, err)
	t.Cleanup(tailer.close)
	return tailer
}

// pacedTailer is pausedTailer over a store, on a clock the test drives and a
// floor far enough away to tell the throttle's answers from it.
func pacedTailer(t *testing.T, store Store, throttle time.Duration) (*objectTailer, *fakeClock) {
	t.Helper()
	bh := newTestBeehive(t, store, withWatchFloorInterval(time.Hour), withWatchScanMinInterval(throttle))
	tailer := pausedTailer(t, bh, clientTestGK)
	return tailer, fakeClockOn(&tailer.now)
}

// seedWriteLog writes n objects straight to the store, so the log grows without
// publishing wakes of its own.
func seedWriteLog(t *testing.T, ctx context.Context, store Store, n int) {
	t.Helper()
	spec, err := json.Marshal(cSpec{})
	require.NoError(t, err)
	for i := range n {
		_, err := store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{
			Name: fmt.Sprintf("seed-%d", i),
			Spec: spec,
		})
		require.NoError(t, err)
	}
}

// pass is the loop's decision, split out so the cadence is asserted without
// waiting on it.
func TestTailerPassDecidesWhenToLookAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	t.Run("a drained log re-arms at the floor", func(t *testing.T) {
		bh := newTestBeehive(t, newClientTestStore(t), withWatchFloorInterval(time.Hour))
		tailer := pausedTailer(t, bh, clientTestGK)

		next, backingOff, done := tailer.pass(ctx, tailer.now(), false)
		assert.Equal(t, time.Hour, next)
		assert.False(t, backingOff)
		assert.False(t, done)
	})

	t.Run("a failed drain climbs the retry ladder and drops wakes", func(t *testing.T) {
		bh := newTestBeehive(t, &failGateStore{Store: newClientTestStore(t)}, withWatchFloorInterval(time.Hour))
		tailer := pausedTailer(t, bh, clientTestGK)

		next, backingOff, done := tailer.pass(ctx, tailer.now(), false)
		assert.Equal(t, watchRetryBase, next, "the retry is the only reason to look again")
		assert.True(t, backingOff, "a live writer must not keep a failing store re-reading")
		assert.False(t, done)

		next, _, _ = tailer.pass(ctx, tailer.now(), true)
		assert.Equal(t, 2*watchRetryBase, next, "a second failure waits longer")
	})

	t.Run("a trimmed cursor ends the tailer", func(t *testing.T) {
		store := newClientTestStore(t)
		bh := newTestBeehive(t, store, withWatchFloorInterval(time.Hour))
		client := NewClient[cSpec, cStatus](bh, clientTestGK)
		tailer := pausedTailer(t, bh, clientTestGK)

		for i := range 3 {
			mustCreate(t, ctx, client, fmt.Sprintf("trimmed-%d", i), cSpec{})
		}
		// Retention overtakes the cursor the tailer started from.
		_, err := store.ObjectWritesSweep(ctx, 1, 0)
		require.NoError(t, err)

		_, _, done := tailer.pass(ctx, tailer.now(), false)
		assert.True(t, done, "there is no single subscriber to hand the error to")
		assert.ErrorIs(t, tailer.failure(), ErrWatchTooOld)
	})
}

// The throttle bounds how much of the single connection a tailer holds away
// from the writers waking it. It is a floor on drain starts, not on delivery:
// a refused wake is remembered by the re-arm, since the drain that runs then
// reads its position from the store.
func TestTailerPassPacesTheLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	const throttle = 100 * time.Millisecond
	paced := func(t *testing.T) (*objectTailer, *countingTailStore, *fakeClock) {
		t.Helper()
		store := &countingTailStore{Store: newClientTestStore(t)}
		tailer, clk := pacedTailer(t, store, throttle)
		return tailer, store, clk
	}

	t.Run("a throttled pass waits out the throttle and reads nothing", func(t *testing.T) {
		tailer, store, clk := paced(t)

		tailer.pass(ctx, clk.now(), false)
		reads := store.positionReads.Load()

		clk.advance(throttle / 4)
		next, _, _ := tailer.pass(ctx, clk.now(), false)
		assert.Equal(t, 3*throttle/4, next, "re-armed for what is left of the throttle")
		assert.Equal(t, reads, store.positionReads.Load(), "and the refused pass read nothing")
	})

	// The property that lets the loop re-arm unconditionally: every refusal
	// inside one window returns a shorter remainder, so a commit stream cannot
	// push the deadline out.
	t.Run("refusals return a strictly decreasing remainder", func(t *testing.T) {
		tailer, _, clk := paced(t)

		tailer.pass(ctx, clk.now(), false)
		last := throttle
		for range 3 {
			clk.advance(throttle / 8)
			next, _, _ := tailer.pass(ctx, clk.now(), false)
			assert.Less(t, next, last)
			last = next
		}
	})

	t.Run("the first wake after a quiet period is eager", func(t *testing.T) {
		tailer, store, clk := paced(t)

		tailer.pass(ctx, clk.now(), false)
		reads := store.positionReads.Load()

		clk.advance(throttle)
		tailer.pass(ctx, clk.now(), false)
		assert.Equal(t, reads+1, store.positionReads.Load(), "an idle-to-active transition pays no added latency")
	})

	// The retry timer armed before a failure can fire inside the throttle
	// window; answering "not backing off" there would hand a degraded store back
	// to the wakes at the throttle's rate.
	t.Run("a throttled pass carries backingOff through", func(t *testing.T) {
		tailer, _, clk := paced(t)

		tailer.pass(ctx, clk.now(), false)
		clk.advance(throttle / 4)
		_, backingOff, _ := tailer.pass(ctx, clk.now(), true)
		assert.True(t, backingOff)
	})

	// A negative interval is a disabled throttle, not a sentinel: reporting
	// "finished" through the delay would kill every subscriber on the kind.
	t.Run("a non-positive interval disables the throttle", func(t *testing.T) {
		for _, d := range []time.Duration{0, -time.Second} {
			t.Run(d.String(), func(t *testing.T) {
				store := &countingTailStore{Store: newClientTestStore(t)}
				tailer, clk := pacedTailer(t, store, d)
				reads := store.positionReads.Load()

				for range 3 {
					_, _, done := tailer.pass(ctx, clk.now(), false)
					require.False(t, done)
				}
				assert.Equal(t, reads+3, store.positionReads.Load(), "every pass drains")
			})
		}
	})
}

// A budget bounds one drain, so a resume after a long gap cannot hold the
// single connection for as long as the backlog is deep. The remainder rides the
// cursor to the next drain, which the throttle paces.
func TestTailerStopsAtThePageBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	const throttle = 100 * time.Millisecond
	const backlog = tailPageCap + 88

	store := newClientTestStore(t)
	tailer, clk := pacedTailer(t, store, throttle)
	tailer.pagesPerDrain = 1
	rx := tailer.hub.Receiver()
	defer rx.Close()

	seedWriteLog(t, ctx, store, backlog)

	next, _, _ := tailer.pass(ctx, clk.now(), false)
	assert.Equal(t, throttle, next, "more work re-arms at the throttle, not the floor")
	assert.Len(t, drainedIDs(rx), tailPageCap, "one drain reads its budget and stops")

	// A budget of zero would read nothing and still report more, turning the
	// loop forever without draining. Only a sweep can set one.
	tailer.pagesPerDrain = 0
	more, err := tailer.drain(ctx)
	require.NoError(t, err)
	assert.False(t, more, "a zero budget reads a page rather than spinning")

	// The remainder is delivered by the next drain, in version order.
	clk.advance(throttle)
	next, _, _ = tailer.pass(ctx, clk.now(), false)
	assert.Equal(t, time.Hour, next, "a drained log re-arms at the floor")
	assert.Len(t, drainedIDs(rx), backlog-tailPageCap)
}

// The pacing tests drive pass by hand, so this is the one that runs the loop
// with the throttle on: a refusal must re-arm, or the write that arrived inside
// the window is never read. The floor is out of reach, so only the wake and the
// throttle's own re-arm can deliver the second write.
func TestTailerDeliversAWriteThatLandsInsideTheThrottle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t),
		withWatchFloorInterval(time.Hour), withWatchScanMinInterval(50*time.Millisecond))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	_, rx := startTailer(t, bh, clientTestGK)

	// The first drain is eager and admits the gate.
	first := mustCreate(t, ctx, client, "eager", cSpec{})
	ev, err := rx.RecvContext(ctx)
	require.NoError(t, err)
	require.Equal(t, first.ID, ev.Key)

	// The second lands inside the window, so its wake is refused; the re-arm is
	// the only thing that comes back for it.
	second := mustCreate(t, ctx, client, "throttled", cSpec{})
	ev, err = rx.RecvContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, second.ID, ev.Key)
}

// slowListStore spends clock on every page, so a drain has a duration to
// measure.
type slowListStore struct {
	Store
	clk     *fakeClock
	perPage time.Duration
}

func (s *slowListStore) ObjectWritesListSince(ctx context.Context, gk GroupKind, afterRV int64, limit int) ([]ObjectWrite, int64, error) {
	s.clk.advance(s.perPage)
	return s.Store.ObjectWritesListSince(ctx, gk, afterRV, limit)
}

// The throttle floors drain *starts*. Re-arming a budget-stopped drain for a
// whole interval would measure from its end instead, costing a resume its own
// duration of throughput on the path the budget exists to bound.
func TestTailerResumeWaitsOutOnlyTheRestOfTheThrottle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	const throttle = 100 * time.Millisecond
	for _, tc := range []struct {
		name    string
		perPage time.Duration
		want    time.Duration
	}{
		{"a drain inside the window waits out the rest", throttle / 4, 3 * throttle / 4},
		{"a drain past the window waits not at all", 2 * throttle, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &slowListStore{Store: newClientTestStore(t), perPage: tc.perPage}
			tailer, clk := pacedTailer(t, store, throttle)
			tailer.pagesPerDrain = 1
			store.clk = clk

			// Written above the tailer's starting cursor, so one page leaves more.
			seedWriteLog(t, ctx, store, tailPageCap+1)

			next, _, _ := tailer.pass(ctx, tailer.now(), false)
			assert.Equal(t, tc.want, next)
		})
	}
}

// drainedIDs takes everything the fan-out is holding, in delivery order.
func drainedIDs(rx *conflate.Receiver[ObjectID, rawChange]) []ObjectID {
	var ids []ObjectID
	for {
		ev, err := rx.TryRecv()
		if err != nil {
			return ids
		}
		ids = append(ids, ev.Key)
	}
}

// gateAheadStore reports a log position with no entries behind it — a Store
// that folds a wider counter into the position, or trims between the two
// reads.
type gateAheadStore struct {
	Store
	ahead int64
}

func (s *gateAheadStore) ObjectWritesMaxVersion(ctx context.Context, gk GroupKind) (int64, error) {
	at, err := s.Store.ObjectWritesMaxVersion(ctx, gk)
	return at + s.ahead, err
}

// A position above the cursor with no entries behind it costs one empty
// listing, not an index into an empty page. Store is a public extension point,
// so the tail cannot assume the two reads agree.
func TestTailerStepToleratesAGateAheadOfTheLog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, &gateAheadStore{Store: newClientTestStore(t), ahead: 1000})
	tailer, err := newObjectTailer(ctx, bh, clientTestGK)
	require.NoError(t, err)
	defer tailer.close()

	// Below the gate the empty log now reports, and above the horizon.
	tailer.cursor = 0

	// Through drain, which is where the gate lives.
	more, err := tailer.drain(ctx)
	require.NoError(t, err)
	assert.False(t, more)
	assert.Zero(t, tailer.cursor, "a drain that found nothing must not advance")
}

// failGateStore fails the log-position read once the tailer has built itself on
// a successful one.
type failGateStore struct {
	Store
	built atomic.Bool
}

func (s *failGateStore) ObjectWritesMaxVersion(ctx context.Context, gk GroupKind) (int64, error) {
	if s.built.Swap(true) {
		return 0, errBoom
	}
	return s.Store.ObjectWritesMaxVersion(ctx, gk)
}

// A failed gate read costs the step and nothing else: the cursor stays where it
// was, so the entries this step never saw are still above it for the retry.
func TestTailerStepReportsAFailedGateRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, &failGateStore{Store: newClientTestStore(t)}, withWatchFloorInterval(time.Hour))
	tailer, err := newObjectTailer(ctx, bh, clientTestGK)
	require.NoError(t, err)
	defer tailer.close()

	at := tailer.cursor
	_, err = tailer.drain(ctx)
	assert.ErrorIs(t, err, errBoom)
	assert.Equal(t, at, tailer.cursor, "a failed gate must not advance the cursor")
}

// A step that finds its fan-out closed stops publishing and reports no error:
// the beehive is stopping, and a cursor left where it was is the next tailer's
// to read.
func TestTailerStepStopsWhenTheFanOutIsClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t))
	tailer, err := newObjectTailer(ctx, bh, clientTestGK)
	require.NoError(t, err)
	defer tailer.close()

	// Written after the tailer read its cursor, so the step has a page to publish.
	mustCreate(t, ctx, NewClient[cSpec, cStatus](bh, clientTestGK), "unpublishable", cSpec{})
	at := tailer.cursor
	tailer.hub.Sender().Close()

	n, err := tailer.step(ctx)
	assert.NoError(t, err, "a closed fan-out is shutdown, not a failure to retry")
	assert.Zero(t, n)
	assert.Equal(t, at, tailer.cursor, "a step that could not publish must not advance")
}

// A tailer lives exactly as long as its subscribers. The last watch to end takes
// it down, so a kind watched once does not cost a goroutine and a floor-tick
// read for the rest of the process's life.
func TestTailerEndsWithItsLastSubscriber(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t), withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	firstCtx, endFirst := context.WithCancel(ctx)
	_, first, err := client.WatchList(firstCtx)
	require.NoError(t, err)
	secondCtx, endSecond := context.WithCancel(ctx)
	_, second, err := client.WatchList(secondCtx)
	require.NoError(t, err)
	require.Equal(t, 1, tailerCount(bh), "two watches on one kind share one tailer")

	// The stream closes after its lease is released, so draining it is the
	// signal that the release has happened — no sleep needed.
	endFirst()
	for range first { // draining is the signal: the stream closes after its release
	}
	assert.Equal(t, 1, tailerCount(bh), "a tailer with a subscriber left must stay")

	endSecond()
	for range second { // same signal
	}
	assert.Zero(t, tailerCount(bh), "the last subscriber left its tailer running")

	// And the kind is watchable again: the teardown left nothing dead behind for
	// the next watch to join.
	_, third, err := client.WatchList(ctx)
	require.NoError(t, err)
	obj := mustCreate(t, ctx, client, "after-teardown", cSpec{})
	assert.Equal(t, obj.ID, recv(t, third).Object.ID)
}

// A watch on a Beehive that was never Started ends with its caller. Nothing else
// can end it: stop is only reachable through the closure Start returns, so
// before the tailer's life was its subscribers' there was no way to stop these
// at all. TestMain's leak check is the other half of this assertion.
func TestWatchOnAnUnstartedBeehiveEndsWithItsCaller(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t), withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	watchCtx, endWatch := context.WithCancel(ctx)
	_, ch, err := client.WatchList(watchCtx)
	require.NoError(t, err)
	obj := mustCreate(t, ctx, client, "unstarted", cSpec{})
	require.Equal(t, obj.ID, recv(t, ch).Object.ID)

	endWatch()
	for range ch { // draining is the signal: the stream closes after its release
	}
	assert.Zero(t, tailerCount(bh), "nothing but the caller's context could have ended this")
}

// A resubscribe after a horizon reset must not rejoin the tailer that just
// failed. A dead tailer stays registered while any subscriber still holds a
// lease on it, so "present in the registry" and "usable" are different
// questions and tailerFor asks both.
func TestWatchAfterAResetJoinsAFreshTailer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	store := &flakyListStore{Store: newClientTestStore(t)}
	bh := newTestBeehive(t, store, withWatchFloorInterval(fastTick))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	// Two watches share the kind's tailer. The second never reads, so it still
	// holds its lease after the failure and the dead tailer stays registered.
	_, drained, err := client.WatchList(ctx)
	require.NoError(t, err)
	_, unread, err := client.WatchList(ctx)
	require.NoError(t, err)
	require.NotNil(t, unread)
	require.Equal(t, 1, tailerCount(bh), "both watches share one tailer")

	// Every step fails while the log grows, so the cursor stays put; then
	// retention overtakes it.
	store.failures.Store(math.MaxInt64)
	for i := range 3 {
		mustCreate(t, ctx, client, fmt.Sprintf("trimmed-%d", i), cSpec{})
	}
	_, err = store.ObjectWritesSweep(ctx, 1, 0)
	require.NoError(t, err)
	store.failures.Store(0)

	ev := recv(t, drained)
	require.Equal(t, Failed, ev.Type)
	require.ErrorIs(t, ev.Err, ErrWatchTooOld)
	require.Equal(t, 1, tailerCount(bh), "the unread subscriber still holds the dead tailer")

	// The resubscribe the caller is told to make. It must get a working stream.
	_, fresh, err := client.WatchList(ctx)
	require.NoError(t, err)
	obj := mustCreate(t, ctx, client, "after-reset", cSpec{})
	live := recv(t, fresh)
	require.Equal(t, Added, live.Type, "the resubscribe rejoined the tailer that had already failed")
	assert.Equal(t, obj.ID, live.Object.ID)
}

// Tailers are lazy, one per kind, and end with the beehive — started or not.
func TestTailerStartsLazilyAndStopsWithBeehive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	assert.Zero(t, tailerCount(bh), "a tailer started before anything watched")

	// Concurrent first watches settle on one tailer for the kind.
	const racers = 8
	got := make([]*objectTailer, racers)
	var wg sync.WaitGroup
	for i := range racers {
		wg.Go(func() {
			tailer, err := bh.tailerFor(ctx, clientTestGK)
			assert.NoError(t, err)
			got[i] = tailer
		})
	}
	wg.Wait()
	// Every tailerFor owes a release; these are held to the end of the test.
	defer func() {
		for _, tailer := range got {
			tailer.release()
		}
	}()
	for _, tailer := range got {
		assert.Same(t, got[0], tailer)
	}
	assert.Equal(t, 1, tailerCount(bh))

	// It runs on a beehive that was never started.
	rx := got[0].hub.Receiver()
	defer rx.Close()
	obj := mustCreate(t, ctx, client, "unstarted", cSpec{})
	ev, err := rx.RecvContext(ctx)
	require.NoError(t, err)
	require.Equal(t, obj.ID, ev.Key)

	require.NoError(t, bh.stop(ctx))
	_, err = rx.RecvContext(ctx)
	assert.ErrorIs(t, err, gobus.ErrClosed, "stop left a subscriber hanging")

	// A watch opened after stop gets a stream that is already over.
	after, err := bh.tailerFor(ctx, clientTestGK)
	require.NoError(t, err)
	defer after.release()
	rxAfter := after.hub.Receiver()
	defer rxAfter.Close()
	_, err = rxAfter.RecvContext(ctx)
	assert.ErrorIs(t, err, gobus.ErrClosed)
}

// A closed wake hub ends the tailer on its own. Stop closes the hub and cancels
// the context together, so which arm of the select wins there is a coin toss —
// this one leaves the context live, which is the only way to pin that a tailer
// does not sit forever on a channel nobody will feed again.
func TestTailerEndsWhenTheKindWriteHubCloses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t), withWatchFloorInterval(time.Hour))
	tailer, err := newObjectTailer(ctx, bh, clientTestGK)
	require.NoError(t, err)

	done := make(chan struct{})
	// Nothing cancels the tailer here: outliving its own context is the point.
	go func() {
		defer close(done)
		tailer.run()
	}()

	bh.kindWriteHub.Close()
	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("the tailer outlived its wake hub")
	}
}

// startTailer runs one tailer with a receiver attached, and tears both down with
// the test.
func startTailer(t *testing.T, bh *Beehive, gk GroupKind) (*objectTailer, *conflate.Receiver[ObjectID, rawChange]) {
	t.Helper()
	tailer, err := newObjectTailer(context.Background(), bh, gk)
	require.NoError(t, err)
	rx := tailer.hub.Receiver()
	done := make(chan struct{})
	go func() {
		defer close(done)
		tailer.run()
	}()
	t.Cleanup(func() {
		rx.Close()
		tailer.close()
		<-done
	})
	return tailer, rx
}

// writeWorld is the beehive plus both write surfaces the wake table drives.
type writeWorld struct {
	bh     *Beehive
	client Client[cSpec, cStatus]
	ctrl   *controllerClientImpl[cStatus]
}

func newWriteWorld(t *testing.T) *writeWorld {
	t.Helper()
	bh := newTestBeehive(t, newClientTestStore(t))
	// Registered but never started: WithFinalizers refuses a kind no controller
	// in this process can clear, and nothing here needs a reconcile loop.
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	return &writeWorld{
		bh:     bh,
		client: NewClient[cSpec, cStatus](bh, clientTestGK),
		ctrl:   &controllerClientImpl[cStatus]{bh: bh, gk: clientTestGK},
	}
}

// The public list watch delivers on the commit's wake, with both the poll
// interval and the floor set far beyond the test timeout.
func TestWatchListDeliversWithoutPolling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t),
		withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	snap, ch, err := client.WatchList(ctx)
	require.NoError(t, err)
	require.Empty(t, snap.Objects)

	obj := mustCreate(t, ctx, client, "prompt", cSpec{Val: "a"})
	ev := recv(t, ch)
	assert.Equal(t, Added, ev.Type)
	assert.Equal(t, obj.ID, ev.Object.ID)
	assert.Equal(t, obj.ResourceVersion, ev.ResourceVersion)
}

// The delete of an object the snapshot carried is always delivered, even when
// the tailer published its create before that snapshot and this subscriber has
// read nothing. Merging the pair away would leave this caller holding a row
// that no longer exists, with no correction coming.
func TestWatchSeesDeleteOfSnapshotObject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t), withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	// A first watch starts the kind's tailer, so the create below is published
	// into the fan-out before the watch under test takes its snapshot.
	_, _, err := client.WatchList(ctx)
	require.NoError(t, err)
	obj := mustCreate(t, ctx, client, "doomed", cSpec{Val: "a"})

	snap, ch, err := client.WatchList(ctx)
	require.NoError(t, err)
	require.Len(t, snap.Objects, 1, "the snapshot holds the object")

	require.NoError(t, client.Delete(ctx, obj.ID))
	deleted, err := bh.gcCollect(ctx, obj.ID)
	require.NoError(t, err)
	require.True(t, deleted)

	for {
		ev := recv(t, ch)
		if ev.Type == Deleted {
			assert.Equal(t, obj.ID, ev.Object.ID)
			return
		}
	}
}

// NewClient is free-form, so two clients with different type parameters over one
// kind are legal. They share the tailer — which is why it fans out raw rows and
// leaves decode to each subscriber; a typed fan-out would panic on one of them.
func TestTwoClientsOverOneKindShareATailer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	store := &countingTailStore{Store: newClientTestStore(t)}
	bh := newTestBeehive(t, store, withWatchFloorInterval(time.Hour))
	typed := NewClient[cSpec, cStatus](bh, clientTestGK)
	raw := NewClient[json.RawMessage, json.RawMessage](bh, clientTestGK)

	_, typedCh, err := typed.WatchList(ctx)
	require.NoError(t, err)
	_, rawCh, err := raw.WatchList(ctx)
	require.NoError(t, err)
	positionReadsAtStart := store.positionReads.Load()

	obj := mustCreate(t, ctx, typed, "shared", cSpec{Val: "a"})

	ev := recv(t, typedCh)
	assert.Equal(t, "a", ev.Object.Spec.Val)
	rawEv := recv(t, rawCh)
	assert.Equal(t, obj.ID, rawEv.Object.ID)
	assert.JSONEq(t, `{"Val":"a"}`, string(rawEv.Object.Spec))

	assert.Equal(t, 1, tailerCount(bh), "the kind is read by one tailer")
	assert.Equal(t, int64(1), store.positionReads.Load()-positionReadsAtStart,
		"one write costs one position read, not one per subscriber")
}

// A single-object watch joins the kind's tailer through a key filter, so its
// memory is one key even though the tailer reads the whole kind.
func TestWatchSingleObjectSeesOnlyItsID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t),
		withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	mine := mustCreate(t, ctx, client, "mine", cSpec{Val: "a"})
	other := mustCreate(t, ctx, client, "other", cSpec{Val: "a"})

	snap, ch, err := client.Watch(ctx, mine.ID)
	require.NoError(t, err)
	require.NotNil(t, snap.Object)
	require.Equal(t, mine.ID, snap.Object.ID)
	require.Equal(t, 1, tailerCount(bh), "a single-object watch joins the kind's tailer")

	// The other object is written first; only the watched one may arrive.
	_, err = client.Update(ctx, other.ID, cSpec{Val: "b"})
	require.NoError(t, err)
	_, err = client.Update(ctx, mine.ID, cSpec{Val: "c"})
	require.NoError(t, err)

	ev := recv(t, ch)
	assert.Equal(t, mine.ID, ev.Object.ID)
	assert.Equal(t, "c", ev.Object.Spec.Val)
}

// A burst costs one relation query per drained batch, not one per object: the
// subscriber drains what is pending before loading. A per-object load would make
// every watch with relations an N+1.
func TestWatchLoadsAreBatchedPerDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	const burst = 64

	store := &countingLoadStore{Store: newClientTestStore(t)}
	bh := newTestBeehive(t, store, withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	_, ch, err := client.WatchList(ctx, WithLoads(LoadOwner()))
	require.NoError(t, err)
	store.relationReads.Store(0)

	require.NoError(t, bh.store.Within(ctx, func(ctx context.Context) error {
		for i := range burst {
			if _, err := client.Create(ctx, fmt.Sprintf("load-%d", i), cSpec{}); err != nil {
				return err
			}
		}
		return nil
	}))

	for range burst {
		ev := recv(t, ch)
		_, _, err := ev.Object.Owner()
		require.NoError(t, err, "the relation the watch asked for is loaded")
	}
	// How many drains the burst takes depends on when the subscriber wakes; what
	// must not happen is one query per object.
	assert.Less(t, store.relationReads.Load(), int64(burst))
}

// countingLoadStore counts the batched relation reads the eager loaders make.
type countingLoadStore struct {
	Store
	relationReads atomic.Int64
}

func (s *countingLoadStore) EdgesGroupOutgoingByID(ctx context.Context, ids []ObjectID, rel Relation) (map[ObjectID][]ObjectRef, error) {
	s.relationReads.Add(1)
	return s.Store.EdgesGroupOutgoingByID(ctx, ids, rel)
}

// A resume replays the log gap before going live, and pages it: with a day of
// retention the gap can far exceed one page.
func TestWatchResumeReplaysGapThenGoesLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t),
		withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	snap, _, err := client.WatchList(ctx)
	require.NoError(t, err)

	missed := mustCreate(t, ctx, client, "missed", cSpec{Val: "a"})

	_, ch, err := client.WatchList(ctx, WithResumeFrom(snap.ResourceVersion))
	require.NoError(t, err)

	ev := recv(t, ch)
	assert.Equal(t, Added, ev.Type, "the gap comes from the log")
	assert.Equal(t, missed.ID, ev.Object.ID)

	live := mustCreate(t, ctx, client, "live", cSpec{Val: "b"})
	ev = recv(t, ch)
	assert.Equal(t, live.ID, ev.Object.ID, "no duplicate across the seam")
}

// The gap is read in pages, so one larger than tailPageCap replays whole.
func TestWatchResumeReplaysBeyondOnePage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	const gap = tailPageCap + 40

	bh := newTestBeehive(t, newClientTestStore(t),
		withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	snap, _, err := client.WatchList(ctx)
	require.NoError(t, err)
	require.NoError(t, bh.store.Within(ctx, func(ctx context.Context) error {
		for i := range gap {
			if _, err := client.Create(ctx, fmt.Sprintf("gap-%d", i), cSpec{}); err != nil {
				return err
			}
		}
		return nil
	}))

	_, ch, err := client.WatchList(ctx, WithResumeFrom(snap.ResourceVersion))
	require.NoError(t, err)

	seen := make(map[ObjectID]bool, gap)
	for len(seen) < gap {
		seen[recv(t, ch).Object.ID] = true
	}
}

// The central cost claim: reads scale with watched kinds, not with watch count,
// and a quiet kind with watches on it reads nothing at all.
func TestTailerQueryCountConstantInSubscribers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	reads := func(t *testing.T, subscribers int) int64 {
		t.Helper()
		store := &countingTailStore{Store: newClientTestStore(t)}
		bh := newTestBeehive(t, store, withWatchFloorInterval(time.Hour))
		client := NewClient[cSpec, cStatus](bh, clientTestGK)

		chans := make([]<-chan ObjectChange[cSpec, cStatus], subscribers)
		for i := range subscribers {
			_, ch, err := client.WatchList(ctx)
			require.NoError(t, err)
			chans[i] = ch
		}
		at := store.positionReads.Load()

		mustCreate(t, ctx, client, "counted", cSpec{})
		for _, ch := range chans {
			recv(t, ch)
		}
		return store.positionReads.Load() - at
	}

	one, two := reads(t, 1), reads(t, 2)
	assert.Equal(t, one, two, "a second subscriber costs no extra read")

	// And a kind nobody writes to costs nothing: the floor is an hour away and
	// no wake fires.
	store := &countingTailStore{Store: newClientTestStore(t)}
	bh := newTestBeehive(t, store, withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	for range 3 {
		_, _, err := client.WatchList(ctx)
		require.NoError(t, err)
	}
	at := store.positionReads.Load()
	mustCreate(t, ctx, NewClient[cSpec, cStatus](bh, GroupKind{Kind: "Other"}), "elsewhere", cSpec{})
	assert.Equal(t, at, store.positionReads.Load(), "a quiet kind reads nothing")
}

// One page collapses to the last entry per object, carrying current state, in
// write order rather than the id order the batched read returns.
func TestOnePageCoalescesToCurrentStateInWriteOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t), withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	first := mustCreate(t, ctx, client, "first", cSpec{Val: "first"})
	second := mustCreate(t, ctx, client, "second", cSpec{Val: "second"})

	// The lower id is written last, so id order and write order disagree.
	_, err := client.Update(ctx, second.ID, cSpec{Val: "second2"})
	require.NoError(t, err)
	_, err = client.Update(ctx, first.ID, cSpec{Val: "first2"})
	require.NoError(t, err)
	_, err = client.Update(ctx, first.ID, cSpec{Val: "first3"})
	require.NoError(t, err)

	page, _, err := bh.store.ObjectWritesListSince(ctx, clientTestGK, 0, tailPageCap)
	require.NoError(t, err)
	changes, err := collectChanges(ctx, bh, clientTestGK, page, false)
	require.NoError(t, err)

	require.Len(t, changes, 2, "five writes to two objects collapse to two changes")
	assert.Equal(t, second.ID, changes[0].ID, "write order, not id order")
	assert.Equal(t, first.ID, changes[1].ID)
	assert.Contains(t, string(changes[1].Object.Spec), "first3", "current state, not a superseded one")
}

// Ownership is resolved from current state, not from the log entry: the create
// entry is appended before the owner edge is written, in the same transaction,
// so an entry that carries an owner would carry none for the write that matters
// most.
func TestOnePageResolvesCurrentOwners(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t), withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	owner := mustCreate(t, ctx, client, "owner", cSpec{})
	child := mustCreate(t, ctx, client, "child", cSpec{}, WithOwner(owner.ID))

	page, _, err := bh.store.ObjectWritesListSince(ctx, clientTestGK, 0, tailPageCap)
	require.NoError(t, err)
	changes, err := collectChanges(ctx, bh, clientTestGK, page, true)
	require.NoError(t, err)

	require.Len(t, changes, 2)
	byID := map[ObjectID]rawChange{changes[0].ID: changes[0], changes[1].ID: changes[1]}
	require.NotNil(t, byID[child.ID].Owner)
	assert.Equal(t, owner.ID, byID[child.ID].Owner.ID)
	assert.Nil(t, byID[owner.ID].Owner, "an unowned object reports no owner")
}

// The lookup is a whole extra query per page, so a kind nobody watches by owner
// must not pay for it.
func TestOnePageSkipsTheOwnerLookupWhenUnscoped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	store := &countingLoadStore{Store: newClientTestStore(t)}
	bh := newTestBeehive(t, store, withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	owner := mustCreate(t, ctx, client, "owner", cSpec{})
	mustCreate(t, ctx, client, "child", cSpec{}, WithOwner(owner.ID))

	page, _, err := bh.store.ObjectWritesListSince(ctx, clientTestGK, 0, tailPageCap)
	require.NoError(t, err)
	before := store.relationReads.Load()

	changes, err := collectChanges(ctx, bh, clientTestGK, page, false)
	require.NoError(t, err)
	assert.Equal(t, before, store.relationReads.Load(), "no owner lookup without a scoped watch")
	for _, ch := range changes {
		assert.Nil(t, ch.Owner)
	}

	_, err = collectChanges(ctx, bh, clientTestGK, page, true)
	require.NoError(t, err)
	assert.Equal(t, before+1, store.relationReads.Load(), "one lookup for the whole page")
}

// Stop drains every watch: each stream closes, and no tailer goroutine outlives
// the beehive that started it.
func TestWatchGoroutinesDrainOnStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t), withWatchFloorInterval(time.Hour))

	var streams []<-chan ObjectChange[cSpec, cStatus]
	for _, kind := range []string{"A", "B", "C"} {
		client := NewClient[cSpec, cStatus](bh, GroupKind{Kind: kind})
		_, list, err := client.WatchList(ctx)
		require.NoError(t, err)
		obj := mustCreate(t, ctx, client, "one", cSpec{})
		_, single, err := client.Watch(ctx, obj.ID)
		require.NoError(t, err)
		streams = append(streams, list, single)
	}
	require.Equal(t, 3, tailerCount(bh))

	require.NoError(t, bh.stop(ctx))
	for i, ch := range streams {
		for range ch { // drains whatever was in flight, then ends
		}
		assert.NotPanics(t, func() { <-ch }, "stream %d is closed", i)
	}
}

// failingLoadStore fails every batched relation read, and reports when it first
// did so a test can act on the retry rather than wait for it.
type failingLoadStore struct {
	Store
	once  sync.Once
	tried chan struct{}
}

func (s *failingLoadStore) EdgesGroupOutgoingByID(context.Context, []ObjectID, Relation) (map[ObjectID][]ObjectRef, error) {
	s.once.Do(func() { close(s.tried) })
	return nil, errBoom
}

// A failed relation load is retried, not skipped: the entries it covers are
// already behind the cursor, so no later read brings them back, and a skip is
// a change the subscriber never hears about. The retry ends with the caller's
// context — on the live path and on a resume's replay, which decode their
// batches the same way.
func TestWatchLoadFailureRetriesUntilTheCallerGivesUp(t *testing.T) {
	for _, tc := range []struct {
		name   string
		resume bool
	}{
		{name: "live"},
		{name: "resume", resume: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			store := &failingLoadStore{Store: newClientTestStore(t), tried: make(chan struct{})}
			bh := newTestBeehive(t, store, withWatchFloorInterval(fastTick))
			client := NewClient[cSpec, cStatus](bh, clientTestGK)

			var ch <-chan ObjectChange[cSpec, cStatus]
			var err error
			if tc.resume {
				at, mErr := store.ObjectWritesMaxVersion(ctx, clientTestGK)
				require.NoError(t, mErr)
				mustCreate(t, ctx, client, "gapped", cSpec{})
				_, ch, err = client.WatchList(ctx, WithResumeFrom(at), WithLoads(LoadOwner()))
				require.NoError(t, err)
			} else {
				_, ch, err = client.WatchList(ctx, WithLoads(LoadOwner()))
				require.NoError(t, err)
				mustCreate(t, ctx, client, "live", cSpec{})
			}

			<-store.tried // retry in progress, and nothing delivered without its relations
			cancel()
			for range ch { // the stream ends rather than parking on a send
			}
		})
	}
}

// resumeListStore intercepts the replay's paged read of the gap above from. The
// kind's tailer starts above the gap, so its own reads never collide with it.
type resumeListStore struct {
	Store
	from int64
	// fail fails the next replay read; failAll fails every one of them.
	fail    atomic.Bool
	failAll atomic.Bool
	trim    atomic.Bool
	// tried and served report the first failed and the first successful replay
	// read, so a test acts on the read rather than waiting for it.
	tried, served         chan struct{}
	triedOnce, servedOnce sync.Once
}

func newResumeListStore(t *testing.T) *resumeListStore {
	t.Helper()
	return &resumeListStore{
		Store:  newClientTestStore(t),
		tried:  make(chan struct{}),
		served: make(chan struct{}),
	}
}

func (s *resumeListStore) ObjectWritesListSince(ctx context.Context, gk GroupKind, afterRV int64, limit int) ([]ObjectWrite, int64, error) {
	replay := afterRV == s.from && limit == tailPageCap
	if replay && (s.failAll.Load() || s.fail.CompareAndSwap(true, false)) {
		s.triedOnce.Do(func() { close(s.tried) })
		return nil, 0, errBoom
	}
	page, trimmedThrough, err := s.Store.ObjectWritesListSince(ctx, gk, afterRV, limit)
	if replay {
		s.servedOnce.Do(func() { close(s.served) })
		if s.trim.Load() {
			trimmedThrough = afterRV + 1 // retention overtook the replay mid-page
		}
	}
	return page, trimmedThrough, err
}

// A transient read costs the replay a retry, not the stream: the cursor has not
// moved, so the gap is still there to read.
func TestWatchResumeRetriesAFailedGapRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	store := newResumeListStore(t)
	bh := newTestBeehive(t, store, withWatchFloorInterval(fastTick))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	at, err := store.ObjectWritesMaxVersion(ctx, clientTestGK)
	require.NoError(t, err)
	store.from = at
	missed := mustCreate(t, ctx, client, "missed", cSpec{})
	store.fail.Store(true)

	_, ch, err := client.WatchList(ctx, WithResumeFrom(at))
	require.NoError(t, err)

	ev := recv(t, ch)
	assert.Equal(t, missed.ID, ev.Object.ID, "the gap was dropped instead of retried")
	assert.False(t, store.fail.Load(), "the test never exercised the failure it set up")
}

// Retention can overtake a replay that is still paging. The subscriber is told,
// because a stream that silently skipped the trimmed entries would look like one
// that had caught up.
func TestWatchResumeEndsWhenRetentionOvertakesIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	store := newResumeListStore(t)
	bh := newTestBeehive(t, store, withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	at, err := store.ObjectWritesMaxVersion(ctx, clientTestGK)
	require.NoError(t, err)
	store.from = at
	mustCreate(t, ctx, client, "trimmed", cSpec{})
	store.trim.Store(true)

	_, ch, err := client.WatchList(ctx, WithResumeFrom(at))
	require.NoError(t, err, "a resume reads nothing on the caller's goroutine")

	ev := recv(t, ch)
	assert.Equal(t, Failed, ev.Type)
	assert.ErrorIs(t, ev.Err, ErrWatchTooOld)
	_, ok := <-ch
	assert.False(t, ok, "a Failed change is the last value before the channel closes")
}

// failListByIDsStore fails the next batched state read, or every one of them,
// and reports the first failure.
type failListByIDsStore struct {
	Store
	fail      atomic.Bool
	failAll   atomic.Bool
	tried     chan struct{}
	triedOnce sync.Once
}

func newFailListByIDsStore(t *testing.T) *failListByIDsStore {
	t.Helper()
	return &failListByIDsStore{Store: newClientTestStore(t), tried: make(chan struct{})}
}

func (s *failListByIDsStore) ObjectsListByIDs(ctx context.Context, gk GroupKind, ids []ObjectID) ([]*RawObject, error) {
	if s.failAll.Load() || s.fail.CompareAndSwap(true, false) {
		s.triedOnce.Do(func() { close(s.tried) })
		return nil, errBoom
	}
	return s.Store.ObjectsListByIDs(ctx, gk, ids)
}

// The state read behind a replayed page is retried with the same backoff as
// the page itself, and for the same reason.
func TestWatchResumeRetriesAFailedStateRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	store := newFailListByIDsStore(t)
	bh := newTestBeehive(t, store, withWatchFloorInterval(fastTick))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	at, err := store.ObjectWritesMaxVersion(ctx, clientTestGK)
	require.NoError(t, err)
	missed := mustCreate(t, ctx, client, "missed", cSpec{})
	// Set last: the tailer this watch starts reads no state of its own, so the
	// replay's read is the first.
	store.fail.Store(true)

	_, ch, err := client.WatchList(ctx, WithResumeFrom(at))
	require.NoError(t, err)

	ev := recv(t, ch)
	assert.Equal(t, missed.ID, ev.Object.ID)
	assert.False(t, store.fail.Load(), "the test never exercised the failure it set up")
}

// A resume with nothing above it replays an empty gap and goes live at once.
func TestWatchResumeWithNoGapGoesLiveAtOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	store := newResumeListStore(t)
	bh := newTestBeehive(t, store, withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	at, err := store.ObjectWritesMaxVersion(ctx, clientTestGK)
	require.NoError(t, err)
	store.from = at

	_, ch, err := client.WatchList(ctx, WithResumeFrom(at))
	require.NoError(t, err)

	// The write waits for the gap read, which would otherwise be the one that
	// returns it — an empty gap is the case under test.
	<-store.served
	live := mustCreate(t, ctx, client, "live", cSpec{})
	ev := recv(t, ch)
	assert.Equal(t, live.ID, ev.Object.ID)
}

// A single-object resume filters the page before the state read, not after: the
// replay of a busy gap costs one object's read rather than the whole page's.
func TestWatchSingleObjectResumeReplaysOnlyItsID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	store := &countingLoadByIDsStore{Store: newClientTestStore(t)}
	bh := newTestBeehive(t, store, withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	mine := mustCreate(t, ctx, client, "mine", cSpec{Val: "a"})
	at, err := store.ObjectWritesMaxVersion(ctx, clientTestGK)
	require.NoError(t, err)

	_, err = client.Update(ctx, mine.ID, cSpec{Val: "b"})
	require.NoError(t, err)
	for i := range 3 {
		mustCreate(t, ctx, client, fmt.Sprintf("other-%d", i), cSpec{})
	}

	_, ch, err := client.Watch(ctx, mine.ID, WithResumeFrom(at))
	require.NoError(t, err)

	ev := recv(t, ch)
	assert.Equal(t, mine.ID, ev.Object.ID)
	assert.Equal(t, "b", ev.Object.Spec.Val, "current state, not the version at the resume position")
	assert.Equal(t, []int{1}, store.batchSizes(), "the replay read back the whole page")

	// And the ids it filtered out stay out, rather than arriving late.
	_, err = client.Update(ctx, mine.ID, cSpec{Val: "c"})
	require.NoError(t, err)
	ev = recv(t, ch)
	assert.Equal(t, mine.ID, ev.Object.ID)
}

// countingLoadByIDsStore records the size of every batched state read.
type countingLoadByIDsStore struct {
	Store
	mu    sync.Mutex
	sizes []int
}

func (s *countingLoadByIDsStore) ObjectsListByIDs(ctx context.Context, gk GroupKind, ids []ObjectID) ([]*RawObject, error) {
	s.mu.Lock()
	s.sizes = append(s.sizes, len(ids))
	s.mu.Unlock()
	return s.Store.ObjectsListByIDs(ctx, gk, ids)
}

func (s *countingLoadByIDsStore) batchSizes() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.sizes)
}

// A replay delivers on the subscriber's own goroutine, so a caller that gives up
// mid-gap ends the stream there rather than leaving it parked on a send nobody
// will take.
func TestWatchResumeStopsDeliveringWhenTheCallerGivesUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t), withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	at, err := bh.store.ObjectWritesMaxVersion(ctx, clientTestGK)
	require.NoError(t, err)
	for i := range 2 {
		mustCreate(t, ctx, client, fmt.Sprintf("gap-%d", i), cSpec{})
	}

	_, ch, err := client.WatchList(ctx, WithResumeFrom(at))
	require.NoError(t, err)

	recv(t, ch) // the first of the gap; the second is now blocked on the send
	cancel()
	for range ch { // the stream ends rather than parking on a send
	}
}

// A replay that cannot read gives up with the caller rather than on its own:
// each failure waits one backoff step, and the retrying ends when the context
// does. Both reads behind a replayed page follow this.
func TestWatchResumeGivesUpWithTheCaller(t *testing.T) {
	for _, tc := range []struct {
		name string
		// failAll arms the store to fail one of the replay's two reads forever,
		// and returns the channel that reports the first failure.
		failAll  func(*resumeListStore, *failListByIDsStore) chan struct{}
		newStore func(*testing.T) (Store, *resumeListStore, *failListByIDsStore)
	}{
		{
			name: "gap read",
			newStore: func(t *testing.T) (Store, *resumeListStore, *failListByIDsStore) {
				s := newResumeListStore(t)
				return s, s, nil
			},
			failAll: func(gap *resumeListStore, _ *failListByIDsStore) chan struct{} {
				gap.failAll.Store(true)
				return gap.tried
			},
		},
		{
			name: "state read",
			newStore: func(t *testing.T) (Store, *resumeListStore, *failListByIDsStore) {
				s := newFailListByIDsStore(t)
				return s, nil, s
			},
			failAll: func(_ *resumeListStore, state *failListByIDsStore) chan struct{} {
				state.failAll.Store(true)
				return state.tried
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			store, gap, state := tc.newStore(t)
			bh := newTestBeehive(t, store, withWatchFloorInterval(fastTick))
			client := NewClient[cSpec, cStatus](bh, clientTestGK)

			at, err := store.ObjectWritesMaxVersion(ctx, clientTestGK)
			require.NoError(t, err)
			if gap != nil {
				gap.from = at
			}
			mustCreate(t, ctx, client, "gapped", cSpec{})

			tried := tc.failAll(gap, state)
			_, ch, err := client.WatchList(ctx, WithResumeFrom(at))
			require.NoError(t, err, "a resume reads nothing on the caller's goroutine")

			<-tried // retry in progress, with the gap still unread
			cancel()
			for range ch { // the stream ends rather than parking on a send
			}
		})
	}
}

// A caller checkpoints the resource version on a delivered change and resumes
// above it, so the drain must never hand back a lower version after a higher
// one — the lower one would be skipped for good. The fan-out coalesces in
// place, so A's re-write keeps A's original queue position while carrying the
// newer version, which is the one way the drain sees them out of order.
func TestDrainPendingIsAscendingByResourceVersion(t *testing.T) {
	hub := conflate.New[ObjectID](mergeRawChange)
	defer hub.Close()
	rx := hub.Receiver()
	defer rx.Close()
	tx := hub.Sender()

	a, b := ObjectID(1), ObjectID(2)
	require.NoError(t, tx.Send(a, rawChange{ID: a, Op: WriteUpdate, ResourceVersion: 10}))
	require.NoError(t, tx.Send(b, rawChange{ID: b, Op: WriteUpdate, ResourceVersion: 11}))
	require.NoError(t, tx.Send(a, rawChange{ID: a, Op: WriteUpdate, ResourceVersion: 12}))

	first, err := rx.Recv()
	require.NoError(t, err)

	var got []int64
	for _, ch := range drainPending(first.Value, rx) {
		got = append(got, ch.ResourceVersion)
	}
	// A@10 coalesced into A@12, so two changes remain: B's older one first.
	assert.Equal(t, []int64{11, 12}, got)
}

// A producer that never stops writing must not hold a subscriber inside
// drainPending: the loop exits on an empty receiver, and the tailer can only
// refill that receiver after a store read. Nothing else here exercises a
// producer that replenishes *during* a drain. Starvation shows up as the
// failsafe timeout rather than as a wrong value.
func TestWatchDeliversUnderSustainedWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	const objects = 8
	ids := make([]ObjectID, objects)
	for i := range ids {
		ids[i] = mustCreate(t, ctx, client, fmt.Sprintf("w%d", i), cSpec{}).ID
	}

	_, stream, err := client.WatchList(ctx)
	require.NoError(t, err)

	writes, stopWrites := context.WithCancel(ctx)
	var writer sync.WaitGroup
	// LIFO: stopWrites runs first, so the wait below is not the thing that ends
	// the writer.
	defer writer.Wait()
	defer stopWrites()
	writer.Add(1)
	go func() {
		defer writer.Done()
		// Errors are the context ending mid-write, which is how the test stops it.
		for i := 0; writes.Err() == nil; i++ {
			_, _ = client.Update(writes, ids[i%objects], cSpec{Val: fmt.Sprint(i)})
		}
	}()

	const want = 50
	for got := 0; got < want; {
		select {
		case ch, ok := <-stream:
			require.True(t, ok, "the stream ended after %d changes", got)
			require.NotEqual(t, Failed, ch.Type, "%v", ch.Err)
			got++
		case <-ctx.Done():
			t.Fatalf("delivered %d of %d changes before the failsafe", got, want)
		}
	}
}

// failingPositionStore fails the tail's position read once armed, and counts the
// attempts, so a test can tell a backoff from a spin. Armed after the tailer is
// built, since newObjectTailer reads the position too.
type failingPositionStore struct {
	Store
	armed    atomic.Bool
	attempts atomic.Int64
	tried    chan struct{}
}

func (s *failingPositionStore) ObjectWritesMaxVersion(ctx context.Context, gk GroupKind) (int64, error) {
	if !s.armed.Load() {
		return s.Store.ObjectWritesMaxVersion(ctx, gk)
	}
	s.attempts.Add(1)
	select {
	case s.tried <- struct{}{}:
	default:
	}
	return 0, errBoom
}

// A commit wake must not void the retry backoff. A wake carries no information
// the drain needs, and a commit landing during a failed drain refills the wake
// slot — so a tailer that honoured one would re-read a degraded store as fast
// as it could fail, for as long as anything kept writing.
//
// The wakes are sent straight to the hub rather than written: this is about the
// tailer's loop, and a real write would need the store the test is failing.
func TestTailBackoffSurvivesCommitWakes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &failingPositionStore{Store: newClientTestStore(t), tried: make(chan struct{}, 1)}
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	mustCreate(t, ctx, client, "w1", cSpec{})

	_, ch, err := client.WatchList(ctx)
	require.NoError(t, err)
	go func() {
		for range ch { // the stream stays open across a failed read
		}
	}()

	store.armed.Store(true)
	require.NoError(t, bh.kindWriteHub.Send(clientTestGK))

	// The first attempt puts the tailer in backoff; everything after is the
	// window under test.
	select {
	case <-store.tried:
	case <-time.After(testTimeout):
		t.Fatal("the tailer never read the log position")
	}
	settled := store.attempts.Load()

	// Far more wakes than watchRetryBase (100ms) could ever admit attempts for:
	// sending them costs microseconds, so a further attempt means the backoff
	// was bypassed rather than that it elapsed.
	for range 500 {
		require.NoError(t, bh.kindWriteHub.Send(clientTestGK))
	}
	assert.LessOrEqual(t, store.attempts.Load()-settled, int64(1),
		"a commit wake bypassed the retry backoff")
}

// Send and Close no-op on the zero wake hub, which a Beehive built field by
// field has. Watch cannot: a receiver has to be tied to a hub. It reports that
// rather than dereferencing nil, so the watch fails where the tailer is built.
func TestWatchOnABeehiveNotBuiltByNewFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bh := &Beehive{store: newClientTestStore(t)}
	// The siblings still tolerate it, which is what makes Watch the odd one.
	require.NoError(t, bh.kindWriteHub.Send(clientTestGK))
	require.NotPanics(t, bh.kindWriteHub.Close)

	_, _, err := NewClient[cSpec, cStatus](bh, clientTestGK).WatchList(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "built by New")
}

// A stream that ends because the beehive stopped must say so. The contract is
// that a silent close means the caller's own context ended, so a supervisor
// written to it — no Failed change and my ctx is live, therefore resubscribe —
// would otherwise loop hot after Stop, re-reading a full snapshot every pass.
func TestWatchEndsWithErrStoppedOnStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t), withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	_, live, err := client.WatchList(ctx)
	require.NoError(t, err)
	require.NoError(t, bh.stop(ctx))

	var last ObjectChange[cSpec, cStatus]
	for ch := range live {
		last = ch
	}
	assert.Equal(t, Failed, last.Type, "the stream closed silently on stop")
	assert.ErrorIs(t, last.Err, ErrStopped)

	// A watch opened after stop ends the same way rather than closing silently,
	// which is what stops a resubscribe loop from spinning.
	_, after, err := client.WatchList(ctx)
	require.NoError(t, err)
	last = ObjectChange[cSpec, cStatus]{}
	for ch := range after {
		last = ch
	}
	assert.Equal(t, Failed, last.Type, "a watch opened after stop closed silently")
	assert.ErrorIs(t, last.Err, ErrStopped)
}

// blockingPositionStore parks one kind's position read until released, so a
// test can hold a tailer build open while it exercises other kinds.
type blockingPositionStore struct {
	Store
	gk      GroupKind
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *blockingPositionStore) ObjectWritesMaxVersion(ctx context.Context, gk GroupKind) (int64, error) {
	if gk == s.gk {
		s.once.Do(func() { close(s.entered) })
		<-s.release
	}
	return s.Store.ObjectWritesMaxVersion(ctx, gk)
}

// Building a tailer must not hold tailMu, which is process-global, across the
// cursor read, which parks on the store's single connection. Holding one across
// the other lets one slow transaction stall every kind's watch setup — and every
// release, which is what closes a cancelled watch's channel. A transaction whose
// own goroutine waits on such a channel then deadlocks outright.
func TestTailerBuildDoesNotStallOtherKinds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	slowKind := GroupKind{Kind: "Slow"}
	store := &blockingPositionStore{
		Store:   newClientTestStore(t),
		gk:      slowKind,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	bh := newTestBeehive(t, store)

	held, err := bh.tailerFor(ctx, clientTestGK)
	require.NoError(t, err)

	built := make(chan error, 1)
	go func() {
		slow, err := bh.tailerFor(ctx, slowKind)
		if err == nil {
			slow.release()
		}
		built <- err
	}()
	<-store.entered // the build is parked inside the store read

	done := make(chan struct{})
	go func() {
		defer close(done)
		again, err := bh.tailerFor(ctx, clientTestGK)
		assert.NoError(t, err, "an unrelated kind could not join its tailer")
		again.release()
		held.release() // the teardown a cancelled watch runs before closing out
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("a tailer build held tailMu across the store read")
	}

	close(store.release)
	require.NoError(t, <-built)
}

// stop closes the wake hub's sender while a client write's AfterCommit hook may
// be sending on it, and nothing fences application writes against Stop.
// watch.Sender.Close allows exactly that (gobus v0.5.1): a racing send either
// publishes or answers ErrClosed, never both and never partially. Which one
// wins is unspecified, and nothing here needs it pinned — every stream is
// ending anyway.
//
// The guarantee is watch.Sender.Close's alone. watch.Hub.Close keeps the
// close-versus-send discipline, so this stays a test of the sender-close path:
// see kindWriteHub.Close, which is deliberately not a hub close.
func TestStopToleratesConcurrentCommitWakes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t), withWatchFloorInterval(time.Hour))

	var senders sync.WaitGroup
	bad := make(chan error, 16)
	for range 8 {
		senders.Go(func() {
			for range 500 {
				if err := bh.kindWriteHub.Send(clientTestGK); err != nil {
					if !errors.Is(err, gobus.ErrClosed) {
						bad <- err
					}
					return // ErrClosed is terminal; the hub does not reopen
				}
			}
		})
	}

	require.NoError(t, bh.stop(ctx))
	senders.Wait()
	close(bad)
	for err := range bad {
		t.Errorf("a wake racing stop answered %v, want nil or ErrClosed", err)
	}
}

// The promotion rule has one statement and two callers, so pin the rule itself
// rather than only its two uses. A run that began with a create reports as a
// create, unless it ends in a delete — the object was never in the subscriber's
// snapshot, so Modified would name an id the cache does not hold.
func TestCoalesceOp(t *testing.T) {
	cases := []struct {
		began, ended, want WriteOp
	}{
		{WriteCreate, WriteUpdate, WriteCreate},
		{WriteCreate, WriteCreate, WriteCreate},
		{WriteCreate, WriteDelete, WriteDelete},
		{WriteUpdate, WriteUpdate, WriteUpdate},
		{WriteUpdate, WriteDelete, WriteDelete},
		{WriteDelete, WriteUpdate, WriteUpdate},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, coalesceOp(tc.began, tc.ended), "%v then %v", tc.began, tc.ended)
	}
}

// failAfterArmStore fails one subscriber-side read once armed, and reports the
// first failure so a test can act on it. Scoped to one method so the tailer's
// own reads keep working — the point is a subscriber stuck retrying, not a
// tailer stuck retrying.
type failAfterArmStore struct {
	Store
	failEdges  bool // the relation load a WithLoads batch runs
	failWrites bool // the log read a resume replay pages through
	armed      atomic.Bool
	once       sync.Once
	tried      chan struct{}
}

func (s *failAfterArmStore) hit() bool {
	if !s.armed.Load() {
		return false
	}
	s.once.Do(func() { close(s.tried) })
	return true
}

func (s *failAfterArmStore) EdgesGroupOutgoingByID(ctx context.Context, ids []ObjectID, rel Relation) (map[ObjectID][]ObjectRef, error) {
	if s.failEdges && s.hit() {
		return nil, errBoom
	}
	return s.Store.EdgesGroupOutgoingByID(ctx, ids, rel)
}

func (s *failAfterArmStore) ObjectWritesListSince(ctx context.Context, gk GroupKind, afterRV int64, limit int) ([]ObjectWrite, int64, error) {
	if s.failWrites && s.hit() {
		return nil, 0, errBoom
	}
	return s.Store.ObjectWritesListSince(ctx, gk, afterRV, limit)
}

// A subscriber retrying a failed read must observe the tailer ending, not only
// its caller's context. Both retry loops here — the relation load and the resume
// replay — retry until their context ends, so on the caller's context alone a
// store that keeps failing past Stop holds the goroutine and its tailer lease
// forever, and the stream never reports ErrStopped, never closes, and never
// releases.
func TestWatchRetryEndsWhenTheBeehiveStops(t *testing.T) {
	cases := []struct {
		name  string
		store func(inner Store) *failAfterArmStore
		opts  []WatchOption
		// drive produces the work the subscriber then fails to finish.
		drive func(t *testing.T, ctx context.Context, c Client[cSpec, cStatus])
	}{
		{
			name: "relation load",
			store: func(inner Store) *failAfterArmStore {
				return &failAfterArmStore{Store: inner, failEdges: true, tried: make(chan struct{})}
			},
			opts: []WatchOption{WithLoads(LoadOwner())},
			drive: func(t *testing.T, ctx context.Context, c Client[cSpec, cStatus]) {
				mustCreate(t, ctx, c, "after", cSpec{})
			},
		},
		{
			name: "resume replay",
			store: func(inner Store) *failAfterArmStore {
				return &failAfterArmStore{Store: inner, failWrites: true, tried: make(chan struct{})}
			},
			opts:  []WatchOption{WithResumeFrom(1)},
			drive: func(*testing.T, context.Context, Client[cSpec, cStatus]) {},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()

			store := tc.store(newClientTestStore(t))
			bh := newTestBeehive(t, store, withWatchFloorInterval(time.Hour))
			client := NewClient[cSpec, cStatus](bh, clientTestGK)
			mustCreate(t, ctx, client, "w1", cSpec{})

			_, ch, err := client.WatchList(ctx, tc.opts...)
			require.NoError(t, err)

			store.armed.Store(true)
			tc.drive(t, ctx, client)
			select { // the subscriber is inside a retry loop
			case <-store.tried:
			case <-ctx.Done():
				t.Fatal("the watch never reached a failing read")
			}

			// The caller's context stays live throughout: this is about the
			// tailer ending, not about cancellation.
			require.NoError(t, bh.stop(ctx))

			var last ObjectChange[cSpec, cStatus]
			for got := range ch {
				last = got
			}
			assert.Equal(t, Failed, last.Type, "the stream never reported why it ended")
			assert.ErrorIs(t, last.Err, ErrStopped)
		})
	}
}

// A create or update that will not decode is dropped and the stream carries on.
// Unlike a delete it needs no tombstone: the row is still there, so the next
// write to it repairs whatever a mirror holds, and reporting a change with no
// state would say less than saying nothing.
//
// The watch starts before the poison row, which is what puts the row on the tail
// rather than in the snapshot — decodeList quarantines the snapshot's copy, and
// this is decodeChanges' arm of the same rule.
func TestWatchQuarantinesAnUndecodableWriteOnTheTail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, _ := watchFixture(t)
	_, ch, err := client.WatchList(ctx)
	require.NoError(t, err)

	_, err = store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`not json`),
	})
	require.NoError(t, err)

	// A decodable object is the barrier: it cannot arrive before the batch that
	// dropped the poison row, so receiving it first proves nothing was sent for
	// that row.
	good := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "good"})

	ev := recv(t, ch)
	assert.Equal(t, Added, ev.Type)
	assert.Equal(t, good.ID, ev.Object.ID, "the undecodable write reached the stream")
}

// blockFirstPositionStore parks the first position read for one kind until
// released, so a test can hold one tailer build open while another finishes.
type blockFirstPositionStore struct {
	Store
	gk      GroupKind
	blocks  atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (s *blockFirstPositionStore) ObjectWritesMaxVersion(ctx context.Context, gk GroupKind) (int64, error) {
	if gk == s.gk && s.blocks.CompareAndSwap(true, false) {
		close(s.entered)
		<-s.release
	}
	return s.Store.ObjectWritesMaxVersion(ctx, gk)
}

// The build runs outside tailMu, so two first watches on one kind can both
// reach it. The one that registers first wins; the other discards the tailer it
// built and joins, leaving the kind with exactly one.
//
// Sequenced rather than raced: the concurrent-racers test reaches this path only
// when the scheduler cooperates, and under GOMAXPROCS=1 it never does.
func TestConcurrentTailerBuildsSettleOnOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	store := &blockFirstPositionStore{
		Store:   newClientTestStore(t),
		gk:      clientTestGK,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	store.blocks.Store(true)
	bh := newTestBeehive(t, store)

	loser := make(chan *objectTailer, 1)
	go func() {
		got, err := bh.tailerFor(ctx, clientTestGK)
		assert.NoError(t, err)
		loser <- got
	}()
	<-store.entered // parked in its cursor read, having already missed the registry

	// This build's read does not park, so it registers while the other waits.
	winner, err := bh.tailerFor(ctx, clientTestGK)
	require.NoError(t, err)
	close(store.release)

	joined := <-loser
	assert.Same(t, winner, joined, "the losing build did not join the winner")
	assert.Equal(t, 1, tailerCount(bh))

	// Both calls owed a lease, the discarded build owed none.
	winner.release()
	joined.release()
	assert.Zero(t, tailerCount(bh))
}

// A resume position above the log's head did not come from this store — a
// restored backup or a swapped file restarts the sequence. It has to be
// reported: the replay reads an empty page, calls itself caught up, and then
// floor sits above every change the stream would deliver, so the subscriber
// receives nothing at all and is told nothing.
func TestResumeAboveTheLogHeadFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t), withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	mustCreate(t, ctx, client, "w1", cSpec{})

	_, ch, err := client.WatchList(ctx, WithResumeFrom(1<<40))
	require.NoError(t, err, "the position arrives on the stream, not from the call")

	var last ObjectChange[cSpec, cStatus]
	for got := range ch {
		last = got
	}
	require.Equal(t, Failed, last.Type)
	assert.ErrorIs(t, last.Err, ErrWatchTooNew)

	// A position exactly at the head is caught up, not ahead: it is what a
	// subscriber that read every entry checkpoints.
	at, err := bh.store.ObjectWritesMaxVersion(ctx, clientTestGK)
	require.NoError(t, err)
	snap, live, err := client.WatchList(ctx, WithResumeFrom(at))
	require.NoError(t, err)
	assert.Equal(t, at, snap.ResourceVersion)
	obj := mustCreate(t, ctx, client, "w2", cSpec{})
	ev := recv(t, live)
	assert.Equal(t, obj.ID, ev.ID, "a resume at the head must still stream")
}

// failHeadCheckStore fails the replay's head check exactly once, and nothing
// else. An empty page is what puts the replay there, so arming on one targets
// that read without counting calls no test should have to know the order of.
type failHeadCheckStore struct {
	Store
	always bool // keep failing it, rather than only the first
	armed  atomic.Bool
	spent  atomic.Bool
	once   sync.Once
	failed chan struct{} // closed once the head check has been failed
}

func (s *failHeadCheckStore) ObjectWritesListSince(ctx context.Context, gk GroupKind, afterRV int64, limit int) ([]ObjectWrite, int64, error) {
	page, trimmedThrough, err := s.Store.ObjectWritesListSince(ctx, gk, afterRV, limit)
	if err == nil && len(page) == 0 && (s.always || !s.spent.Load()) {
		s.armed.Store(true) // the head check is the next position read
	}
	return page, trimmedThrough, err
}

func (s *failHeadCheckStore) ObjectWritesMaxVersion(ctx context.Context, gk GroupKind) (int64, error) {
	if s.armed.CompareAndSwap(true, false) {
		s.spent.Store(true)
		s.once.Do(func() { close(s.failed) })
		return 0, errBoom
	}
	return s.Store.ObjectWritesMaxVersion(ctx, gk)
}

// The read that tells "caught up" from "resumed past the head" can fail, and a
// failed read is a retry rather than the stream: it decides nothing on its own,
// so guessing either way would either end a good stream or keep the silent-drop
// one this check exists to catch.
func TestResumeRetriesAFailedHeadCheck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	store := &failHeadCheckStore{Store: newClientTestStore(t), failed: make(chan struct{})}
	bh := newTestBeehive(t, store, withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	mustCreate(t, ctx, client, "w1", cSpec{})

	at, err := store.ObjectWritesMaxVersion(ctx, clientTestGK)
	require.NoError(t, err)

	_, ch, err := client.WatchList(ctx, WithResumeFrom(at))
	require.NoError(t, err)

	// Write only once the head check has failed: a write landing first gives the
	// replay a page to read, and it never reaches the empty-page branch at all.
	select {
	case <-store.failed:
	case <-ctx.Done():
		t.Fatal("the replay never ran its head check")
	}

	// The retry succeeds, so the resume completes and the stream goes live.
	obj := mustCreate(t, ctx, client, "w2", cSpec{})
	ev := recv(t, ch)
	assert.Equal(t, obj.ID, ev.ID)
	assert.True(t, store.spent.Load(), "the head check never failed")
}

// A head check that never succeeds ends with the caller, not on its own: the
// read decides nothing, so giving up on it would be guessing, and the retry has
// to be bounded by the same context everything else in the stream is.
func TestResumeHeadCheckRetryEndsWithTheCaller(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	store := &failHeadCheckStore{
		Store:  newClientTestStore(t),
		always: true,
		failed: make(chan struct{}),
	}
	bh := newTestBeehive(t, store, withWatchFloorInterval(time.Hour))
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	mustCreate(t, ctx, client, "w1", cSpec{})

	at, err := store.ObjectWritesMaxVersion(ctx, clientTestGK)
	require.NoError(t, err)

	watchCtx, endWatch := context.WithCancel(ctx)
	_, ch, err := client.WatchList(watchCtx, WithResumeFrom(at))
	require.NoError(t, err)

	select {
	case <-store.failed:
	case <-ctx.Done():
		t.Fatal("the replay never ran its head check")
	}
	endWatch()

	for got := range ch {
		require.NotEqual(t, Failed, got.Type, "a cancelled caller gets a silent close")
	}
}

// countingLoadFailStore fails the batched relation read a fixed number of times
// and then succeeds, so a test can watch the retry loop go round and finish.
type countingLoadFailStore struct {
	Store
	failuresLeft atomic.Int64
}

func (s *countingLoadFailStore) EdgesGroupOutgoingByID(ctx context.Context, ids []ObjectID, rel Relation) (map[ObjectID][]ObjectRef, error) {
	if s.failuresLeft.Add(-1) >= 0 {
		return nil, errBoom
	}
	return s.Store.EdgesGroupOutgoingByID(ctx, ids, rel)
}

// Only the relation load is retried. Decoding is pure and cannot fail the call,
// so a batch that repeated it would re-unmarshal and re-migrate every object in
// the page — up to tailPageCap of them — on each backoff step, and re-log every
// quarantined row with it.
func TestWatchLoadRetryDecodesOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	var decodes atomic.Int64
	mig := &fakeMigrator{
		specVersion: 1,
		convertSpec: func(_ int, raw json.RawMessage) (json.RawMessage, error) {
			decodes.Add(1)
			return raw, nil
		},
	}

	const failures = 2
	store := &countingLoadFailStore{Store: newClientTestStore(t)}
	store.failuresLeft.Store(failures)
	bh := newTestBeehive(t, store, withWatchFloorInterval(time.Hour))
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{}, WithMigrator(mig))
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	// Watch first: an object created before this is in the snapshot, which
	// decodes on its own path.
	_, ch, err := client.WatchList(ctx, WithLoads(LoadOwner()))
	require.NoError(t, err)
	decodes.Store(0)

	// Written through the store, so the row carries no schema version and the
	// migrator has something to convert. A Create would stamp the current one.
	spec, err := json.Marshal(cSpec{Val: "a"})
	require.NoError(t, err)
	obj, err := store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{Name: "w1", Spec: spec})
	require.NoError(t, err)
	require.NoError(t, bh.kindWriteHub.Send(clientTestGK))

	ev := recv(t, ch)
	require.Equal(t, obj.ID, ev.ID)

	assert.Zero(t, store.failuresLeft.Load()+1, "the load did not exhaust its failures")
	assert.Equal(t, int64(1), decodes.Load(), "the batch was decoded once per attempt")
}
