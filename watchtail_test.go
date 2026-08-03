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
	"fmt"
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

// A create publishes its kind's new log position, so a tailer never has to poll
// to learn that the kind moved.
func TestWakeHubPublishesOnCreate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t))
	rx := bh.wakes.Watch(clientTestGK)
	defer rx.Close()

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	mustCreate(t, ctx, client, "w1", cSpec{})

	ev, err := rx.RecvContext(ctx)
	require.NoError(t, err)
	assert.Greater(t, ev.Value, int64(0))
}

// Every path that appends an object_writes entry wakes the kind. The rows are
// the store's write-log call sites, not the public verbs: ConditionsSet and
// ConditionsDelete reach the log through bumpObject, which a verb-derived
// table misses. A write path missing from this table is one watchers see only
// on the floor tick.
func TestWakeHubPublishesOnEveryWrite(t *testing.T) {
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
			rx := w.bh.wakes.Watch(clientTestGK)
			defer rx.Close()

			var id ObjectID
			if tc.setup != nil {
				id = tc.setup(t, ctx, w)
			}
			drainRecv(rx)

			tc.write(t, ctx, w, id)
			ev, err := rx.RecvContext(ctx)
			require.NoError(t, err, "write published no wake")
			assert.Greater(t, ev.Value, int64(0))
		})
	}
}

// The owner cascade marks children of several kinds in one call, so one wake on
// the caller's kind is not enough: it is routed by the refs the store returns.
// The collection is driven directly — DeletionRequestsCreateFromOwner has one
// caller, gcCollect, and Delete on the owner only marks the owner.
func TestWakeHubPublishesPerCascadedKind(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	gkChildA := GroupKind{Kind: "ChildA"}
	gkChildB := GroupKind{Kind: "ChildB"}

	w := newWriteWorld(t)
	owner := mustCreate(t, ctx, w.client, "owner", cSpec{})
	mustCreate(t, ctx, NewClient[cSpec, cStatus](w.bh, gkChildA), "a", cSpec{}, WithOwner(owner.ID))
	mustCreate(t, ctx, NewClient[cSpec, cStatus](w.bh, gkChildB), "b", cSpec{}, WithOwner(owner.ID))
	require.NoError(t, w.client.Delete(ctx, owner.ID))

	rxA := w.bh.wakes.Watch(gkChildA)
	defer rxA.Close()
	rxB := w.bh.wakes.Watch(gkChildB)
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
func TestWakeHubSilentOnRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	w := newWriteWorld(t)
	rx := w.bh.wakes.Watch(clientTestGK)
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
func TestWakeHubClosesOnStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	t.Run("after start", func(t *testing.T) {
		bh, err := New(newClientTestStore(t), fast()...)
		require.NoError(t, err)
		stop, err := bh.Start(ctx)
		require.NoError(t, err)
		rx := bh.wakes.Watch(clientTestGK)
		defer rx.Close()

		require.NoError(t, stop(ctx))
		_, err = rx.RecvContext(ctx)
		assert.ErrorIs(t, err, gobus.ErrClosed)
	})

	t.Run("never started", func(t *testing.T) {
		bh, err := New(newClientTestStore(t))
		require.NoError(t, err)
		rx := bh.wakes.Watch(clientTestGK)
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
	require.NoError(t, bh.wakes.Send(clientTestGK))

	seen := make(map[ObjectID]bool, burst)
	for len(seen) < burst {
		ev, err := rx.RecvContext(ctx)
		require.NoError(t, err, "burst stalled after %d of %d", len(seen), burst)
		seen[ev.Key] = true
	}
	// Two pages, so two steps, so two position reads.
	assert.Equal(t, int64(2), store.positionReads.Load()-positionReadsAtStart)
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

// A writer in another Beehive publishes no wake here, so only the floor tick
// makes its write visible. Same store, second Beehive.
func TestTailerFloorTickPicksUpAForeignWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	store := newClientTestStore(t)
	bh := newTestBeehive(t, store, withWatchFloorInterval(fastTick))
	_, rx := startTailer(t, bh, clientTestGK)

	foreign, err := New(store)
	require.NoError(t, err)
	obj := mustCreate(t, ctx, NewClient[cSpec, cStatus](foreign, clientTestGK), "foreign", cSpec{})

	ev, err := rx.RecvContext(ctx)
	require.NoError(t, err, "a write through another Beehive was never picked up")
	assert.Equal(t, obj.ID, ev.Key)
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

	n, err := tailer.step(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)
	assert.Zero(t, tailer.cursor, "a step that found nothing must not advance")
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
	n, err := tailer.step(ctx)
	assert.ErrorIs(t, err, errBoom)
	assert.Zero(t, n)
	assert.Equal(t, at, tailer.cursor, "a failed step must not advance the cursor")
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

	bh, err := New(newClientTestStore(t), withWatchFloorInterval(time.Hour))
	require.NoError(t, err)
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
func TestTailerEndsWhenTheWakeHubCloses(t *testing.T) {
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

	bh.wakes.Close()
	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("the tailer outlived its wake hub")
	}
}

// buildRaceStore runs a hook inside the store read newObjectTailer makes, which
// is the window tailerFor holds no lock across — the one a rival start can win.
type buildRaceStore struct {
	Store
	// Not a sync.Once: the hook re-enters this method, and a second Do from the
	// same goroutine blocks on the first rather than skipping it.
	fired atomic.Bool
	hook  func()
}

func (s *buildRaceStore) ObjectWritesMaxVersion(ctx context.Context, gk GroupKind) (int64, error) {
	if s.hook != nil && s.fired.CompareAndSwap(false, true) {
		s.hook()
	}
	return s.Store.ObjectWritesMaxVersion(ctx, gk)
}

// The loser of a start race hands back the live tailer and discards its own, so
// a kind never ends up with two readers of one cursor. Driven through the build
// window rather than by racing goroutines: the losing branch needs a rival to
// win between the two lock holds, which real parallelism produces only sometimes
// and GOMAXPROCS=1 — how CI runs — never.
func TestTailerForDiscardsTheLoserOfAStartRace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	store := &buildRaceStore{Store: newClientTestStore(t)}
	bh := newTestBeehive(t, store, withWatchFloorInterval(time.Hour))

	var winner *objectTailer
	store.hook = func() {
		// Reentrant on purpose, and safe: tailerFor holds no lock here.
		var err error
		winner, err = bh.tailerFor(ctx, clientTestGK)
		require.NoError(t, err)
	}

	loser, err := bh.tailerFor(ctx, clientTestGK)
	require.NoError(t, err)
	defer func() { winner.release(); loser.release() }()
	require.NotNil(t, winner)
	assert.Same(t, winner, loser, "the loser returned its own tailer instead of the live one")
	assert.Equal(t, 1, tailerCount(bh), "the discarded tailer stayed registered")

	// And what it handed back is live, not the closed one it built.
	rx := loser.hub.Receiver()
	defer rx.Close()
	obj := mustCreate(t, ctx, NewClient[cSpec, cStatus](bh, clientTestGK), "raced", cSpec{})
	ev, err := rx.RecvContext(ctx)
	require.NoError(t, err)
	assert.Equal(t, obj.ID, ev.Key)
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
		withWatchPollInterval(time.Hour), withWatchFloorInterval(time.Hour))
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
		withWatchPollInterval(time.Hour), withWatchFloorInterval(time.Hour))
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
		withWatchPollInterval(time.Hour), withWatchFloorInterval(time.Hour))
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
		withWatchPollInterval(time.Hour), withWatchFloorInterval(time.Hour))
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
	changes, err := collectChanges(ctx, bh, clientTestGK, page)
	require.NoError(t, err)

	require.Len(t, changes, 2, "five writes to two objects collapse to two changes")
	assert.Equal(t, second.ID, changes[0].ID, "write order, not id order")
	assert.Equal(t, first.ID, changes[1].ID)
	assert.Contains(t, string(changes[1].Object.Spec), "first3", "current state, not a superseded one")
}

// Stop drains every watch: each stream closes, and no tailer goroutine outlives
// the beehive that started it.
func TestWatchGoroutinesDrainOnStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh, err := New(newClientTestStore(t), withWatchFloorInterval(time.Hour))
	require.NoError(t, err)

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
// resume check reads one entry and the kind's tailer starts above the gap, so
// neither collides with it.
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
	require.NoError(t, err, "the resume check reads its own entry and still passes")

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
			require.NoError(t, err, "the resume check reads one entry and still passes")

			<-tried // retry in progress, with the gap still unread
			cancel()
			for range ch { // the stream ends rather than parking on a send
			}
		})
	}
}
