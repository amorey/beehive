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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/amorey/gobus"
	"github.com/amorey/gobus/conflate"
	"github.com/amorey/gobus/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A create publishes its kind's new log position, so a tailer never has to poll
// to learn that the kind moved.
func TestWakeHubPublishesOnCreate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
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
// ConditionsDelete reach the log through bumpObject, which a verb-derived table
// misses. A write no row covers is a write watchers see only on the floor tick.
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
			drainWakes(rx)

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
// after the read has taken its value — the interleaving that costs a wake if the
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

// The tailer registers its wake receiver before it reads its starting cursor. A
// write that commits in between is above the cursor and its wake is in the slot;
// the other order drops it and the object is never delivered.
func TestTailerLosesNoWriteAtStartup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	store := &writeDuringMaxVersionStore{Store: newClientTestStore(t)}
	bh, err := New(store)
	require.NoError(t, err)
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
	gateReads atomic.Int64
}

func (s *countingTailStore) ObjectWritesMaxVersion(ctx context.Context, gk GroupKind) (int64, error) {
	s.gateReads.Add(1)
	return s.Store.ObjectWritesMaxVersion(ctx, gk)
}

// A burst larger than one page drains on its own. The wakes coalesce into one
// slot, so a tailer that read a single page per wake would strand the remainder
// until some unrelated later write. The drain loop ends on a short page rather
// than on a second position read, so a step costs one gate read.
func TestTailerDrainsBurstAbovePageCap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	const burst = tailPageCap + 88

	store := &countingTailStore{Store: newClientTestStore(t)}
	bh, err := New(store)
	require.NoError(t, err)

	_, rx := startTailer(t, bh, clientTestGK)
	gateReadsAtStart := store.gateReads.Load()

	// Written past the client so the burst carries no wakes of its own, then
	// woken once: a burst coalesces to one wake anyway, and this makes the read
	// count the tailer's rather than the interleaving's.
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
	// Two pages, so two steps, so two gate reads.
	assert.Equal(t, int64(2), store.gateReads.Load()-gateReadsAtStart)
}

// Two changes for one object that a subscriber has not read yet coalesce by the
// merge table. Nothing annihilates: the pending slot is hub-wide but a
// subscriber's snapshot is its own, so dropping a create/delete pair would leave
// a subscriber that snapshotted between them holding a deleted object forever.
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
				drainConflate(pending)
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

func drainConflate(rx *conflate.Receiver[ObjectID, rawChange]) {
	for {
		if _, err := rx.TryRecv(); err != nil {
			return
		}
	}
}

// A writer this process shares no memory with publishes no wake, so the floor
// tick is what makes its write visible. Same store, second Beehive.
func TestTailerFloorTickPicksUpAForeignWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	store := newClientTestStore(t)
	bh, err := New(store, withWatchFloorInterval(fastTick))
	require.NoError(t, err)
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
	bh, err := New(store, withWatchFloorInterval(fastTick))
	require.NoError(t, err)
	_, rx := startTailer(t, bh, clientTestGK)

	store.failures.Store(1)
	obj := mustCreate(t, ctx, NewClient[cSpec, cStatus](bh, clientTestGK), "retried", cSpec{})

	ev, err := rx.RecvContext(ctx)
	require.NoError(t, err, "the tailer never retried the failed step")
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
		tailer.run(bh.tailCtx)
	}()
	t.Cleanup(func() {
		rx.Close()
		bh.tailCancel()
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
	bh, err := New(newClientTestStore(t))
	require.NoError(t, err)
	// Registered but never started: WithFinalizers refuses a kind no controller
	// in this process can clear, and nothing here needs a reconcile loop.
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	return &writeWorld{
		bh:     bh,
		client: NewClient[cSpec, cStatus](bh, clientTestGK),
		ctrl:   &controllerClientImpl[cStatus]{bh: bh, gk: clientTestGK},
	}
}

// drainWakes discards whatever the receiver is holding, so the next Recv proves
// a wake published after the drain.
func drainWakes(rx *watch.Receiver[GroupKind, int64]) {
	for {
		if _, err := rx.TryRecv(); err != nil {
			return
		}
	}
}
