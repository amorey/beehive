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
	"testing"

	"github.com/amorey/gobus"
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
