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
	"sync/atomic"
	"testing"

	"github.com/amorey/beehive/internal/storeapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// statusWriteStore counts the status writes that reach the store, so a test can
// tell a skipped write from one the store compared and declined, and fails them
// while failNext is set.
type statusWriteStore struct {
	Store
	writes   atomic.Int64
	failNext atomic.Bool
}

func (s *statusWriteStore) Objects() storeapi.Objects {
	inner := s.Store.Objects()
	return objectsOverride{
		Objects: inner,
		updateStatus: func(ctx context.Context, gk GroupKind, id ObjectID, status []byte, v int) (bool, error) {
			s.writes.Add(1)
			if s.failNext.Load() {
				return false, errBoom
			}
			return inner.UpdateStatus(ctx, gk, id, status, v)
		},
	}
}

// newStatusBaselineFixture stores one object of the client kind and returns the
// pieces every test below builds its pass clients from.
func newStatusBaselineFixture(t *testing.T) (context.Context, *statusWriteStore, *Beehive, ObjectID) {
	t.Helper()
	ctx := context.Background()
	store := &statusWriteStore{Store: newClientTestStore(t)}
	bh := newTestBeehive(t, store)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "hello"})
	return ctx, store, bh, obj.ID
}

// passClientAt builds the client a pass would get for id: bound to the object,
// carrying the status bytes as currently stored.
func passClientAt(t *testing.T, ctx context.Context, bh *Beehive, store Store, id ObjectID) *controllerClientImpl[cStatus] {
	t.Helper()
	raw, err := store.Objects().Get(ctx, id)
	require.NoError(t, err)
	pass := newPassClient[cStatus](bh, clientTestGK, id)
	pass.baseline = newStatusBaseline(raw, migratorStatusVersion(bh.migratorFor(clientTestGK)))
	return pass
}

// The bytes the pass was handed are what the store holds, so a status equal to
// them is a write the store would decline. Skip it without the transaction.
func TestUpdateStatusSkipsWhatThePassCanSeeIsANoOp(t *testing.T) {
	ctx, store, bh, id := newStatusBaselineFixture(t)
	pass := passClientAt(t, ctx, bh, store, id)

	// A first status differs from the stored NULL, so it is written. Pins that
	// the empty baseline is "no bytes stored", not "no baseline".
	require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "done"}))
	assert.EqualValues(t, 1, store.writes.Load(), "a first status is a write")

	// Same bytes again, this time against the baseline its own write promoted.
	require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "done"}))
	assert.EqualValues(t, 1, store.writes.Load(), "promotion must re-enable the skip")

	// A fresh pass loads those bytes as its baseline and skips from the start.
	next := passClientAt(t, ctx, bh, store, id)
	require.NoError(t, next.UpdateStatus(ctx, cStatus{Val: "done"}))
	assert.EqualValues(t, 1, store.writes.Load(), "a load-time match skips too")
}

// The write-loss regression: a pass that writes A and then writes back the value
// it was loaded with must reach the store the second time.
func TestUpdateStatusBaselineAdvancesWithItsOwnWrites(t *testing.T) {
	ctx, store, bh, id := newStatusBaselineFixture(t)

	require.NoError(t, NewAdminClient[cStatus](bh, clientTestGK).UpdateStatus(ctx, id, cStatus{Val: "loaded"}))
	pass := passClientAt(t, ctx, bh, store, id)
	before := store.writes.Load()

	require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "other"}))
	require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "loaded"}))
	assert.EqualValues(t, before+2, store.writes.Load(),
		"the second write differs from what is stored and must not be skipped")

	assertStoredStatus(t, ctx, store, id, `{"Val":"loaded"}`)
}

// Which path a call takes depends on the bytes, so the skip must report a dead
// context exactly as the store call it replaced would have.
func TestUpdateStatusSkipReportsACanceledContext(t *testing.T) {
	ctx, store, bh, id := newStatusBaselineFixture(t)
	pass := passClientAt(t, ctx, bh, store, id)
	require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "done"}))

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	before := store.writes.Load()

	assert.ErrorIs(t, pass.UpdateStatus(canceled, cStatus{Val: "done"}), context.Canceled)
	assert.Equal(t, before, store.writes.Load(), "still a skip: the store is never reached")
}

// AdminClient is handed no object, so it holds no baseline: every call reaches
// the store, and claim must not panic on the nil.
func TestAdminClientHasNoBaseline(t *testing.T) {
	ctx, store, bh, id := newStatusBaselineFixture(t)

	admin := NewAdminClient[cStatus](bh, clientTestGK)
	require.NoError(t, admin.UpdateStatus(ctx, id, cStatus{Val: "same"}))
	require.NoError(t, admin.UpdateStatus(ctx, id, cStatus{Val: "same"}))
	assert.EqualValues(t, 2, store.writes.Load(), "no baseline, no skip")
}

// AfterCommit hooks run at the outermost commit, so inside a controller's own
// transaction no write has promoted yet. The claim is what stops the second call
// matching the stale load-time baseline and dropping the pass's last word.
func TestUpdateStatusInsideWithinDoesNotSkipOnAStaleBaseline(t *testing.T) {
	ctx, store, bh, id := newStatusBaselineFixture(t)

	require.NoError(t, NewAdminClient[cStatus](bh, clientTestGK).UpdateStatus(ctx, id, cStatus{Val: "loaded"}))
	pass := passClientAt(t, ctx, bh, store, id)

	require.NoError(t, pass.Within(ctx, func(ctx context.Context) error {
		if err := pass.UpdateStatus(ctx, cStatus{Val: "other"}); err != nil {
			return err
		}
		return pass.UpdateStatus(ctx, cStatus{Val: "loaded"})
	}))

	assertStoredStatus(t, ctx, store, id, `{"Val":"loaded"}`)
}

// The promote hook carries its own bytes, so a later write that failed cannot be
// promoted by an earlier write's hook.
func TestUpdateStatusFailedWriteDoesNotPromoteAnEarlierOne(t *testing.T) {
	ctx, store, bh, id := newStatusBaselineFixture(t)
	pass := passClientAt(t, ctx, bh, store, id)

	require.NoError(t, pass.Within(ctx, func(ctx context.Context) error {
		require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "first"}))
		store.failNext.Store(true)
		require.Error(t, pass.UpdateStatus(ctx, cStatus{Val: "second"}))
		store.failNext.Store(false)
		return nil // the controller swallows it
	}))

	// "second" never reached the store, so writing it now must not be skipped.
	require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "second"}))
	assertStoredStatus(t, ctx, store, id, `{"Val":"second"}`)
}

// A failed write stores nothing, so the bytes it carried must not become the
// baseline. With no other write outstanding there is nothing else holding the
// fast path off, so this is the case a promote on the error path loses.
func TestUpdateStatusFailedWriteDoesNotPromoteItsOwnBytes(t *testing.T) {
	ctx, store, bh, id := newStatusBaselineFixture(t)
	pass := passClientAt(t, ctx, bh, store, id)

	store.failNext.Store(true)
	require.Error(t, pass.UpdateStatus(ctx, cStatus{Val: "attempted"}))
	store.failNext.Store(false)

	require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "attempted"}))
	assertStoredStatus(t, ctx, store, id, `{"Val":"attempted"}`)
}

// A rolled-back write never landed, so the baseline must not claim it.
func TestUpdateStatusRolledBackWriteDoesNotPromote(t *testing.T) {
	ctx, store, bh, id := newStatusBaselineFixture(t)
	pass := passClientAt(t, ctx, bh, store, id)

	require.Error(t, pass.Within(ctx, func(ctx context.Context) error {
		if err := pass.UpdateStatus(ctx, cStatus{Val: "rolled-back"}); err != nil {
			return err
		}
		return errBoom
	}))

	require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "rolled-back"}))
	assertStoredStatus(t, ctx, store, id, `{"Val":"rolled-back"}`)
}

// A skip never reads, so it cannot notice that the row was collected mid-pass.
// Documented behavior: the write would have written nothing either way.
func TestUpdateStatusSkipOnACollectedObject(t *testing.T) {
	ctx, store, bh, id := newStatusBaselineFixture(t)
	pass := passClientAt(t, ctx, bh, store, id)

	require.NoError(t, pass.UpdateStatus(ctx, cStatus{Val: "done"}))
	next := passClientAt(t, ctx, bh, store, id)

	require.NoError(t, store.Objects().Delete(ctx, id))
	before := store.writes.Load()

	assert.NoError(t, next.UpdateStatus(ctx, cStatus{Val: "done"}),
		"a skip answers from the baseline and never learns the row is gone")
	assert.Equal(t, before, store.writes.Load())
}

func assertStoredStatus(t *testing.T, ctx context.Context, store Store, id ObjectID, want string) {
	t.Helper()
	raw, err := store.Objects().Get(ctx, id)
	require.NoError(t, err)
	assert.JSONEq(t, want, string(raw.Status))
}
