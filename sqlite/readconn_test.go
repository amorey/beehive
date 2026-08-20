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
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/amorey/beehive/internal/storeapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A read inside a live transaction must run on that transaction, or it reads
// the database as of before the transaction's own uncommitted writes.
func TestAReadJoinsItsTransaction(t *testing.T) {
	store := newTestStore(t).(*sqliteStore)
	ctx := context.Background()

	require.NoError(t, store.Within(ctx, func(ctx context.Context) error {
		obj, err := store.Objects().Create(ctx, testGK, storeapi.ObjectsCreateInput{
			Name: "written-inside", Spec: []byte(`{}`),
		})
		require.NoError(t, err)

		got, err := store.Objects().GetMeta(ctx, obj.ID)
		require.NoError(t, err, "the read must see the transaction's own write")
		assert.Equal(t, obj.ID, got.ID)
		return nil
	}))
}

// A ctx outlives its transaction, so a read on a closed frame degrades to the
// pool. Returning the dead transaction yields sql.ErrTxDone on every hook.
func TestAReadOnAClosedFrameUsesThePool(t *testing.T) {
	store := newTestStore(t).(*sqliteStore)
	ctx := context.Background()

	var escaped context.Context
	require.NoError(t, store.Within(ctx, func(ctx context.Context) error {
		escaped = ctx
		return nil
	}))

	// The frame is closed now; read must hand back the pool.
	require.NotNil(t, store.read(escaped))
	_, err := store.Objects().GetMeta(escaped, storeapi.ObjectID(1))
	assert.ErrorIs(t, err, storeapi.ErrNotFound, "want a clean miss, not sql.ErrTxDone")
}

func TestOpenUsesADefaultReadPool(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "b.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	require.NotNil(t, store.readDB, "an on-disk store gets a read pool")
	assert.Equal(t, defaultReadConnections, store.readDB.Stats().MaxOpenConnections)
}

func TestWithReadConnections(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "b.db"), WithReadConnections(2))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	assert.Equal(t, 2, store.readDB.Stats().MaxOpenConnections)

	for _, n := range []int{0, -1} {
		_, err := Open(filepath.Join(t.TempDir(), "bad.db"), WithReadConnections(n))
		assert.ErrorIs(t, err, ErrInvalidOption, "n = %d", n)
	}
}

// The database cannot be opened twice in memory, so the option is a no-op there
// rather than a second, empty database.
func TestOpenMemoryHasNoReadPool(t *testing.T) {
	store, err := OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	assert.Nil(t, store.readDB)
}

func TestCloseClosesBothPools(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "b.db"))
	require.NoError(t, err)
	require.NoError(t, store.Close())

	_, err = store.readDB.ExecContext(context.Background(), `SELECT 1`)
	assert.Error(t, err, "the read pool should be closed too")
	assert.NoError(t, store.Close(), "Close is idempotent")
}

// newDiskStore is the split store: OpenMemory cannot open its database twice,
// so only an on-disk store exercises two pools.
func newDiskStore(t *testing.T, opts ...Option) *sqliteStore {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "b.db"), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

// The point of the split: a read that is not inside a transaction completes
// while a write transaction holds the writer. On one pool it would block until
// the transaction committed.
func TestAReadProceedsDuringAWriteTransaction(t *testing.T) {
	store := newDiskStore(t)
	ctx := context.Background()

	obj, err := store.Objects().Create(ctx, testGK, storeapi.ObjectsCreateInput{
		Name: "target", Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	inTx := make(chan struct{})
	readDone := make(chan error, 1)
	require.NoError(t, store.Within(ctx, func(txCtx context.Context) error {
		// A write, so the transaction really holds the writer.
		_, err := store.Objects().UpdateStatus(txCtx, testGK, obj.ID, []byte(`{"a":1}`), 0)
		require.NoError(t, err)

		go func() {
			<-inTx
			_, err := store.Objects().GetMeta(ctx, obj.ID) // ctx, not txCtx
			readDone <- err
		}()
		close(inTx)

		select {
		case err := <-readDone:
			return err
		case <-time.After(10 * time.Second):
			return errors.New("a read outside the transaction blocked on the writer")
		}
	}))
}

// ...and it reads committed state, not the transaction's uncommitted write.
func TestAReadOutsideATransactionDoesNotSeeItsWrites(t *testing.T) {
	store := newDiskStore(t)
	ctx := context.Background()

	obj, err := store.Objects().Create(ctx, testGK, storeapi.ObjectsCreateInput{
		Name: "target", Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	seen := make(chan []byte, 1)
	require.NoError(t, store.Within(ctx, func(txCtx context.Context) error {
		_, err := store.Objects().UpdateStatus(txCtx, testGK, obj.ID, []byte(`{"a":1}`), 0)
		require.NoError(t, err)

		done := make(chan struct{})
		go func() {
			defer close(done)
			got, err := store.Objects().GetMeta(ctx, obj.ID)
			require.NoError(t, err)
			seen <- got.Status
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			return errors.New("the read blocked")
		}
		return nil
	}))
	assert.Empty(t, <-seen, "the reader must not see an uncommitted write")

	got, err := store.Objects().GetMeta(ctx, obj.ID)
	require.NoError(t, err)
	assert.JSONEq(t, `{"a":1}`, string(got.Status), "and must see it once committed")
}

// Every write path, on a split store. The suite runs in memory, where read
// falls back to the writer and a write misrouted onto the reader would pass;
// here query_only refuses it and says so.
func TestEveryWritePathRunsOnTheWriter(t *testing.T) {
	store := newDiskStore(t)
	ctx := context.Background()

	obj, err := store.Objects().Create(ctx, testGK, storeapi.ObjectsCreateInput{
		Name: "a", Spec: []byte(`{}`), Finalizers: []string{"f"},
	})
	require.NoError(t, err)
	target, err := store.Objects().Create(ctx, testGK, storeapi.ObjectsCreateInput{
		Name: "b", Spec: []byte(`{}`),
	})
	require.NoError(t, err)

	_, _, err = store.Objects().UpdateSpec(ctx, testGK, obj.ID, []byte(`{"s":1}`), 0)
	require.NoError(t, err)
	_, _, err = store.Objects().UpdateSpecByName(ctx, testGK, "a", []byte(`{"s":2}`), 0)
	require.NoError(t, err)
	_, err = store.Objects().UpdateStatus(ctx, testGK, obj.ID, []byte(`{"t":1}`), 0)
	require.NoError(t, err)
	_, err = store.Objects().SetObservedGeneration(ctx, testGK, obj.ID, 1)
	require.NoError(t, err)
	require.NoError(t, store.Conditions().Set(ctx, testGK, obj.ID,
		storeapi.Condition{Type: "Ready", Status: "True"}))
	require.NoError(t, store.Conditions().Delete(ctx, testGK, obj.ID, "Ready"))
	require.NoError(t, store.Events().Add(ctx, testGK, obj.ID,
		storeapi.EventsAddInput{Category: "c", Type: "Normal", Reason: "R"}))
	_, err = store.Edges().Add(ctx, obj.ID, target.ID, storeapi.RelationDependsOn)
	require.NoError(t, err)
	require.NoError(t, store.Dependencies().WatermarkSet(ctx, obj.ID, 1))
	require.NoError(t, store.ReconcileOwed().Stamp(ctx,
		[]storeapi.ObjectRef{{ID: obj.ID, Group: testGK.Group, Kind: testGK.Kind}}))
	require.NoError(t, store.ReconcileOwed().Decrement(ctx, testGK, obj.ID, 1))
	_, err = store.ReconcileOwed().Sweep(ctx, []storeapi.GroupKind{testGK})
	require.NoError(t, err)
	require.NoError(t, store.DriverCursors().Set(ctx, "probe", 7))
	_, err = store.Edges().Delete(ctx, obj.ID, target.ID, storeapi.RelationDependsOn)
	require.NoError(t, err)

	_, err = store.DeletionRequests().Create(ctx, testGK, obj.ID)
	require.NoError(t, err)
	_, err = store.Objects().DeleteFinalizer(ctx, testGK, obj.ID, "f")
	require.NoError(t, err)
	require.NoError(t, store.Objects().Delete(ctx, obj.ID))

	_, err = store.Events().Sweep(ctx, 1, time.Hour, 16)
	require.NoError(t, err)
	_, err = store.ObjectWrites().Sweep(ctx, 1, time.Hour)
	require.NoError(t, err)
	_, err = store.ReclaimSpace(ctx, 8)
	require.NoError(t, err)
}

// database/sql keeps two idle connections by default and OpenPool reaps after
// five minutes, so a reader would churn connections between ticks. Asserted on
// the pool's own counters: no test can wait out a five-minute timer.
func TestTheReaderKeepsItsConnections(t *testing.T) {
	const n = 4
	store := newDiskStore(t, WithReadConnections(n))
	ctx := context.Background()
	id := newRefObject(t, store).ID

	// More concurrent reads than the idle default, so connections are opened and
	// then returned to the pool.
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				_, err := store.Objects().GetMeta(ctx, id)
				require.NoError(t, err)
			}
		}()
	}
	wg.Wait()

	stats := store.readDB.Stats()
	assert.Zero(t, stats.MaxIdleClosed, "a returned connection was closed over the idle count")
	assert.Zero(t, stats.MaxIdleTimeClosed, "a returned connection was reaped on a timer")
}
