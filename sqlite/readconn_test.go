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
	"path/filepath"
	"testing"

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
