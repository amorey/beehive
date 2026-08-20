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
