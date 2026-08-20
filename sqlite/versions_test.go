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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A block covers takes that fit and refuses the one that does not, and a refused
// take spends what is left: the fallback draws from the table, which already sits
// above the block, so a later take from the same block would go backwards.
func TestVersionBlockCoversWhatFits(t *testing.T) {
	t.Run("takes in order", func(t *testing.T) {
		v := versions{next: 1, end: 5}
		hi, ok := v.take(2)
		assert.True(t, ok)
		assert.Equal(t, int64(2), hi)

		hi, ok = v.take(2)
		assert.True(t, ok)
		assert.Equal(t, int64(4), hi)

		_, ok = v.take(1)
		assert.False(t, ok, "the block is spent")
	})

	t.Run("a refused take spends the block", func(t *testing.T) {
		v := versions{next: 1, end: 5}
		_, ok := v.take(9)
		assert.False(t, ok)

		_, ok = v.take(1)
		assert.False(t, ok, "a version left behind the fallback would go backwards")
	})

	t.Run("record carries the fallback's draw", func(t *testing.T) {
		v := versions{next: 1, end: 1}
		v.record(40)
		v.publish()
		assert.Equal(t, int64(40), v.latest())
	})

	t.Run("publish reports only what a commit took", func(t *testing.T) {
		v := versions{next: 1, end: 5}
		v.publish()
		assert.Equal(t, int64(0), v.latest(), "nothing has been handed out")

		_, _ = v.take(2)
		assert.Equal(t, int64(0), v.latest(), "drawn, not committed")
		v.publish()
		assert.Equal(t, int64(2), v.latest())
	})
}

// withBlockSize sets the reservation size for one test.
func withBlockSize(t *testing.T, n int) {
	t.Helper()
	prev := blockSize
	blockSize = n
	t.Cleanup(func() { blockSize = prev })
}

// Writes keep taking increasing versions across a block boundary, and the counter
// page is written once a block rather than once a write — which is the whole point.
func TestWritesDrawFromTheBlock(t *testing.T) {
	withBlockSize(t, 4)
	ctx := context.Background()
	store := newRawStore(t)
	obj := newRefObject(t, store)

	before := seqValue(t, store)
	var last int64
	for i := range 10 {
		_, err := store.Objects().UpdateStatus(ctx, testGK, obj.ID, fmt.Appendf(nil, `{"n":%d}`, i), 0)
		require.NoError(t, err)
		got, err := store.Objects().Get(ctx, obj.ID)
		require.NoError(t, err)
		assert.Greater(t, got.ResourceVersion, last, "versions must increase across a block boundary")
		last = got.ResourceVersion
	}

	drawn := seqValue(t, store) - before
	assert.Less(t, drawn, int64(10), "ten writes must not write the counter ten times")
}

// The counter row holds the block's end, so the write cursor cannot be read from
// it: a value above what has been handed out strands every write in the gap.
func TestLatestResourceVersionStaysAtWhatWasHandedOut(t *testing.T) {
	withBlockSize(t, 64)
	ctx := context.Background()
	store := newRawStore(t)
	obj := newRefObject(t, store)

	_, err := store.Objects().UpdateStatus(ctx, testGK, obj.ID, []byte(`{"n":1}`), 0)
	require.NoError(t, err)
	written, err := store.Objects().Get(ctx, obj.ID)
	require.NoError(t, err)

	latest, err := store.GetLatestResourceVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, written.ResourceVersion, latest,
		"the cursor must be the last version a write took, not the block's end")
	assert.Less(t, latest, seqValue(t, store), "...which is below the reservation")
}

// GetForReconcile's cursor becomes reconciled_against, and ListStaleSince selects
// on target.resource_version > it. Above what was handed out, every target write
// in the rest of the block sits under the watermark and the dependent never learns.
func TestReconcileCursorStaysAtWhatWasHandedOut(t *testing.T) {
	withBlockSize(t, 64)
	ctx := context.Background()
	store := newRawStore(t)
	obj := newRefObject(t, store)

	_, err := store.Objects().UpdateStatus(ctx, testGK, obj.ID, []byte(`{"n":1}`), 0)
	require.NoError(t, err)

	load, err := store.Objects().GetForReconcile(ctx, obj.ID)
	require.NoError(t, err)
	assert.Equal(t, load.Object.ResourceVersion, load.Cursor,
		"the cursor must not run ahead of the write it just read")

	// A write after the load must land above the cursor, or the sweep skips it.
	_, err = store.Objects().UpdateStatus(ctx, testGK, obj.ID, []byte(`{"n":2}`), 0)
	require.NoError(t, err)
	after, err := store.Objects().Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Greater(t, after.ResourceVersion, load.Cursor)
}

// markForDeletion used to assign its version in SQL, from the counter row. With a
// block reserved that row is the block's end, so the mark took a version outside
// the block and a later write handed the same one out again.
func TestDeletionMarkTakesItsVersionFromTheBlock(t *testing.T) {
	withBlockSize(t, 64)
	ctx := context.Background()
	store := newRawStore(t)
	obj := newRefObject(t, store)

	res, err := store.DeletionRequests().Create(ctx, testGK, obj.ID)
	require.NoError(t, err)
	require.True(t, res.Marked)

	marked, err := store.Objects().Get(ctx, obj.ID)
	require.NoError(t, err)
	latest, err := store.GetLatestResourceVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, latest, marked.ResourceVersion,
		"the mark must take the version the allocator handed out")

	entries, _, err := store.ObjectWrites().ListSinceAll(ctx, marked.ResourceVersion-1, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, marked.ResourceVersion, entries[0].ResourceVersion,
		"the row and its write log entry must carry one version")
}

