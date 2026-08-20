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
	"fmt"
	"go/ast"
	"path/filepath"
	"regexp"
	"sync"
	"testing"

	storeapi "github.com/amorey/beehive/internal/storeapi"
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
		v.settle()
		assert.Equal(t, int64(40), v.latest())
	})

	t.Run("settle reports only what a commit took", func(t *testing.T) {
		v := versions{next: 1, end: 5}
		v.settle()
		assert.Equal(t, int64(0), v.latest(), "nothing has been handed out")

		_, _ = v.take(2)
		assert.Equal(t, int64(0), v.latest(), "drawn, not committed")
		v.settle()
		assert.Equal(t, int64(2), v.latest())
	})
}

// A reservation that lands after the allocator has moved past it is discarded.
// Two refills can be in flight at once — and a fallback draw can land between a
// refill's draw and its install — so the block installed last is not the block
// drawn last.
func TestAStaleReservationIsDiscarded(t *testing.T) {
	t.Run("a later refill installed first", func(t *testing.T) {
		var v versions
		v.reserve(3072, 1024) // the higher block, installed first
		hi, ok := v.take(1)
		require.True(t, ok)

		v.reserve(2048, 1024) // the lower block, drawn first, landing late
		next, ok := v.take(1)
		require.True(t, ok)
		assert.Greater(t, next, hi, "a version must never be handed out below one already taken")
	})

	t.Run("a fallback draw landed first", func(t *testing.T) {
		var v versions
		v.record(2049) // a fallback draw took the counter past the pending block

		v.reserve(2048, 1024)
		_, ok := v.take(1)
		assert.False(t, ok, "the stale block must not resurrect versions below the fallback")
	})
}

// End to end, with a block small enough that most writes refill. Not the guard for
// the reordering above — the window between a refill's draw and its install is too
// narrow to hit on demand, and removing the guard does not fail this. It catches a
// duplicate or a backwards version from any cause. On disk and under -race,
// because OpenMemory's single connection hides the overlap.
func TestConcurrentWritersNeverGoBackwards(t *testing.T) {
	withBlockSize(t, 2)
	ctx := context.Background()
	store := newDiskStore(t)

	const writers, each = 8, 20
	objs := make([]*storeapi.RawObject, writers)
	for i := range objs {
		objs[i] = newKindObject(t, store, testGK)
	}

	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				_, err := store.Objects().UpdateStatus(ctx, testGK, objs[w].ID, fmt.Appendf(nil, `{"n":%d}`, i), 0)
				assert.NoError(t, err)
			}
		}()
	}
	wg.Wait()

	entries, _, err := store.ObjectWrites().ListSinceAll(ctx, 0, writers*each*2)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	seen := make(map[int64]bool, len(entries))
	var last int64
	for _, e := range entries {
		assert.False(t, seen[e.ResourceVersion], "version %d handed out twice", e.ResourceVersion)
		seen[e.ResourceVersion] = true
		assert.Greater(t, e.ResourceVersion, last, "the write log must not go backwards")
		last = e.ResourceVersion
	}
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

// A mark takes its version from the allocator, so the row and its write log entry
// carry one value.
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

// A restart resumes above the previous process's whole block, not above the
// versions it happened to use: the unused tail is burned, and gaps are free.
func TestVersionsResumeAboveThePreviousProcess(t *testing.T) {
	withBlockSize(t, 64)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "b.db")

	first, err := Open(path)
	require.NoError(t, err)
	obj := newRefObject(t, first)
	_, err = first.Objects().UpdateStatus(ctx, testGK, obj.ID, []byte(`{"n":1}`), 0)
	require.NoError(t, err)
	reserved := seqValue(t, first)
	require.NoError(t, first.Close())

	second, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, second.Close()) })
	_, err = second.Objects().UpdateStatus(ctx, testGK, obj.ID, []byte(`{"n":2}`), 0)
	require.NoError(t, err)
	after, err := second.Objects().Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Greater(t, after.ResourceVersion, reserved,
		"a resumed process must start above the whole block, not above what it used")
}

// The refill runs after the commit, so its failure cannot be reported. The write
// stands, and the next draw raises the error where a caller can act on it.
func TestAFailedRefillLeavesTheWriteAlone(t *testing.T) {
	withBlockSize(t, 1)
	ctx := context.Background()
	store := newRawStore(t)
	obj := newRefObject(t, store)

	_, err := store.db.ExecContext(ctx, `DROP TABLE resource_version_seq`)
	require.NoError(t, err)

	_, err = store.Objects().UpdateStatus(ctx, testGK, obj.ID, []byte(`{"n":1}`), 0)
	require.NoError(t, err, "the block still covered this write; the failed refill is not its problem")

	_, err = store.Objects().UpdateStatus(ctx, testGK, obj.ID, []byte(`{"n":2}`), 0)
	require.Error(t, err, "with the block spent the fallback must surface it")
}

// published is what the cursor sites read, so it must never cover a version an
// open transaction has drawn but not committed.
func TestAnOpenTransactionPublishesNothing(t *testing.T) {
	withBlockSize(t, 64)
	ctx := context.Background()
	store := newRawStore(t)
	obj := newRefObject(t, store)

	var inside int64
	require.NoError(t, store.Within(ctx, func(ctx context.Context) error {
		if _, err := store.Objects().UpdateStatus(ctx, testGK, obj.ID, []byte(`{"n":1}`), 0); err != nil {
			return err
		}
		var err error
		inside, err = store.GetLatestResourceVersion(context.Background())
		return err
	}))

	written, err := store.Objects().Get(ctx, obj.ID)
	require.NoError(t, err)
	assert.Less(t, inside, written.ResourceVersion, "an uncommitted version must stay unpublished")

	after, err := store.GetLatestResourceVersion(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, after, written.ResourceVersion, "...and be published once it commits")
}

// A rolled-back transaction burns its versions rather than publishing them.
func TestARollbackPublishesNothing(t *testing.T) {
	withBlockSize(t, 64)
	ctx := context.Background()
	store := newRawStore(t)
	obj := newRefObject(t, store)

	before, err := store.GetLatestResourceVersion(ctx)
	require.NoError(t, err)

	boom := errors.New("boom")
	err = store.Within(ctx, func(ctx context.Context) error {
		if _, err := store.Objects().UpdateStatus(ctx, testGK, obj.ID, []byte(`{"n":1}`), 0); err != nil {
			return err
		}
		return boom
	})
	require.ErrorIs(t, err, boom)

	after, err := store.GetLatestResourceVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

// Every draw goes through the allocator, so the counter statement has one site.
// A second one would draw behind the block and hand out a version twice.
func TestTheCounterIsWrittenInOnePlace(t *testing.T) {
	writesSeq := regexp.MustCompile(`(?is)UPDATE\s+resource_version_seq`)
	sites := sqlSites(t, writesSeq.MatchString)
	assert.Equal(t, []string{"drawResourceVersions"}, sites)
}

// published is only a lower bound on committed versions because draws are ordered
// by the writer connection: every draw site runs inside a Within, so two of them
// cannot interleave. A draw added outside one would autocommit, never publish, and
// freeze the cursor below live writes — silently. "No site draws outside a
// transaction" is a claim over an open set, so this asserts the structure instead.
func TestEveryDrawSiteIsInsideATransaction(t *testing.T) {
	var sites []string
	require.NoError(t, inspectPackage(t, func(fn string, n ast.Node) {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if ok && (sel.Sel.Name == "nextResourceVersion" || sel.Sel.Name == "advanceResourceVersion") {
			sites = append(sites, fn)
		}
	}))

	assert.ElementsMatch(t, []string{
		"nextResourceVersion", // the one-version wrapper over advanceResourceVersion
		"objectsCreate",
		"recordObjectWrite",
		"Add", // Events().Add
		"markForDeletion",
		"markManyForDeletion",
		"objectsDelete",
	}, sites, "a new draw site must run inside Within, or published stops bounding it")
}

// The seed read runs after the migrations, so a database whose migrations are
// recorded but whose counter row is gone fails at open rather than handing out
// versions from zero.
func TestOpenReportsAMissingCounter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.db")
	first, err := Open(path)
	require.NoError(t, err)
	_, err = first.db.ExecContext(context.Background(), `DROP TABLE resource_version_seq`)
	require.NoError(t, err)
	require.NoError(t, first.Close())

	_, err = Open(path)
	require.Error(t, err)
}
