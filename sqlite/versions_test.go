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
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"
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

// withoutVersionBlocks sends every draw to the counter row, which is what the
// draw-failure and draw-accounting tests observe. Call it before the store is
// built: open reserves a block.
func withoutVersionBlocks(t *testing.T) {
	t.Helper()
	withBlockSize(t, 0)
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

	var sites []string
	require.NoError(t, filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		var fn string
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.FuncDecl:
				fn = node.Name.Name
			case *ast.BasicLit:
				if node.Kind == token.STRING && writesSeq.MatchString(node.Value) {
					sites = append(sites, fn)
				}
			}
			return true
		})
		return nil
	}))
	assert.Equal(t, []string{"drawResourceVersions"}, sites)
}
