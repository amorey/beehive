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
	"strconv"
	"testing"

	"github.com/amorey/beehive/sqlite"
	"github.com/stretchr/testify/require"
)

// benchStaleEdgesPerObject is how many targets each object depends on, so the
// graph carries twice the objects in edges — the shape
// docs/adr/2026-08-03-stale-dependents-cursor.md measured the uncursored scan
// against.
const benchStaleEdgesPerObject = 2

// BenchmarkStaleDependentsSweep measures what one sweep of the stale-dependents
// pass costs against a converged graph.
//
// Converged is the worst case, not the easy one: LIMIT cannot stop the scan
// early when no row matches, so a healthy system pays the whole scan in one
// query. The pass cannot be disabled, so that is a cost every deployment pays
// on every tick forever, which is what the cursor is for.
//
// The three cases are the three positions a cursor can be in relative to the
// store's sequence, and the claim is that only the first tracks graph size:
//
//   - cold-start: the cursor at 0, the whole graph in scope. This is the
//     uncursored cost, kept as the reference the other two are read against.
//   - one-target-moved: the cursor one write behind. Should track the change,
//     not the graph.
//   - quiet: the cursor level with the mark. Should be one
//     ResourceVersionsMaxIssued and no listing at all.
//
// The sweeper is built over a bare Beehive, so every enqueue resolves to no
// reconciler and drops. That isolates the store work; a sweep in a running
// beehive also pays a work-queue add per finding, which coalesces per object.
//
// Each case builds its own graph. one-target-moved leaves dependents stale
// behind it, which a shared graph would hand to the next case as work its own
// setup never asked for.
func BenchmarkStaleDependentsSweep(b *testing.B) {
	ctx := context.Background()
	for _, objects := range []int{1_000, 10_000} {
		b.Run(strconv.Itoa(objects)+"-objects", func(b *testing.B) {
			b.Run("cold-start", func(b *testing.B) {
				store, _ := benchStaleGraph(b, objects)
				sd := sweeperOver(store)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					sd.cursor = 0
					sd.sweep(ctx)
				}
			})

			// Each iteration moves a different target, so no iteration re-reads
			// the rows the last one left in the page cache.
			b.Run("one-target-moved", func(b *testing.B) {
				store, ids := benchStaleGraph(b, objects)
				sd := sweeperOver(store)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					// The mark is read before the write, so the write lands
					// above the cursor and the sweep has exactly one target in
					// scope. Its dependents stay stale afterwards, but every
					// later iteration's cursor sits above them.
					from, err := store.ResourceVersionsMaxIssued(ctx)
					require.NoError(b, err)
					target := ids[i%len(ids)]
					_, _, err = store.ObjectsUpdateSpec(ctx, clientTestGK, target, benchSpec(), 0)
					require.NoError(b, err)
					sd.cursor = from
					b.StartTimer()

					sd.sweep(ctx)
				}
			})

			b.Run("quiet", func(b *testing.B) {
				store, _ := benchStaleGraph(b, objects)
				sd := sweeperOver(store)
				mark, err := store.ResourceVersionsMaxIssued(ctx)
				require.NoError(b, err)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					sd.cursor = mark
					sd.sweep(ctx)
				}
			})
		})
	}
}

// BenchmarkWakerScanRateUnderSustainedWrites measures what one wake-driven pass
// costs. That is what wakeScanMinInterval has to be set against: the throttle
// bounds passes per second, and a pass's cost times that rate is the share of
// the read pool the waker holds away from the other readers generating the
// wakes.
//
// Reads and cursor writes are reported separately on purpose. Every read
// competes for connection time; the cursor write competes for the write lock,
// which is what the commits themselves need.
//
// The three backlogs are the three shapes a wake finds:
//
//   - quiet: nothing above the watermark, which is what most wakes find. One
//     empty listing, no edges lookup, no cursor write.
//   - one-page: an ordinary burst.
//   - full-budget: a resume, where the pass stops at wakeScanPagesPerPass and
//     the loop re-arms at the throttle rather than the floor.
func BenchmarkWakerScanRateUnderSustainedWrites(b *testing.B) {
	ctx := context.Background()
	for _, backlog := range []struct {
		name string
		rows int
	}{
		{"quiet", 0},
		{"one-page", wakeScanPageCap},
		{"full-budget", wakeFullBudget},
	} {
		b.Run(backlog.name, func(b *testing.B) {
			store := benchWakeLog(b, backlog.rows)
			dw, _ := wakerOver(store, clientTestGK)
			clk := fakeClockOn(&dw.now)
			require.NotEqual(b, scanFailed, dw.seed(ctx))
			resume := dw.watermark - int64(backlog.rows)

			store.reads, store.cursorWrites = 0, 0
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Timed: two int64 stores are far below the noise, and
				// StopTimer/StartTimer would cost two stop-the-worlds a pass.
				dw.watermark, dw.persisted = resume, resume

				dw.pass(ctx, clk.now(), false)
				clk.advance(defaultWakeScanMinInterval) // the rate the throttle allows
			}
			b.StopTimer()

			b.ReportMetric(float64(store.reads)/float64(b.N), "reads/pass")
			b.ReportMetric(float64(store.cursorWrites)/float64(b.N), "cursor-writes/pass")
		})
	}
}

// benchWakeLog builds a store whose write log holds rows entries above the
// waker's seed point, each with dependents to wake, and counts what a pass
// costs it.
//
// The graph is benchStaleGraph's: same shape, and its watermark backfill writes
// no write-log entry, so the log still holds exactly one row per object.
func benchWakeLog(b *testing.B, rows int) *wakeCountingStore {
	b.Helper()
	store, _ := benchStaleGraph(b, max(rows, 1))
	cursors, ok := store.(DriverCursorer)
	require.True(b, ok, "the waker's cursor path needs a store that can persist one")
	return &wakeCountingStore{Store: store, DriverCursorer: cursors}
}

// wakeCountingStore counts what one pass costs the store, split the way the
// waker's two kinds of query are paid for.
type wakeCountingStore struct {
	Store
	DriverCursorer
	reads        int
	cursorWrites int
}

func (s *wakeCountingStore) ObjectWritesListSinceAll(ctx context.Context, after int64, limit int) ([]ObjectWrite, int64, error) {
	s.reads++
	return s.Store.ObjectWritesListSinceAll(ctx, after, limit)
}

func (s *wakeCountingStore) EdgesGroupIncomingByID(ctx context.Context, ids []ObjectID, rel Relation) (map[ObjectID][]ObjectRef, error) {
	s.reads++
	return s.Store.EdgesGroupIncomingByID(ctx, ids, rel)
}

func (s *wakeCountingStore) DriverCursorsSet(ctx context.Context, name string, at int64) error {
	s.cursorWrites++
	return s.DriverCursorer.DriverCursorsSet(ctx, name, at)
}

// benchSpec returns a spec no row has held. A byte-identical write is a no-op
// that issues no version, which would leave the sweep nothing to find.
func benchSpec() []byte {
	return []byte(`{"Val":"` + strconv.FormatInt(nameSeq.Add(1), 10) + `"}`)
}

// benchStaleGraph builds a converged dependency graph: objects rows, each
// depending on the next benchStaleEdgesPerObject rows, every watermark caught
// up so the listing matches nothing.
//
// The whole build runs in one transaction. Per-write transactions dominate the
// setup otherwise, and the store has one connection to serialize them over.
func benchStaleGraph(b *testing.B, objects int) (Store, []ObjectID) {
	b.Helper()
	ctx := context.Background()

	store, err := sqlite.OpenMemory()
	require.NoError(b, err)
	b.Cleanup(func() { store.Close() })

	ids := make([]ObjectID, objects)
	err = store.Within(ctx, func(ctx context.Context) error {
		for i := range objects {
			obj, err := store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{
				Name: uniqueName(),
				Spec: []byte(`{"Val":"0"}`),
			})
			if err != nil {
				return err
			}
			ids[i] = obj.ID
		}
		for i, from := range ids {
			for n := 1; n <= benchStaleEdgesPerObject; n++ {
				to := ids[(i+n)%objects]
				if _, err := store.EdgesAdd(ctx, from, to, RelationDependsOn); err != nil {
					return err
				}
			}
		}
		return nil
	})
	require.NoError(b, err)

	// After the edges: EdgesAdd clears the watermark it finds, and the mark has
	// to cover the versions the creates issued.
	mark, err := store.ResourceVersionsMaxIssued(ctx)
	require.NoError(b, err)
	err = store.Within(ctx, func(ctx context.Context) error {
		for _, id := range ids {
			if err := store.DependencyWatermarksSet(ctx, id, mark); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(b, err)

	return store, ids
}
