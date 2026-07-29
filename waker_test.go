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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStartWithNoControllersSkipsWaker verifies a Beehive with nothing
// registered never scans the write log. There is nothing to wake — every
// dependent would find no reconciler to queue onto — and the scan is not
// free: it costs an edges query per change in the whole store, on the single
// connection every writer shares.
func TestStartWithNoControllersSkipsWaker(t *testing.T) {
	store := &replayStore{rows: replayRows(1)}
	bh, err := New(store, withDependencyWakeInterval(fastTick))
	require.NoError(t, err)

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	// Stop drains the waker goroutine, so its return is the proof the scan loop is
	// over — no bounded wait needed to assert the negative.
	require.NoError(t, stop(context.Background()))

	assert.Empty(t, store.pages, "no controllers, no scan")
}

// wakerOver builds a waker over a scripted write log, plus reconcilers for the
// given kinds so a wake has somewhere to land. The Beehive is assembled by hand
// rather than through New: these tests drive seed and scan directly, so nothing
// should be running concurrently with the assertions.
func wakerOver(store *replayStore, kinds ...GroupKind) (*waker, map[GroupKind]*reconciler) {
	rs := make(map[GroupKind]*reconciler, len(kinds))
	order := make([]*reconciler, 0, len(kinds))
	for _, gk := range kinds {
		r := &reconciler{gk: gk, work: newWorkQueue()}
		rs[gk] = r
		order = append(order, r)
	}
	bh := &Beehive{store: store, reconcilers: rs, order: order}
	bh.waker = &waker{bh: bh}
	return bh.waker, rs
}

// The seed is what keeps the first scan from replaying history. Without it the
// watermark starts at zero and the first tick walks every live row in the store,
// paying an edges lookup per page — the whole-world pass this design exists to
// avoid — to wake dependents for changes that happened before the process began,
// which the startup pass already covers.
func TestWakerSeedsFromTheStoreCursor(t *testing.T) {
	store := &replayStore{seed: 500, rows: replayRows(3)}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})

	dw.seed(context.Background())
	require.True(t, dw.seeded)
	assert.EqualValues(t, 500, dw.watermark)

	dw.scan(context.Background())
	assert.Equal(t, []int64{500}, store.cursors(), "the first scan starts at the seed, not at zero")
}

// A seed that fails leaves the waker unseeded, and the next tick seeds instead of
// scanning. Scanning would be the harmful choice: an unseeded watermark is zero,
// so it would replay the whole table on the strength of a transient error.
func TestWakerRetriesSeedOnTheNextTick(t *testing.T) {
	store := &replayStore{seed: 500, seedErr: errBoom}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	ctx := context.Background()

	dw.seed(ctx)
	require.False(t, dw.seeded)

	dw.scan(ctx) // still unseeded: this tick seeds rather than scans
	assert.Empty(t, store.pages, "an unseeded waker must not scan from zero")

	store.seedErr = nil
	dw.scan(ctx)
	require.True(t, dw.seeded)
	assert.EqualValues(t, 500, dw.watermark)
}

// The scan's whole job: every row above the watermark has its dependents
// requeued, each on its own kind's reconciler.
func TestWakerScanWakesDependentsByTheirOwnKind(t *testing.T) {
	widget := GroupKind{Kind: "Widget"}
	gadget := GroupKind{Kind: "Gadget"}
	store := &replayStore{rows: changedAt(10)} // one row, id 1, version 10
	store.deps = map[ObjectID][]ObjectRef{
		1: {{ID: 7, Kind: "Widget"}, {ID: 8, Kind: "Gadget"}},
	}
	dw, rs := wakerOver(store, widget, gadget)
	dw.seeded = true

	dw.scan(context.Background())

	assert.Equal(t, []ObjectID{7}, queuedIDs(rs[widget].work))
	assert.Equal(t, []ObjectID{8}, queuedIDs(rs[gadget].work), "routed by the dependent's kind, not the target's")
	assert.EqualValues(t, 10, dw.watermark, "the watermark advances past what it processed")
}

// A dependent whose kind has no registered controller is dropped rather than
// misrouted: a depends_on edge may point at (or come from) a client-only kind,
// and there is no reconcile loop to enqueue onto.
func TestWakerSkipsUnregisteredKinds(t *testing.T) {
	widget := GroupKind{Kind: "Widget"}
	store := &replayStore{rows: changedAt(10)}
	store.deps = map[ObjectID][]ObjectRef{1: {{ID: 7, Kind: "Widget"}, {ID: 9, Kind: "Config"}}}
	dw, rs := wakerOver(store, widget)
	dw.seeded = true

	dw.scan(context.Background())

	assert.Equal(t, []ObjectID{7}, queuedIDs(rs[widget].work))
}

// A self-edge is not a wake. A spec write already leaves the object unsettled and
// so owed a pass, and a status or condition write is the object's own pass, which
// has just run — so waking it here re-enqueues at full speed with nothing to
// converge it.
func TestWakerSkipsTheSelfEdge(t *testing.T) {
	widget := GroupKind{Kind: "Widget"}
	store := &replayStore{rows: changedAt(10)}
	store.deps = map[ObjectID][]ObjectRef{1: {{ID: 1, Kind: "Widget"}, {ID: 7, Kind: "Widget"}}}
	dw, rs := wakerOver(store, widget)
	dw.seeded = true

	dw.scan(context.Background())

	assert.Equal(t, []ObjectID{7}, queuedIDs(rs[widget].work), "the self-edge is skipped, its sibling is not")
}

// A whole scan resolves in one edges query per page, not one per changed object.
// The store runs on a single connection, so every lookup the waker makes
// serializes against every writer in the process — and it sees every change in
// the store, not just the registered kinds'.
func TestWakerResolvesAPageInOneQuery(t *testing.T) {
	widget := GroupKind{Kind: "Widget"}
	store := &replayStore{rows: changedAt(10, 11, 12)}
	dw, _ := wakerOver(store, widget)
	dw.seeded = true

	dw.scan(context.Background())

	assert.EqualValues(t, 1, store.calls.Load(), "three changed targets, one lookup")
}

// The watermark advances per page, which is sound only because pages come back in
// resource_version order: a page that succeeded really does mean everything below
// it is done. Each page therefore resumes where the last one ended, so the cost of
// a scan is what changed rather than what exists.
func TestWakerPagesTheScan(t *testing.T) {
	store := &replayStore{rows: replayRows(wakeScanPageCap + 5)}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.seeded = true

	dw.scan(context.Background())

	assert.Equal(t, []int64{0, wakeScanPageCap}, store.cursors(), "the second page resumes where the first ended")
	assert.EqualValues(t, wakeScanPageCap+5, dw.watermark)
	assert.Equal(t, wakeScanPageCap+5, store.read, "every row above the watermark was processed exactly once")
}

// A scan that stops on a short page saves the empty query that would otherwise
// end every one. Nothing is lost: a row committed since is above the watermark
// and belongs to the next tick.
func TestWakerStopsOnAShortPage(t *testing.T) {
	store := &replayStore{rows: replayRows(3)}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.seeded = true

	dw.scan(context.Background())

	assert.Len(t, store.pages, 1, "a short page ends the scan")
}

// A failed listing holds the watermark, so the next tick re-reads exactly what is
// still owed. Advancing anyway would strand every dependent of those rows: a
// settled dependent is invisible to every owed-work listing, because its own
// generation never moved, so nothing else would ever find it.
func TestWakerHoldsTheWatermarkOnScanFailure(t *testing.T) {
	store := &replayStore{rows: replayRows(3), err: errBoom}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.seeded = true
	dw.watermark = 100
	ctx := context.Background()

	dw.scan(ctx)
	assert.EqualValues(t, 100, dw.watermark, "a failed listing advances nothing")

	dw.scan(ctx)
	assert.Equal(t, []int64{100, 100}, store.cursors(), "the next tick asks for the same range again")
}

// The same for a failed dependents lookup, which is the harder half: the rows
// were read, so it is tempting to count them as processed. Nothing here can name
// who was missed — the lookup that failed is exactly the one that would have said
// — so holding the cursor and re-reading the changes is the only repair.
func TestWakerHoldsTheWatermarkOnLookupFailure(t *testing.T) {
	store := &replayStore{rows: changedAt(10, 11)}
	store.err = nil
	store.depsStore.err = errBoom
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.seeded = true
	ctx := context.Background()

	dw.scan(ctx)
	assert.Zero(t, dw.watermark, "the rows were read but not serviced, so they are still owed")

	dw.scan(ctx)
	assert.Equal(t, []int64{0, 0}, store.cursors())
}

// A cancelled context is shutdown, not a loss, so it stops the scan quietly
// rather than logging an outage on the way out.
func TestWakerScanStopsQuietlyOnShutdown(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	store := &replayStore{rows: replayRows(3), err: errBoom}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.bh.logger = logger
	dw.seeded = true

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dw.scan(ctx)

	assert.Empty(t, buf.String(), "shutdown is not an outage to report")
}

// One reconcile pass usually writes an object several times — status, then
// conditions, then the generation handshake — so the same id arrives on a page at
// several versions. Resolving it once per version would walk its dependents once per
// write, for a page that says nothing new after the first row.
func TestWakerResolvesEachTargetOnce(t *testing.T) {
	// Three versions of object 1, one of object 2, interleaved as the write log would
	// hold them.
	store := &replayStore{rows: []ObjectWrite{
		{ID: 1, ResourceVersion: 10},
		{ID: 2, ResourceVersion: 11},
		{ID: 1, ResourceVersion: 12},
		{ID: 1, ResourceVersion: 13},
	}}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.seeded = true

	dw.scan(context.Background())

	require.Len(t, store.seen, 1, "one page, one lookup")
	assert.Equal(t, []ObjectID{1, 2}, store.seen[0], "each target is asked for once")
	assert.EqualValues(t, 13, dw.watermark, "the watermark still advances past the whole page")
}

// A failed dependents lookup during shutdown is not an outage. The waker's ctx is
// the control plane's, so a page in flight when Stop lands fails for a reason that
// has nothing to do with the store — reporting it would put a warning in every
// clean shutdown that happened to overlap a scan.
func TestWakerLookupFailureDuringShutdownIsQuiet(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	store := &replayStore{rows: changedAt(10, 11)}
	store.depsStore.err = errBoom // the rows read fine; resolving their dependents does not
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.bh.logger = logger
	dw.seeded = true

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dw.scan(ctx)

	assert.Zero(t, dw.watermark, "the changes are still owed, cancelled or not")
	assert.Empty(t, buf.String(), "shutdown is not a lookup outage to report")
}

// A non-positive interval disables the waker outright. It is a supported choice —
// the reconcile_owed stamp still covers a dependency declared against a target
// that moved — so run must simply return rather than panic in NewTicker.
func TestWakerDisabledByNonPositiveInterval(t *testing.T) {
	store := &replayStore{rows: replayRows(3)}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.bh.wakeInterval = 0

	dw.run(context.Background()) // returns immediately; a running waker would block

	assert.Empty(t, store.pages, "a disabled waker never scans")
}
