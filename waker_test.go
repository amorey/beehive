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
	"strings"
	"sync/atomic"
	"testing"

	"github.com/amorey/beehive/sqlite"
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
// should be running concurrently with the assertions. It mirrors New's own
// type-assertion, so a store double opts into the durable-cursor path exactly by
// implementing DriverCursorer — nothing here has to say which.
func wakerOver(store Store, kinds ...GroupKind) (*waker, map[GroupKind]*reconciler) {
	rs := make(map[GroupKind]*reconciler, len(kinds))
	order := make([]*reconciler, 0, len(kinds))
	for _, gk := range kinds {
		r := &reconciler{gk: gk, work: newWorkQueue()}
		rs[gk] = r
		order = append(order, r)
	}
	bh := &Beehive{store: store, reconcilers: rs, order: order}
	cursors, _ := store.(DriverCursorer)
	bh.waker = &waker{bh: bh, cursors: cursors}
	return bh.waker, rs
}

// The seed is what keeps the first scan from replaying history. Without it the
// watermark starts at zero and the first tick walks every live row in the store,
// paying an edges lookup per page — the whole-world pass this design exists to
// avoid — to wake dependents for changes that happened before the process began,
// which the startup pass already covers.
//
// *replayStore alone implements no DriverCursorer, so this is also the
// no-capability fallback: it pins that a store which cannot persist a cursor
// seeds exactly as it always did.
func TestWakerSeedsFromTheWriteLogMax(t *testing.T) {
	store := &replayStore{seed: 500, rows: replayRows(3)}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})

	dw.seed(context.Background())
	require.True(t, dw.seeded)
	assert.EqualValues(t, 500, dw.watermark)

	dw.scan(context.Background())
	assert.Equal(t, []int64{500}, store.cursors(), "the first scan starts at the seed, not at zero")
}

// A store that implements DriverCursorer but has never persisted a cursor for
// this waker seeds the same way a store with no capability at all does: there is
// nothing stored to prefer over the write log's max.
func TestWakerSeedsFromMaxWithoutAStoredCursor(t *testing.T) {
	store := &cursorStore{replayStore: replayStore{seed: 500, rows: replayRows(3)}}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})

	dw.seed(context.Background())
	require.True(t, dw.seeded)
	assert.EqualValues(t, 500, dw.watermark, "no stored cursor: fall back to the write log's max")
}

// A run that seeds and then stops without a single change to scan must still
// leave the seed point behind. Without the row, its successor seeds from the
// mark as of *its* start, which sits above anything committed in between — so
// the change that landed while nothing was running is never scanned, which is
// precisely the gap this cursor exists to close, reopened for the whole first
// run of a fresh store.
func TestWakerPersistsTheSeedBeforeSeeingAnyWrite(t *testing.T) {
	widget := GroupKind{Kind: "Widget"}
	store := &cursorStore{replayStore: replayStore{seed: 10}}
	store.deps = map[ObjectID][]ObjectRef{1: {{ID: 7, Kind: "Widget"}}}

	first, _ := wakerOver(store, widget)
	require.True(t, first.seed(context.Background()))
	require.Equal(t, []int64{10}, store.setCalls, "the seed point is durable before any change arrives")

	// Target 1 changes with no waker running to see it.
	store.rows, store.seed = changedAt(20), 20

	second, rs := wakerOver(store, widget)
	require.True(t, second.seed(context.Background()))
	require.EqualValues(t, 10, second.watermark, "the restart resumes at the stored seed, not the new mark")

	second.scan(context.Background())
	assert.Equal(t, []ObjectID{7}, queuedIDs(rs[widget].work),
		"the dependent of a change made while nothing was running is woken on the first scan back")
}

// Zero is a position, not an absence: it is what an empty write log reports, and
// a run that seeds there and stops is the same story as the test above. The row
// has to be created for it, which is why persisted starts at noStoredCursor
// rather than at zero.
func TestWakerPersistsAZeroSeed(t *testing.T) {
	store := &cursorStore{replayStore: replayStore{seed: 0}}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})

	require.True(t, dw.seed(context.Background()))
	assert.Equal(t, []int64{0}, store.setCalls, "an empty store still records where it started")
}

// A restart resumes from the stored cursor rather than the write log's max, which
// is the entire point: the interval the process was down for is still scanned.
func TestWakerSeedsFromTheStoredCursor(t *testing.T) {
	store := &cursorStore{
		replayStore: replayStore{seed: 500, rows: replayRows(3)},
		stored:      map[string]int64{cursorNameWaker: 200},
	}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})

	dw.seed(context.Background())
	require.True(t, dw.seeded)
	assert.EqualValues(t, 200, dw.watermark, "the stored cursor precedes the write log's max, so it wins")

	dw.scan(context.Background())
	assert.Equal(t, []int64{200}, store.cursors(), "the first scan resumes at the stored cursor")
}

// ObjectWritesMaxVersion is a max over live rows, so deleting the
// highest-versioned object legitimately lowers it below a cursor the waker
// really did process. A stored cursor above the mark is therefore not evidence
// of a swapped or truncated database, and clamping to the mark rather than
// resetting to zero is what makes replaying that case free: the next listing
// asks for everything above the mark, which is empty by definition.
func TestWakerClampsAStoredCursorAboveTheMark(t *testing.T) {
	store := &cursorStore{
		replayStore: replayStore{seed: 90, rows: replayRows(3)},
		stored:      map[string]int64{cursorNameWaker: 100},
	}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})

	dw.seed(context.Background())
	require.True(t, dw.seeded)
	assert.EqualValues(t, 90, dw.watermark, "the mark clamps a stored cursor that overshoots it")

	dw.scan(context.Background())
	assert.Equal(t, []int64{90}, store.cursors(), "the scan asks for everything above the mark")
	assert.Zero(t, store.read, "which is empty by definition, so nothing is walked")
	assert.Zero(t, store.calls.Load(), "and no edges lookup is paid for")
}

// A clamp leaves the row above the watermark, and persisted has to track the
// row rather than the watermark or every tick between the two pays a round trip
// for a write DriverCursorsSet's own WHERE discards. Only once the watermark
// climbs past what the row already holds is there anything worth writing.
func TestWakerWritesNothingUntilItPassesAClampedRow(t *testing.T) {
	store := &cursorStore{
		replayStore: replayStore{seed: 90, rows: changedAt(95)},
		stored:      map[string]int64{cursorNameWaker: 100},
	}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	require.True(t, dw.seed(context.Background()))
	require.EqualValues(t, 90, dw.watermark)

	dw.scan(context.Background())
	require.EqualValues(t, 95, dw.watermark, "the scan advanced past the mark but not past the row")
	assert.Empty(t, store.setCalls, "the row already holds 100, so the store is not asked to store 95")

	store.rows = changedAt(95, 105)
	dw.scan(context.Background())
	require.EqualValues(t, 105, dw.watermark)
	assert.Equal(t, []int64{105}, store.setCalls, "past the row, the write is worth making")
}

// A seed that fails leaves the waker unseeded, and the next tick seeds instead of
// scanning. Scanning would be the harmful choice: an unseeded watermark is zero,
// so it would replay the whole table on the strength of a transient error. This
// covers both reads seed makes: the write log's max and, when the store persists
// one, the stored cursor.
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

// The same retry contract, for a failure reading the stored cursor rather than
// the write log's max: nothing here can tell whether the stored value would have
// mattered, so the safe answer is to try both reads again next tick.
func TestWakerRetriesSeedOnAFailedCursorRead(t *testing.T) {
	store := &cursorStore{replayStore: replayStore{seed: 500}, getErr: errBoom}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	ctx := context.Background()

	dw.seed(ctx)
	require.False(t, dw.seeded)

	store.getErr = nil
	dw.seed(ctx)
	require.True(t, dw.seeded)
	assert.EqualValues(t, 500, dw.watermark)
}

// A cursor read that fails because stop cancelled the ctx is shutdown, not an
// outage — the same treatment the write-log read beside it already gets, and the
// rest of the waker gives a cancelled ctx.
func TestWakerCursorReadFailureDuringShutdownIsQuiet(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	store := &cursorStore{replayStore: replayStore{seed: 500}, getErr: errBoom}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.bh.logger = logger

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.False(t, dw.seed(ctx), "a cancelled read still leaves the waker unseeded")
	assert.False(t, dw.seeded)
	assert.Empty(t, buf.String(), "shutdown is not an outage to report")
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

// A scan that advances the watermark persists it once, at the value the scan
// reached — not per page, and not the watermark it started from.
func TestWakerPersistsOnceWhenTheCursorMoves(t *testing.T) {
	store := &cursorStore{replayStore: replayStore{rows: replayRows(3)}}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.seeded = true

	dw.scan(context.Background())

	assert.Equal(t, []int64{3}, store.setCalls, "persisted once, at the watermark the scan reached")
	assert.EqualValues(t, 3, dw.persisted)
}

// A tick that finds nothing above the watermark writes nothing. The guard is on
// the in-memory watermark rather than a hopeful call the store's own upsert
// would just discard, so a quiet store costs this driver no round trip at all.
func TestWakerSkipsTheWriteWhenQuiet(t *testing.T) {
	store := &cursorStore{replayStore: replayStore{rows: replayRows(3)}}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.seeded, dw.watermark, dw.persisted = true, 3, 3

	dw.scan(context.Background())

	assert.Empty(t, store.setCalls, "nothing above the watermark, so nothing to persist")
}

// stop cancels the waker's ctx. A scan that reached ctx cancellation must not
// persist or report anything on the way out — the rest of the waker already
// treats a cancelled ctx as shutdown rather than a loss, and this has to match
// it rather than log a warning on every clean stop that overlapped a scan.
func TestWakerSkipsTheWriteOnShutdown(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	store := &cursorStore{replayStore: replayStore{rows: replayRows(3)}}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.bh.logger = logger
	dw.seeded = true

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dw.scan(ctx)

	assert.Empty(t, store.setCalls, "a scan caught by shutdown persists nothing")
	assert.Empty(t, buf.String(), "and logs nothing: shutdown is not an outage")
}

// The defer that persists the cursor is what makes a mid-scan failure keep the
// pages that already succeeded: an end-of-loop write would never run on this
// path at all, since every exit from the paging loop is a return.
func TestWakerPersistsProgressOnAFailedPage(t *testing.T) {
	store := &cursorStore{replayStore: replayStore{
		rows: replayRows(wakeScanPageCap + 5), err: errBoom, failFromCall: 2,
	}}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.seeded = true

	dw.scan(context.Background())

	require.EqualValues(t, wakeScanPageCap, dw.watermark, "the first page succeeded before the second failed")
	assert.Equal(t, []int64{wakeScanPageCap}, store.setCalls, "and that progress is what gets persisted")
}

// The whole point, proven end to end: a dependent whose target changed while
// the process was down is still woken on the very first scan back, because the
// new process resumes from the stored cursor rather than reseeding at the
// write log's current max — which would place the watermark past that change
// and never scan it at all.
func TestWakerResumesFromTheStoredCursor(t *testing.T) {
	widget := GroupKind{Kind: "Widget"}
	store := &cursorStore{replayStore: replayStore{seed: 10, rows: changedAt(10)}}
	store.deps = map[ObjectID][]ObjectRef{1: {{ID: 7, Kind: "Widget"}}}

	first, rsFirst := wakerOver(store, widget)
	require.True(t, first.seed(context.Background()))
	// A write lands while the first process is up; its scan finds and persists it.
	store.rows = append(store.rows, ObjectWrite{ID: 2, ResourceVersion: 20})
	store.deps[2] = []ObjectRef{{ID: 8, Kind: "Widget"}}
	store.seed = 20
	first.scan(context.Background())
	require.Equal(t, []ObjectID{8}, queuedIDs(rsFirst[widget].work))
	require.Equal(t, []int64{10, 20}, store.setCalls,
		"the seed point first, then the progress the scan made, both before the process goes away")

	// The process is gone; a second write lands while nothing is running to see it.
	store.rows = append(store.rows, ObjectWrite{ID: 3, ResourceVersion: 30})
	store.deps[3] = []ObjectRef{{ID: 9, Kind: "Widget"}}
	store.seed = 30

	second, rsSecond := wakerOver(store, widget)
	require.True(t, second.seed(context.Background()))
	assert.EqualValues(t, 20, second.watermark, "seeded from the stored cursor, not the write log's new max")

	second.scan(context.Background())
	assert.Equal(t, []ObjectID{9}, queuedIDs(rsSecond[widget].work),
		"the dependent of the write made while the process was down is woken on the first scan back")
}

// One tick reads at most wakeScanPagesPerTick pages, so a long backlog cannot
// monopolise the single connection the reconcile loops need too. The remainder
// is not lost: the cursor persists at whatever this tick reached, and the next
// tick resumes there rather than re-reading it.
func TestWakerStopsAtThePageBudget(t *testing.T) {
	total := wakeScanPagesPerTick*wakeScanPageCap + 5
	store := &cursorStore{replayStore: replayStore{rows: replayRows(total)}}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.seeded = true

	dw.scan(context.Background())
	assert.Len(t, store.pages, wakeScanPagesPerTick, "the tick stops at the page budget")
	assert.EqualValues(t, wakeScanPagesPerTick*wakeScanPageCap, dw.watermark)
	assert.Equal(t, []int64{wakeScanPagesPerTick * wakeScanPageCap}, store.setCalls,
		"progress within the budget is still persisted")

	dw.scan(context.Background())
	assert.EqualValues(t, total, dw.watermark, "the next tick resumes at the budget, not from the start")
}

// However far behind a stored cursor is, seed resumes from it: the distance is
// in resource_version units, which EventsAdd inflates without adding anything
// this scan would read, so no threshold over it could say whether the gap is
// worth draining. wakeScanPagesPerTick is what bounds the cost instead, per
// tick, whatever the gap holds.
func TestWakerResumesAnEnormousBacklog(t *testing.T) {
	const mark = 50_000_000
	store := &cursorStore{
		replayStore: replayStore{seed: mark, rows: changedAt(10)},
		stored:      map[string]int64{cursorNameWaker: 0},
	}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})

	require.True(t, dw.seed(context.Background()))
	assert.Zero(t, dw.watermark, "the cursor is resumed, however far behind the mark it sits")

	dw.scan(context.Background())
	assert.Equal(t, []int64{0}, store.cursors(), "so the scan starts where the last run stopped")
	assert.Equal(t, 1, store.read, "and the change above it is found rather than skipped")
}

// resumeWatermark's cases, exercised directly rather than through seed and a
// store double: no stored cursor, one at or past the mark, and one below it.
func TestResumeWatermark(t *testing.T) {
	cases := []struct {
		name   string
		stored int64
		ok     bool
		mark   int64
		want   int64
	}{
		{"no stored cursor", 0, false, 500, 500},
		{"stored cursor at the mark", 500, true, 500, 500},
		{"stored cursor past the mark: clamp", 600, true, 500, 500},
		{"stored cursor below the mark: resume", 400, true, 500, 400},
		{"stored cursor far below the mark: still resume", 1, true, 50_000_000, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, resumeWatermark(c.stored, c.ok, c.mark))
		})
	}
}

// The retry ladder, straight rather than through 60-odd scan ticks: immediate
// after the first failure, doubling, then flat at the cap — including for a
// streak long enough that the shift itself would overflow if it were taken.
func TestWakePersistRetrySkips(t *testing.T) {
	assert.Equal(t, 0, wakePersistRetrySkips(1), "the first retry is immediate")
	assert.Equal(t, 1, wakePersistRetrySkips(2))
	assert.Equal(t, 3, wakePersistRetrySkips(3))
	assert.Equal(t, 31, wakePersistRetrySkips(6))
	assert.Equal(t, wakePersistRetryCap, wakePersistRetrySkips(7), "the ladder reaches the cap")
	assert.Equal(t, wakePersistRetryCap, wakePersistRetrySkips(8), "and stays there")
	assert.Equal(t, wakePersistRetryCap, wakePersistRetrySkips(1000), "however long the streak runs")
}

// A failed persist write is logged and leaves dw.persisted at its old value, so
// the next tick's guard (watermark > persisted) still holds and the write is
// retried — even on a tick that finds nothing new to scan, since it is
// persisted's staleness against watermark that drives the retry, not fresh
// pages.
func TestWakerRetriesPersistOnAFailedWrite(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	store := &cursorStore{replayStore: replayStore{rows: replayRows(3)}, setErr: errBoom}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.bh.logger = logger
	dw.seeded = true

	dw.scan(context.Background())
	assert.EqualValues(t, 3, dw.watermark, "the in-memory watermark still advances; only the durable write failed")
	assert.Empty(t, store.setCalls, "the failed write leaves no record of succeeding")
	assert.Zero(t, dw.persisted, "so persisted stays at its old baseline")
	assert.NotEmpty(t, buf.String(), "the failure is logged")

	store.setErr = nil
	dw.scan(context.Background()) // no rows above watermark=3, but persisted still lags it
	assert.Equal(t, []int64{3}, store.setCalls, "the next tick retries the write even though this scan found nothing new")
}

// A write that fails forever — a read-only or full database — must not become a
// doomed round trip and a warning every second. Holding persisted is what makes
// the retry happen at all, so nothing here stops retrying; the streak is what
// paces it, and what keeps the log to one line about one cause.
func TestWakerBacksOffAFailingPersist(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	store := &cursorStore{replayStore: replayStore{rows: replayRows(3)}, setErr: errBoom}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.bh.logger = logger
	dw.seeded = true

	const ticks = 30
	for range ticks {
		dw.scan(context.Background())
	}

	assert.Equal(t, 1, strings.Count(buf.String(), "persisting the dependency waker's cursor failed"),
		"one warning for the streak, not one per tick")
	assert.Less(t, store.setAttempts, ticks/2,
		"and the retries back off rather than paying a round trip every tick")
	assert.Positive(t, store.setAttempts, "without giving up on the write entirely")

	// Recovery closes the streak, so a later failure is a fresh warning rather
	// than silence inherited from this one.
	store.setErr = nil
	for range ticks {
		dw.scan(context.Background())
	}
	assert.Equal(t, []int64{3}, store.setCalls, "the write lands once the store accepts it again")
	assert.Zero(t, dw.persistFailures, "and the streak is closed")
}

// Every waker test above drives a double, so all of them would stay green if the
// real store stopped satisfying DriverCursorer — New's type assertion discards
// its failure, leaving cursors nil and the waker silently back on
// reseed-from-max. The static assertions in sqlite/store.go are the primary
// guard; this pins the other half, that New actually hands the capability to the
// waker rather than dropping it somewhere in between.
func TestNewGivesTheWakerTheStoresCursorCapability(t *testing.T) {
	store, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

	bh, err := New(store)
	require.NoError(t, err)
	require.NotNil(t, bh.waker.cursors, "the sqlite store persists cursors, so the waker must have them")

	// And the wiring carries all the way through a real seed and back.
	require.True(t, bh.waker.seed(context.Background()))
	cursor, ok, err := store.DriverCursorsGet(context.Background(), cursorNameWaker)
	require.NoError(t, err)
	require.True(t, ok, "the seed point reached the database")
	assert.Equal(t, bh.waker.watermark, cursor)
}

// settlingCapture reports every object it reconciles and settles it, so a later
// pass cannot be attributed to the owed-work listing: a settled object is
// invisible to it.
type settlingCapture struct{ ch chan ObjectID }

func (c *settlingCapture) Reconcile(ctx context.Context, cc ControllerClient[cStatus], obj *Object[cSpec, cStatus]) (Result, error) {
	if err := cc.UpdateStatus(ctx, obj.ID, obj.Generation, cStatus{}); err != nil {
		return Result{}, err
	}
	c.ch <- obj.ID
	return Result{}, nil
}

// TestStaleDependentsPassEnqueuesStaleDependents is the driver end to end, with
// every other route to the dependent closed: the waker is off, the full pass is
// off, and the dependent is settled — so no owed-work listing can name it. What
// is left is re-derivation from the watermark.
func TestStaleDependentsPassEnqueuesStaleDependents(t *testing.T) {
	ctx := context.Background()
	probe := &listProbeStore{
		Store:        newClientTestStore(t),
		watermarkSet: make(chan struct{}, 1),
	}
	// The waker off, so only re-derivation can reach the dependent.
	bh, err := New(probe, fast(withDependencyWakeInterval(0))...)
	require.NoError(t, err)

	reconciled := make(chan ObjectID, 8)
	_, err = Register(bh, clientTestGK, &settlingCapture{ch: reconciled}, WithFullPassInterval(0))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	target := mustCreate(t, ctx, client, uniqueSlug(), cSpec{Val: "a"})
	dep := mustCreate(t, ctx, client, uniqueSlug(), cSpec{Val: "b"})
	require.NoError(t, addEdge(ctx, probe.Store, dep.ID, target.ID, RelationDependsOn))

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { stop(ctx) })

	// Wait for the dependent's own watermark write, not just its pass: the write
	// lands after Reconcile returns, and a target change that slipped under it would
	// be recorded as observed.
	waitClosed(t, probe.watermarkSet, "the dependent's watermark write")
	drainProbe(reconciled)

	_, err = client.UpdateByID(ctx, target.ID, cSpec{Val: "moved"})
	require.NoError(t, err)

	awaitMatch(t, reconciled, func(id ObjectID) bool { return id == dep.ID },
		"the stale pass to re-derive a wake nothing recorded")
}

// TestStaleDependentsPassIgnoresUnregisteredKinds pins the filter the scan is
// asked for: only kinds with a reconcile loop, since a client-only dependent is
// stale forever and would otherwise be re-scanned on every pass for the life of
// the row. A kind registered in a later build joins the list and is found on the
// next pass, so nothing is stranded by the exclusion.
func TestStaleDependentsPassIgnoresUnregisteredKinds(t *testing.T) {
	ctx := context.Background()
	probe := &listProbeStore{
		Store:       newClientTestStore(t),
		staleListed: make(chan struct{}, 1),
	}
	bh, err := New(probe, fast()...)
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { stop(ctx) })
	waitClosed(t, probe.staleListed, "the stale-dependents listing")

	asked := probe.kindsAsked()
	require.NotEmpty(t, asked)
	assert.Equal(t, []GroupKind{clientTestGK}, asked[0],
		"the scan is asked only for kinds that have somewhere to enqueue")
}

// staleListErrorStore fails the staleness listing, for the sweep's failure arm.
type staleListErrorStore struct {
	fakeStore
	calls atomic.Int64
}

func (s *staleListErrorStore) DependentsListStale(context.Context, []GroupKind, ObjectID, int) ([]ObjectRef, error) {
	s.calls.Add(1)
	return nil, errBoom
}

// TestStaleDependentsSweepWarnsAndRetriesOnListFailure pins the failure contract:
// a sweep that cannot read gives up on this pass and says so. There is no cursor
// to hold and nothing was drained — the listing derives its answer from current
// state — so the next tick re-derives the same set, which is why abandoning the
// sweep is the whole of the repair.
func TestStaleDependentsSweepWarnsAndRetriesOnListFailure(t *testing.T) {
	ctx := context.Background()
	store := &staleListErrorStore{}
	logger, logs := captureLogger(slog.LevelWarn)
	bh := &Beehive{store: store, logger: logger}
	kinds := []GroupKind{clientTestGK}

	bh.staleDependentsSweep(ctx, kinds)
	require.EqualValues(t, 1, store.calls.Load(), "a failed page abandons the sweep")
	assert.Contains(t, logs.String(), "listing stale dependents failed")

	bh.staleDependentsSweep(ctx, kinds)
	assert.EqualValues(t, 2, store.calls.Load(), "and the next pass asks again from the start")
}

// TestStaleDependentsSweepIsQuietOnShutdown separates the two reasons a listing
// fails. Stop cancels the same ctx the sweep reads on, so a pass in flight when
// the control plane goes down fails for no reason of its own — warning there
// would report a fault on every clean shutdown.
func TestStaleDependentsSweepIsQuietOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	logger, logs := captureLogger(slog.LevelWarn)
	bh := &Beehive{store: &staleListErrorStore{}, logger: logger}

	bh.staleDependentsSweep(ctx, []GroupKind{clientTestGK})

	assert.Empty(t, logs.String(), "a cancelled read is shutdown, not a lost pass")
}
