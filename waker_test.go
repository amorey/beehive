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
	"slices"
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
	bh := newTestBeehive(t, store, withDependencyWakeInterval(fastTick))

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

// Retention trims the write log's tail, so the mark can legitimately sit below a
// cursor the waker really did process. A stored cursor above the mark is
// therefore not evidence of a swapped or truncated database, and clamping to the
// mark rather than resetting to zero is what makes replaying that case free: the
// wakes it re-derives are idempotent.
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

	bh := newTestBeehive(t, store)
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
	bh := newTestBeehive(t, probe, fast(withDependencyWakeInterval(0))...)

	reconciled := make(chan ObjectID, 8)
	_, err := Register(bh, clientTestGK, &settlingCapture{ch: reconciled}, WithFullPassInterval(0))
	require.NoError(t, err)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	target := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	dep := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "b"})
	require.NoError(t, addEdge(ctx, probe.Store, dep.ID, target.ID, RelationDependsOn))

	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { stop(ctx) })

	// Wait for the dependent's own watermark write, not just its pass: the write
	// lands after Reconcile returns, and a target change that slipped under it would
	// be recorded as observed.
	waitClosed(t, probe.watermarkSet, "the dependent's watermark write")
	drainProbe(reconciled)

	_, err = client.Update(ctx, target.ID, cSpec{Val: "moved"})
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
	bh := newTestBeehive(t, probe, fast()...)
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	// One object, so a version has been issued: the sweep skips a store where
	// nothing has ever been written, and would never reach the listing.
	_, err = probe.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: []byte(`{}`)})
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
// It is a DriverCursorer so a test can watch the cursor it must not move.
type staleListErrorStore struct {
	fakeStore
	calls    atomic.Int64
	issued   int64
	setCalls []int64
}

func (s *staleListErrorStore) ResourceVersionsMaxIssued(context.Context) (int64, error) {
	return s.issued, nil
}

func (s *staleListErrorStore) DriverCursorsGet(context.Context, string) (int64, bool, error) {
	return 0, false, nil
}

func (s *staleListErrorStore) DriverCursorsReset(context.Context, string, int64) error {
	return nil
}

func (s *staleListErrorStore) DriverCursorsSet(_ context.Context, _ string, cursor int64) error {
	s.setCalls = append(s.setCalls, cursor)
	return nil
}

func (s *staleListErrorStore) DependentsListStaleSince(_ context.Context, _ []GroupKind, after StalePos, _ int64, _ int) ([]ObjectRef, StalePos, error) {
	s.calls.Add(1)
	return nil, after, errBoom
}

// staleSweepStore serves the cursor-form listing one page at a time and records
// what the sweep asked for, stamped, and persisted.
type staleSweepStore struct {
	fakeStore
	issued     int64
	issuedErr  error
	stampErr   error
	resetErr   error
	pages      [][]ObjectRef
	asked      []StalePos
	throughs   []int64
	stamped    [][]ObjectRef
	stored     map[string]int64
	setCalls   []int64
	resetCalls []int64
}

func (s *staleSweepStore) ResourceVersionsMaxIssued(context.Context) (int64, error) {
	return s.issued, s.issuedErr
}

func (s *staleSweepStore) DependentsListStaleSince(_ context.Context, _ []GroupKind, after StalePos, through int64, _ int) ([]ObjectRef, StalePos, error) {
	s.asked = append(s.asked, after)
	s.throughs = append(s.throughs, through)
	if len(s.pages) == 0 {
		return nil, after, nil
	}
	page := s.pages[0]
	s.pages = s.pages[1:]
	return page, StalePos{TargetVersion: after.TargetVersion + 1}, nil
}

func (s *staleSweepStore) ReconcileOwedStamp(_ context.Context, refs []ObjectRef) error {
	if s.stampErr != nil {
		return s.stampErr
	}
	s.stamped = append(s.stamped, slices.Clone(refs))
	return nil
}

func (s *staleSweepStore) DriverCursorsGet(_ context.Context, name string) (int64, bool, error) {
	v, ok := s.stored[name]
	return v, ok, nil
}

func (s *staleSweepStore) DriverCursorsReset(_ context.Context, name string, cursor int64) error {
	if s.resetErr != nil {
		return s.resetErr
	}
	s.resetCalls = append(s.resetCalls, cursor)
	if s.stored == nil {
		s.stored = map[string]int64{}
	}
	s.stored[name] = cursor
	return nil
}

func (s *staleSweepStore) DriverCursorsSet(_ context.Context, name string, cursor int64) error {
	s.setCalls = append(s.setCalls, cursor)
	if s.stored == nil {
		s.stored = map[string]int64{}
	}
	if stored, ok := s.stored[name]; !ok || cursor > stored {
		s.stored[name] = cursor
	}
	return nil
}

// sweeperOver builds a stale-dependents sweeper over a store double, mirroring
// what staleDependentsRun assembles.
func sweeperOver(store Store) *staleDependents {
	return sweeperOverKinds(store, clientTestGK)
}

// sweeperOverKinds is sweeperOver for an explicit kind set, so a test can change
// it between processes.
func sweeperOverKinds(store Store, kinds ...GroupKind) *staleDependents {
	bh := &Beehive{store: store, logger: slog.New(slog.DiscardHandler)}
	cursors, _ := store.(DriverCursorer)
	resets, _ := store.(DriverCursorResetter)
	return &staleDependents{
		bh:         bh,
		kinds:      kinds,
		cursors:    cursors,
		resets:     resets,
		cursorName: staleDependentsCursorName(kinds),
	}
}

// TestStaleDependentsSweepStampsWhatItFinds pins the durable half of a finding.
// The enqueue lives in memory and a restart loses it; the stamp is what the owed
// pass drains afterwards. One call per page, not per row.
func TestStaleDependentsSweepStampsWhatItFinds(t *testing.T) {
	page := []ObjectRef{
		{ID: 1, Group: clientTestGK.Group, Kind: clientTestGK.Kind},
		{ID: 2, Group: clientTestGK.Group, Kind: clientTestGK.Kind},
	}
	sd := sweeperOver(&staleSweepStore{issued: 500, pages: [][]ObjectRef{page}})

	sd.sweep(context.Background())

	assert.Equal(t, [][]ObjectRef{page}, sd.bh.store.(*staleSweepStore).stamped)
}

// TestStaleDependentsSweepResumesAndRecordsThePreScanMark pins both ends of the
// cursor. It resumes where the last completed sweep stopped, and it records the
// mark read *before* this scan — a target written while the sweep runs sits above
// that mark, so the next sweep still finds it.
func TestStaleDependentsSweepResumesAndRecordsThePreScanMark(t *testing.T) {
	store := &staleSweepStore{issued: 500, stored: map[string]int64{staleCursorName(clientTestGK): 200}}
	sd := sweeperOver(store)
	ctx := context.Background()

	sd.resume(ctx)
	require.EqualValues(t, 200, sd.cursor, "a stored cursor is where the next sweep starts")

	sd.sweep(ctx)

	assert.Equal(t, []StalePos{{TargetVersion: 201}}, store.asked, "above the version the last sweep consumed")
	assert.Equal(t, []int64{500}, store.throughs, "and bounds the scan at that same mark")
	assert.EqualValues(t, 500, sd.cursor, "the sweep advances to the mark it read first")
	assert.Equal(t, []int64{500}, store.setCalls, "and persists it once")
}

// TestStaleDependentsSweepSkipsAQuietStore: no version issued since the last
// sweep means no target moved, so no row can be stale. The listing and the
// cursor write are both skipped, leaving one read per tick.
func TestStaleDependentsSweepSkipsAQuietStore(t *testing.T) {
	store := &staleSweepStore{issued: 500, stored: map[string]int64{staleCursorName(clientTestGK): 500}}
	sd := sweeperOver(store)
	ctx := context.Background()
	sd.resume(ctx)

	sd.sweep(ctx)

	assert.Empty(t, store.asked, "nothing has moved, so nothing is listed")
	assert.Empty(t, store.setCalls, "and the cursor row is left alone")
}

// TestStaleDependentsSweepRederivesFromAForeignCursor: a cursor above the store's
// own sequence came from another database. Scanning above it would find nothing
// for good, which would silently disable the one pass that cannot be disabled.
func TestStaleDependentsSweepRederivesFromAForeignCursor(t *testing.T) {
	store := &staleSweepStore{issued: 500, stored: map[string]int64{staleCursorName(clientTestGK): 9000}}
	sd := sweeperOver(store)
	ctx := context.Background()
	sd.resume(ctx)

	sd.sweep(ctx)

	assert.Equal(t, []StalePos{{TargetVersion: 1}}, store.asked, "the scan restarts from the beginning")
	assert.EqualValues(t, 500, sd.cursor)
	assert.Equal(t, []int64{0}, store.resetCalls,
		"the row is reset too: DriverCursorsSet cannot lower it")

	// The repair is durable, so the next process resumes rather than re-deriving.
	next := sweeperOver(store)
	next.resume(ctx)
	assert.EqualValues(t, 500, next.cursor, "a restart reads the repaired cursor")
}

// staleCursorName is the cursor key for a kind set, for seeding a double.
func staleCursorName(kinds ...GroupKind) string {
	return staleDependentsCursorName(kinds)
}

// TestStaleDependentsCursorNameScopesToTheKindSet: order does not matter, but
// membership does. A name that ignored membership would let a cursor earned
// without a kind be resumed once that kind is back.
func TestStaleDependentsCursorNameScopesToTheKindSet(t *testing.T) {
	a := GroupKind{Kind: "A"}
	b := GroupKind{Group: "g", Kind: "B"}

	assert.Equal(t, staleCursorName(a, b), staleCursorName(b, a), "order is not membership")
	assert.NotEqual(t, staleCursorName(a), staleCursorName(a, b), "an added kind is a different set")
	assert.NotEqual(t, staleCursorName(a, b), staleCursorName(b), "so is a dropped one")
	assert.Contains(t, staleCursorName(a), cursorNameStaleDependents+"/")
}

// TestStaleDependentsCursorNameIsUnambiguous: nothing restricts a group or kind
// to delimiter-free text, so the encoding must not depend on delimiters. Both
// pairs below collide under a "group/kind\n" join, and a collision means a set
// silently inherits another set's cursor and skips the sweep it is owed.
func TestStaleDependentsCursorNameIsUnambiguous(t *testing.T) {
	assert.NotEqual(t,
		staleCursorName(GroupKind{Group: "a", Kind: "b/c"}),
		staleCursorName(GroupKind{Group: "a/b", Kind: "c"}),
		"a slash inside a field is not a field boundary")

	assert.NotEqual(t,
		staleCursorName(GroupKind{Group: "a", Kind: "b\nc/d"}),
		staleCursorName(GroupKind{Group: "a", Kind: "b"}, GroupKind{Group: "c", Kind: "d"}),
		"a newline inside a field is not a set boundary")
}

// TestStaleDependentsSweepRederivesForANewlyRegisteredKind is the strand the
// scoping exists to stop. A process running without a kind still advances its
// cursor past target writes; if that cursor were shared, the process that brings
// the kind back would resume above those writes and never find its dependents
// while the targets stay quiet.
func TestStaleDependentsSweepRederivesForANewlyRegisteredKind(t *testing.T) {
	other := GroupKind{Kind: "Other"}
	store := &staleSweepStore{issued: 500}
	ctx := context.Background()

	// A process that registers only clientTestGK sweeps and records its cursor.
	first := sweeperOverKinds(store, clientTestGK)
	first.resume(ctx)
	first.sweep(ctx)
	require.EqualValues(t, 500, first.cursor)
	require.Len(t, store.setCalls, 1)

	// A later process registers one more kind. Its dependents were never in the
	// first process's scope, so the cursor above says nothing about them.
	second := sweeperOverKinds(store, clientTestGK, other)
	second.resume(ctx)

	assert.Zero(t, second.cursor, "a wider kind set re-derives from the start")

	store.asked = nil
	second.sweep(ctx)
	assert.Equal(t, []StalePos{{TargetVersion: 1}}, store.asked, "and scans the whole graph once")
}

// cursorOnlyStore implements DriverCursorer and nothing more — a backend written
// against the interface before DriverCursorResetter existed.
type cursorOnlyStore struct {
	fakeStore
	issued   int64
	stored   map[string]int64
	setCalls []int64
}

func (s *cursorOnlyStore) ResourceVersionsMaxIssued(context.Context) (int64, error) {
	return s.issued, nil
}

func (s *cursorOnlyStore) DriverCursorsGet(_ context.Context, name string) (int64, bool, error) {
	v, ok := s.stored[name]
	return v, ok, nil
}

// Set-if-greater, as the contract requires: without the guard a test could not
// tell a cursor that was lowered from one that was left alone.
func (s *cursorOnlyStore) DriverCursorsSet(_ context.Context, name string, cursor int64) error {
	s.setCalls = append(s.setCalls, cursor)
	if s.stored == nil {
		s.stored = map[string]int64{}
	}
	if stored, ok := s.stored[name]; !ok || cursor > stored {
		s.stored[name] = cursor
	}
	return nil
}

// TestStaleDependentsSweepKeepsCursorsWithoutTheResetter is the compatibility
// guarantee. Reset is a capability of its own, so a store that predates it still
// persists its cursors — folding Reset into DriverCursorer would have made such
// a store fail the assertion and lose persistence with no build error.
func TestStaleDependentsSweepKeepsCursorsWithoutTheResetter(t *testing.T) {
	store := &cursorOnlyStore{issued: 500, stored: map[string]int64{staleCursorName(clientTestGK): 200}}
	sd := sweeperOver(store)
	ctx := context.Background()

	require.NotNil(t, sd.cursors, "the older capability is still satisfied")
	require.Nil(t, sd.resets)

	sd.resume(ctx)
	require.EqualValues(t, 200, sd.cursor, "the stored cursor is still read")

	sd.sweep(ctx)
	assert.Equal(t, []int64{500}, store.setCalls, "and still written")
}

// A foreign cursor is repaired in memory even where the store cannot lower the
// row. The cost is one re-derivation per start, which is what this pass did
// before the cursor existed.
func TestStaleDependentsSweepRederivesInMemoryWithoutTheResetter(t *testing.T) {
	store := &cursorOnlyStore{issued: 500, stored: map[string]int64{staleCursorName(clientTestGK): 9000}}
	sd := sweeperOver(store)
	ctx := context.Background()
	sd.resume(ctx)

	sd.sweep(ctx)

	assert.EqualValues(t, 500, sd.cursor, "this process re-derives and moves on")
	assert.Equal(t, 9000, int(store.stored[staleCursorName(clientTestGK)]), "the row it cannot lower is left alone")
}

// TestStaleDependentsSweepSweepsWhenTheForeignResetFails: the reset is the
// durable half of the repair, and the in-memory half stands on its own. A failed
// reset costs one more re-derivation at the next start, not a skipped sweep.
func TestStaleDependentsSweepSweepsWhenTheForeignResetFails(t *testing.T) {
	store := &staleSweepStore{
		issued:   500,
		stored:   map[string]int64{staleCursorName(clientTestGK): 9000},
		resetErr: errBoom,
	}
	logger, logs := captureLogger(slog.LevelWarn)
	sd := sweeperOver(store)
	sd.bh.logger = logger
	ctx := context.Background()
	sd.resume(ctx)

	sd.sweep(ctx)

	assert.Equal(t, []StalePos{{TargetVersion: 1}}, store.asked, "the scan still restarts from the beginning")
	assert.EqualValues(t, 500, sd.cursor)
	assert.Contains(t, logs.String(), "resetting the stale-dependents cursor failed")
}

// TestStaleDependentsSweepDoesNotRestampAConsumedVersion: a completed sweep
// consumed everything up to its mark, so the next one must start above it.
// Resuming at (mark, 0, 0) still matches every target at that version, because
// ids are positive — so a target sitting exactly there, whose dependents have
// not reconciled yet, would have its whole fan-out stamped again on every sweep.
func TestStaleDependentsSweepDoesNotRestampAConsumedVersion(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	sd := sweeperOver(store)
	spec := []byte(`{}`)

	// The target is written last, so it sits at exactly the mark sweep one ends on.
	dep, err := store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: spec})
	require.NoError(t, err)
	target, err := store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: spec})
	require.NoError(t, err)
	require.NoError(t, addEdge(ctx, store, dep.ID, target.ID, RelationDependsOn))

	sd.sweep(ctx)
	raw, err := store.ObjectsGet(ctx, dep.ID)
	require.NoError(t, err)
	require.EqualValues(t, 1, raw.ReconcileOwed, "the dependent has no watermark, so one pass is owed")

	// An unrelated write moves the mark, so the next tick sweeps. The target
	// itself has not moved, and the dependent is still stale.
	_, err = store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: spec})
	require.NoError(t, err)

	sd.sweep(ctx)

	raw, err = store.ObjectsGet(ctx, dep.ID)
	require.NoError(t, err)
	assert.EqualValues(t, 1, raw.ReconcileOwed, "the target did not move, so nothing more is owed")
}

// TestStaleDependentsSweepLeavesADurableFinding is the restart property the
// cursor rests on. The enqueue dies with the process, so what the sweep found
// has to be on the row: the owed pass names it on the way back up, and the
// cursor is free to move past a target that has since gone quiet.
func TestStaleDependentsSweepLeavesADurableFinding(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	sd := sweeperOver(store)

	spec := []byte(`{}`)
	target, err := store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: spec})
	require.NoError(t, err)
	dep, err := store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: spec})
	require.NoError(t, err)
	require.NoError(t, addEdge(ctx, store, dep.ID, target.ID, RelationDependsOn))

	sd.sweep(ctx)

	owed, err := store.ReconcileOwedListIDs(ctx, clientTestGK)
	require.NoError(t, err)
	assert.Equal(t, []ObjectID{dep.ID}, owed, "the finding outlives the queue it was also put on")
}

// TestStaleDependentsSweepWarnsWhenTheMarkReadFails: without the mark the sweep
// cannot say what it would have covered, so it lists nothing and keeps its
// cursor.
func TestStaleDependentsSweepWarnsWhenTheMarkReadFails(t *testing.T) {
	store := &staleSweepStore{issued: 500, issuedErr: errBoom}
	logger, logs := captureLogger(slog.LevelWarn)
	sd := sweeperOver(store)
	sd.bh.logger = logger

	sd.sweep(context.Background())

	assert.Empty(t, store.asked, "nothing is listed")
	assert.Zero(t, sd.cursor, "and the cursor stays put")
	assert.Contains(t, logs.String(), "reading the resource version failed")
}

// TestStaleDependentsSweepHoldsTheCursorOnStampFailure: a finding that could not
// be recorded must not be counted as covered. The enqueue alone dies with the
// process, so moving the cursor past it would strand the dependent.
func TestStaleDependentsSweepHoldsTheCursorOnStampFailure(t *testing.T) {
	page := []ObjectRef{{ID: 1, Group: clientTestGK.Group, Kind: clientTestGK.Kind}}
	store := &staleSweepStore{issued: 500, pages: [][]ObjectRef{page}, stampErr: errBoom}
	logger, logs := captureLogger(slog.LevelWarn)
	sd := sweeperOver(store)
	sd.bh.logger = logger

	sd.sweep(context.Background())

	assert.Zero(t, sd.cursor, "the cursor does not move past an unrecorded finding")
	assert.Empty(t, store.setCalls, "and nothing is persisted")
	assert.Contains(t, logs.String(), "stamping stale dependents failed")
}

// TestStaleDependentsSweepHoldsTheCursorOnListFailure pins the failure contract:
// a sweep that cannot read gives up on this pass, says so, and leaves the cursor
// where it was. Holding it is the whole of the repair now that the scan is
// bounded — advancing past a page that was never read would strand every
// dependent in it, because nothing re-derives a target that has gone quiet.
func TestStaleDependentsSweepHoldsTheCursorOnListFailure(t *testing.T) {
	ctx := context.Background()
	store := &staleListErrorStore{issued: 900}
	logger, logs := captureLogger(slog.LevelWarn)
	sd := sweeperOver(store)
	sd.bh.logger, sd.cursor = logger, 200

	sd.sweep(ctx)
	require.EqualValues(t, 1, store.calls.Load(), "a failed page abandons the sweep")
	assert.Contains(t, logs.String(), "listing stale dependents failed")
	assert.EqualValues(t, 200, sd.cursor, "the cursor does not move past a page nobody read")
	assert.Empty(t, store.setCalls, "and nothing is persisted")

	sd.sweep(ctx)
	assert.EqualValues(t, 2, store.calls.Load(), "the next pass asks again")
	assert.EqualValues(t, 200, sd.cursor, "from the same place")
}

// TestStaleDependentsSweepIsQuietOnShutdown separates the two reasons a listing
// fails. Stop cancels the same ctx the sweep reads on, so a pass in flight when
// the control plane goes down fails for no reason of its own — warning there
// would report a fault on every clean shutdown.
func TestStaleDependentsSweepIsQuietOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	logger, logs := captureLogger(slog.LevelWarn)
	sd := sweeperOver(&staleListErrorStore{issued: 900})
	sd.bh.logger = logger

	sd.sweep(ctx)

	assert.Empty(t, logs.String(), "a cancelled read is shutdown, not a lost pass")
}
