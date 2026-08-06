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
	"time"

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
	bh := newTestBeehive(t, store)

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
	// The cadences New would have set: a rate limit left at zero is a rate
	// limit switched off, which is not what production does.
	bh := &Beehive{
		store: store, reconcilers: rs, order: order,
		wakeScanMinInterval:     defaultWakeScanMinInterval,
		wakePersistInterval:     defaultWakePersistInterval,
		staleDependentsInterval: defaultStaleDependentsInterval,
	}
	bh.waker = newWaker(bh)
	return bh.waker, rs
}

// seededWaker is wakerOver for a test that starts from a waker already past its
// seed, with a clock it drives by hand.
func seededWaker(store Store, kinds ...GroupKind) (*waker, *fakeClock, map[GroupKind]*reconciler) {
	dw, rs := wakerOver(store, kinds...)
	clk := fakeClockOn(&dw.now)
	dw.seeded = true
	return dw, clk, rs
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

// scan's result is what the run loop dispatches on: how soon to look again, and
// whether to drop the wakes arriving meanwhile.
func TestWakerScanReportsWhatHappened(t *testing.T) {
	widget := GroupKind{Kind: "Widget"}

	t.Run("a fresh seed has nothing behind it", func(t *testing.T) {
		store := &replayStore{seed: 500, rows: replayRows(3)}
		dw, _ := wakerOver(store, widget)

		assert.Equal(t, scanIdle, dw.scan(context.Background()), "seeding at the mark leaves no backlog")
	})

	t.Run("a resumed seed reports the backlog it found", func(t *testing.T) {
		store := &cursorStore{
			replayStore: replayStore{seed: 500, rows: replayRows(3)},
			stored:      map[string]int64{cursorNameWaker: 200},
		}
		dw, _ := wakerOver(store, widget)

		assert.Equal(t, scanMore, dw.scan(context.Background()),
			"resuming below the mark must not wait a floor for its first page")
	})

	t.Run("a failed seed", func(t *testing.T) {
		store := &replayStore{seedErr: errBoom}
		dw, _ := wakerOver(store, widget)

		assert.Equal(t, scanFailed, dw.scan(context.Background()))
	})

	t.Run("a drained log", func(t *testing.T) {
		store := &replayStore{rows: replayRows(3)}
		dw, _, _ := seededWaker(store, widget)

		assert.Equal(t, scanIdle, dw.scan(context.Background()), "a short page means the log is drained")
	})

	t.Run("a full page budget", func(t *testing.T) {
		store := &replayStore{rows: replayRows(wakeFullBudget + 5)}
		dw, _, _ := seededWaker(store, widget)

		assert.Equal(t, scanMore, dw.scan(context.Background()), "stopping at the budget leaves work behind")
	})

	t.Run("a failed page", func(t *testing.T) {
		store := &replayStore{rows: replayRows(3), err: errBoom}
		dw, _, _ := seededWaker(store, widget)

		assert.Equal(t, scanFailed, dw.scan(context.Background()))
	})
}

// Priming is what Start does before it returns, and the order inside it is the
// whole design: the waker has no tick, so a commit landing before the subscribe
// wakes nothing at all.
func TestWakerPrimeSubscribesBeforeItSeeds(t *testing.T) {
	store := &seedProbe{Store: &fakeStore{}, mark: 500}
	bh := newTestBeehive(t, store)
	_, err := Register(bh, GroupKind{Kind: "Widget"}, &reconcileCapture{})
	require.NoError(t, err)

	var subscribed bool
	store.onRead = func() { subscribed = bh.waker.rx != nil }

	bh.waker.prime(context.Background())
	defer bh.waker.teardown()

	assert.True(t, subscribed, "a write committed during the seed read must still find a listener")
	require.True(t, bh.waker.seeded)
	assert.EqualValues(t, 500, bh.waker.watermark)
}

// The commit wake is the whole point: a dependent must not wait out a tick to
// learn its target moved. The floor here is an hour, so a scan can only be the
// wake's doing.
func TestWakerScansWhenAWriteCommits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inner := &replayStore{rows: replayRows(1), lists: make(chan struct{}, 8)}
	seeded := make(chan struct{}, 8)
	store := &seedProbe{Store: inner, onRead: func() { probeSignal(seeded) }}
	bh := newTestBeehive(t, store)
	_, err := Register(bh, GroupKind{Kind: "Widget"}, &reconcileCapture{})
	require.NoError(t, err)

	// Priming is what Start does, and it subscribes before it seeds — so once it
	// returns, "the waker was listening" is a fact rather than a bet on
	// scheduling. A send with no receiver reaches nobody, and there is no replay.
	bh.waker.prime(ctx)
	waitClosed(t, chanAfter(seeded, 1), "the waker to seed its watermark")

	done := make(chan struct{})
	go func() { defer close(done); bh.waker.run(ctx) }()

	require.NoError(t, bh.kindWriteHub.Send(GroupKind{Kind: "Unwatched"}),
		"any kind wakes it: the scan is store-wide")
	waitClosed(t, chanAfter(inner.lists, 1), "the waker to scan on the commit wake")

	cancel()
	waitClosed(t, done, "the waker to stop")
}

// The link between a client write and the waker: a commit publishes a wake to
// the subscription waker.run holds. The loop test above sends on the hub by
// hand and takes it from there, and the scan tests take it from there again —
// so this is the one that pins the publish actually happening.
//
// The kind here has no controller and no watch, which is the case a per-kind
// subscription could not serve: nothing but a store-wide subscriber would name
// it. Nothing is started, so no driver can be the cause of what arrives.
func TestAClientWriteWakesTheWakersSubscription(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	bh := newTestBeehive(t, newClientTestStore(t))
	rx, ok := bh.kindWriteHub.WatchAcross() // as waker.run subscribes
	require.True(t, ok)
	defer rx.Close()

	client := NewClient[cSpec, cStatus](bh, clientOnlyGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "v1"})

	ev, err := rx.RecvContext(ctx)
	require.NoError(t, err, "the create published no wake")
	assert.Equal(t, clientOnlyGK, ev.Key)

	_, err = client.Update(ctx, obj.ID, cSpec{Val: "v2"})
	require.NoError(t, err)

	ev, err = rx.RecvContext(ctx)
	require.NoError(t, err, "the spec write published no wake")
	assert.Equal(t, clientOnlyGK, ev.Key)
}

// The wake is the waker's only cadence, so a Beehive assembled without a hub —
// every waker test above — still runs, but on the eager first pass alone. The
// stale-dependents pass is what covers it from there.
func TestWakerRunsWithoutAWriteHub(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &replayStore{rows: replayRows(1), lists: make(chan struct{}, 8)}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.prime(ctx)

	done := make(chan struct{})
	go func() { defer close(done); dw.run(ctx) }()

	waitClosed(t, chanAfter(store.lists, 1), "the eager first pass")
	cancel()
	waitClosed(t, done, "the waker to stop")
}

// The waker holds no timer of its own while it is idle: with nothing to drive
// it but a commit, the scans are exactly the ones this test caused — the eager
// first pass and one per wake.
func TestIdleWakerIssuesNoQueries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	widget := GroupKind{Kind: "Widget"}
	store := &replayStore{lists: make(chan struct{}, 8)}
	// No throttle, so every wake drives a scan of its own rather than folding
	// into the one before it.
	bh := newTestBeehive(t, store, withWakeScanMinInterval(0))
	_, err := Register(bh, widget, &reconcileCapture{})
	require.NoError(t, err)
	bh.waker.prime(ctx)

	done := make(chan struct{})
	go func() { defer close(done); bh.waker.run(ctx) }()

	// One wake at a time: the hub holds one slot per receiver, so two sends the
	// waker has not read yet collapse into one.
	waitClosed(t, chanAfter(store.lists, 1), "the eager first pass")
	for range 2 {
		require.NoError(t, bh.kindWriteHub.Send(widget))
		waitClosed(t, chanAfter(store.lists, 1), "a scan for the wake")
	}

	cancel()
	waitClosed(t, done, "the waker to stop")
	assert.Len(t, store.pages, 3, "nothing but a wake reads the store")
}

// The retry is the only way back from a failed scan: backingOff drops the wakes
// arriving meanwhile, and nothing ticks. No wake is sent here at all.
func TestWakerRecoversFromAFailedScanWithoutATick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &replayStore{
		rows:         replayRows(1),
		err:          errBoom,
		healFromCall: 2,
		lists:        make(chan struct{}, 8),
	}
	// The ladder is capped at the stale-dependents cadence, which is what makes
	// the retry here fast enough to wait on.
	bh := newTestBeehive(t, store, withStaleDependentsInterval(fastTick))
	_, err := Register(bh, GroupKind{Kind: "Widget"}, &reconcileCapture{})
	require.NoError(t, err)
	bh.waker.prime(ctx)

	done := make(chan struct{})
	go func() { defer close(done); bh.waker.run(ctx) }()

	waitClosed(t, chanAfter(store.lists, 2), "the waker to retry the scan that failed")
	cancel()
	waitClosed(t, done, "the waker to stop")
}

// A commit landing during a failed scan refills the wake slot, so honouring it
// would keep a degraded store re-reading as fast as it can fail. The wake is
// consumed and dropped until the retry timer fires.
//
// Deterministic without a sleep: Sender.Close lets a receiver read its unread
// value once more before reporting ErrClosed, so the wake below is delivered
// first and the close ends the loop second.
func TestWakerDropsWakesWhileBackingOff(t *testing.T) {
	store := &replayStore{rows: replayRows(3), err: errBoom, lists: make(chan struct{}, 8)}
	bh := newTestBeehive(t, store)
	widget := GroupKind{Kind: "Widget"}
	_, err := Register(bh, widget, &reconcileCapture{})
	require.NoError(t, err)
	// Primed, so the eager first pass is a scan — and it fails, which is what
	// starts the backoff the wake below must not break into.
	bh.waker.prime(context.Background())

	done := make(chan struct{})
	go func() { defer close(done); bh.waker.run(context.Background()) }()
	waitClosed(t, chanAfter(store.lists, 1), "the waker's first scan")

	require.NoError(t, bh.kindWriteHub.Send(widget))
	bh.kindWriteHub.Close()
	waitClosed(t, done, "the waker to stop")

	// The floor is an hour, so a second read could only have come from the wake.
	assert.Len(t, store.pages, 1, "the wake must not drive a re-read while the store is failing")
}

// Closing the hub ends the waker. It is a safety net rather than the normal
// exit — stop cancels runCtx and waits on the WaitGroup the waker is in, and
// only closes the hub after — so drive it directly, or the test pins nothing.
func TestWakerClosedHubArmReturns(t *testing.T) {
	store := &replayStore{rows: replayRows(1)}
	bh := newTestBeehive(t, store)
	_, err := Register(bh, GroupKind{Kind: "Widget"}, &reconcileCapture{})
	require.NoError(t, err)
	bh.waker.prime(context.Background())

	done := make(chan struct{})
	go func() { defer close(done); bh.waker.run(context.Background()) }()

	bh.kindWriteHub.Close()
	waitClosed(t, done, "the waker to end with the closed hub, rather than spin on it")
}

// pass is one turn of the run loop: it reports how long to wait and whether
// wakes are being dropped. The rate assertions drive it at instants of their
// own choosing, so none of them sleeps.
func TestWakerPassPacesTheLoop(t *testing.T) {
	widget := GroupKind{Kind: "Widget"}
	ctx := context.Background()

	t.Run("a quiet pass arms nothing", func(t *testing.T) {
		dw, clk, _ := seededWaker(&replayStore{}, widget)

		next, backingOff := dw.pass(ctx, clk.now(), false)
		assert.Equal(t, wakeIdle, next, "a drained log gives the loop no reason to look again")
		assert.False(t, backingOff)
	})

	t.Run("a throttled pass waits out the throttle and scans nothing", func(t *testing.T) {
		store := &replayStore{}
		dw, clk, _ := seededWaker(store, widget)

		dw.pass(ctx, clk.now(), false)
		pagesAfterFirst := len(store.pages)

		clk.advance(defaultWakeScanMinInterval / 4)
		next, _ := dw.pass(ctx, clk.now(), false)
		assert.Equal(t, 3*defaultWakeScanMinInterval/4, next, "re-armed for what is left of the throttle")
		assert.Len(t, store.pages, pagesAfterFirst, "and the refused pass read nothing")
	})

	t.Run("the first wake after a quiet period is eager", func(t *testing.T) {
		store := &replayStore{}
		dw, clk, _ := seededWaker(store, widget)

		dw.pass(ctx, clk.now(), false)
		clk.advance(defaultWakeScanMinInterval)
		dw.pass(ctx, clk.now(), false)

		assert.Len(t, store.pages, 2, "an idle-to-active transition pays no added latency")
	})

	t.Run("more work re-arms at the throttle, not the floor", func(t *testing.T) {
		store := &replayStore{rows: replayRows(wakeFullBudget + 5)}
		dw, clk, _ := seededWaker(store, widget)

		next, _ := dw.pass(ctx, clk.now(), false)
		assert.Equal(t, defaultWakeScanMinInterval, next, "a resume drains at the throttle's rate")
	})

	t.Run("a failed pass climbs its own retry ladder and drops wakes", func(t *testing.T) {
		store := &replayStore{rows: replayRows(3), err: errBoom}
		dw, clk, _ := seededWaker(store, widget)

		next, backingOff := dw.pass(ctx, clk.now(), false)
		assert.Equal(t, wakeRetryBase, next, "the retry is the waker's only reason to look again")
		assert.True(t, backingOff, "a live writer must not keep a degraded store re-reading as fast as it can fail")

		clk.advance(defaultWakeScanMinInterval)
		next, _ = dw.pass(ctx, clk.now(), true)
		assert.Equal(t, 2*wakeRetryBase, next, "a second failure waits longer")

		store.err = nil
		clk.advance(defaultWakeScanMinInterval)
		_, backingOff = dw.pass(ctx, clk.now(), true)
		require.False(t, backingOff)
		store.err = errBoom
		clk.advance(defaultWakeScanMinInterval)
		next, _ = dw.pass(ctx, clk.now(), false)
		assert.Equal(t, wakeRetryBase, next, "a scan that worked resets the ladder")
	})

	t.Run("the retry ladder is capped by the backstop's cadence", func(t *testing.T) {
		dw, clk, _ := seededWaker(&replayStore{rows: replayRows(3), err: errBoom}, widget)

		var last time.Duration
		for range 20 {
			last, _ = dw.pass(ctx, clk.now(), false)
			clk.advance(defaultWakeScanMinInterval)
		}
		assert.Equal(t, defaultStaleDependentsInterval, last,
			"past the stale-dependents pass a retry finds a subset of what the backstop already found")
	})

	t.Run("a throttled pass keeps a failure's backoff", func(t *testing.T) {
		dw, _, _ := seededWaker(&replayStore{rows: replayRows(3), err: errBoom}, widget)
		clk := fakeClockOn(&dw.now)

		_, backingOff := dw.pass(ctx, clk.now(), false)
		require.True(t, backingOff, "the scan failed")

		// The floor timer was already armed when the scan failed, so it fires
		// inside the throttle window and the next pass is refused. Reporting
		// "not backing off" there would hand a degraded store back to the
		// wakes, at the throttle's rate rather than the floor's.
		clk.advance(defaultWakeScanMinInterval / 2)
		next, backingOff := dw.pass(ctx, clk.now(), true)
		assert.Equal(t, defaultWakeScanMinInterval/2, next)
		assert.True(t, backingOff, "a refused pass decides nothing about the store's health")
	})

	t.Run("an outstanding cursor write is a reason to look again", func(t *testing.T) {
		store := &cursorStore{replayStore: replayStore{rows: replayRows(3)}, setErr: errBoom}
		dw, clk, _ := seededWaker(store, widget)

		next, _ := dw.pass(ctx, clk.now(), false)
		assert.Equal(t, defaultWakePersistInterval, next,
			"the log is drained, but a successor that finds no row would reseed at the mark")

		store.setErr = nil
		clk.advance(defaultWakePersistInterval)
		next, _ = dw.pass(ctx, clk.now(), false)
		assert.Equal(t, wakeIdle, next, "once the write lands there is nothing left to look again for")
	})

	t.Run("a failing cursor write paces the retry in time, not in passes", func(t *testing.T) {
		store := &cursorStore{replayStore: replayStore{rows: replayRows(3)}, setErr: errBoom}
		dw, clk, _ := seededWaker(store, widget)

		// A minute of it, each pass waiting exactly what it asked for. Every
		// pass costs a scan, so a ladder counted in passes would spend the
		// minute reading a store that is already failing its writes.
		var elapsed time.Duration
		passes := 0
		for elapsed < time.Minute {
			next, _ := dw.pass(ctx, clk.now(), false)
			require.Positive(t, next, "a cursor write still owed is a reason to look again")
			clk.advance(next)
			elapsed += next
			passes++
		}
		assert.Less(t, passes, 10, "the retry backs off in time rather than looking every floor")
		assert.Equal(t, passes, store.setAttempts, "and every pass it does make attempts the write")
	})

	t.Run("a disabled throttle drains without pausing", func(t *testing.T) {
		store := &replayStore{rows: replayRows(wakeFullBudget + 5)}
		dw, _, _ := seededWaker(store, widget)
		// Rebuilt, not just re-set: the gates take their intervals at
		// construction, which is what keeps an option from being ignored.
		dw.bh.wakeScanMinInterval = 0
		dw = newWaker(dw.bh)
		clk := fakeClockOn(&dw.now) // after the rebuild, or it clocks the discarded waker
		dw.seeded = true

		next, _ := dw.pass(ctx, clk.now(), false)
		assert.Zero(t, next, "with no throttle to wait out, the backlog is drained at once")
	})
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
	require.NotEqual(t, scanFailed, first.seed(context.Background()))
	require.Equal(t, []int64{10}, store.setCalls, "the seed point is durable before any change arrives")

	// Target 1 changes with no waker running to see it.
	store.rows, store.seed = changedAt(20), 20

	second, rs := wakerOver(store, widget)
	require.NotEqual(t, scanFailed, second.seed(context.Background()))
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

	require.NotEqual(t, scanFailed, dw.seed(context.Background()))
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

// A cursor below the horizon lost the entries in between: retention deleted them
// before this waker read them, so their dependents were never woken here. The
// span is reported, and the resume skips the range rather than replaying a log
// that no longer holds it.
func TestWakerReportsATrimmedSpanAtSeed(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	store := &cursorStore{
		replayStore: replayStore{seed: 500, trimmed: 450, rows: replayRows(3)},
		stored:      map[string]int64{cursorNameWaker: 400},
	}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.bh.logger = logger

	dw.seed(context.Background())

	assert.EqualValues(t, 400, dw.watermark, "reported, but the resume still starts at the cursor")
	assert.Contains(t, buf.String(), "trimmed")
	assert.Contains(t, buf.String(), "cursor=400", "the span starts at the stored cursor, not at the clamp")
	assert.Contains(t, buf.String(), "trimmedThrough=450")
}

// The clamp lowers the watermark below the horizon on every restart of a store
// whose log was trimmed empty, so comparing the horizon against the watermark
// would report a span this waker had in fact processed.
func TestWakerReportsNothingWhenTheClampLowersTheWatermark(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	store := &cursorStore{
		replayStore: replayStore{seed: 0, trimmed: 900}, // the log is trimmed away entirely
		stored:      map[string]int64{cursorNameWaker: 950},
	}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.bh.logger = logger

	require.Equal(t, scanIdle, dw.seed(context.Background()))
	require.Equal(t, scanIdle, dw.scan(context.Background()))

	assert.Empty(t, buf.String(), "this waker processed past the horizon; nothing was skipped")
}

// A waker whose scans have been failing sits still while retention keeps
// trimming, so the boundary rises under a live cursor too — and the page it
// finally reads is the one that says so. One line per boundary, not one per page.
func TestWakerReportsATrimmedSpanOnce(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	store := &replayStore{rows: replayRows(2*wakeScanPageCap + 1), trimmed: 450}
	dw, _, _ := seededWaker(store, GroupKind{Kind: "Widget"})
	dw.bh.logger = logger

	require.Equal(t, scanIdle, dw.scan(context.Background()), "several pages, one scan")
	require.Equal(t, 1, strings.Count(buf.String(), "trimmedThrough=450"))

	buf.Reset()
	dw.watermark = 0 // fall behind again, under a boundary that has not moved
	require.Equal(t, scanIdle, dw.scan(context.Background()))
	assert.Empty(t, buf.String(), "the same boundary is reported once, not once per pass")

	store.trimmed = 900
	dw.watermark = 0
	require.Equal(t, scanIdle, dw.scan(context.Background()))
	assert.Contains(t, buf.String(), "trimmedThrough=900", "a boundary that rises is a new loss")
}

// An untrimmed log reports 0, and a boundary the waker has already scanned past
// cost it nothing — equality included, since the next unread entry is above it.
func TestWakerReportsNothingOnAnUntrimmedLog(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	store := &replayStore{rows: replayRows(3)}
	dw, _, _ := seededWaker(store, GroupKind{Kind: "Widget"})
	dw.bh.logger = logger

	require.Equal(t, scanIdle, dw.scan(context.Background()))
	require.Empty(t, buf.String(), "nothing has been trimmed")

	store.trimmed = 3 // exactly the watermark: the next unread entry is 4
	dw.watermark = 3
	require.Equal(t, scanIdle, dw.scan(context.Background()))
	assert.Empty(t, buf.String(), "a cursor on the boundary has lost nothing")
}

// The horizon is a maximum over kinds, and the per-kind count bound trims a chatty
// kind far deeper than a quiet one: kind A trimmed through 1000 while kind B still
// holds an unread entry at 500. So the horizon proves entries below it were
// deleted, never that the whole range is gone — resuming above it would skip B's
// surviving entry for good.
func TestWakerResumeKeepsEntriesBelowTheHorizon(t *testing.T) {
	store := &cursorStore{
		replayStore: replayStore{
			seed:    500,
			trimmed: 1000, // a chatty kind, trimmed well past what a quiet one still holds
			rows:    []ObjectWrite{{ID: 1, ResourceVersion: 500}},
		},
		stored: map[string]int64{cursorNameWaker: 400},
	}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})

	require.Equal(t, scanMore, dw.seed(context.Background()))
	require.EqualValues(t, 400, dw.watermark, "the clamp decides the resume point, not the horizon")

	require.Equal(t, scanIdle, dw.scan(context.Background()))
	assert.Equal(t, 1, store.read, "the entry that survived the trim is still scanned")
	assert.EqualValues(t, 500, dw.watermark)
}

// A stalled waker whose backlog retention removed entirely reads an empty page,
// which carries no horizon — so the loss would go unreported unless the pass asks
// for it. It asks once per watermark, not once per wake.
func TestWakerReportsAFullyTrimmedBacklog(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	store := &replayStore{seed: 400, trimmed: 900} // every entry above the cursor is gone
	dw, _, _ := seededWaker(store, GroupKind{Kind: "Widget"})
	dw.bh.logger = logger
	dw.watermark = 400

	require.Equal(t, scanIdle, dw.scan(context.Background()))
	assert.Contains(t, buf.String(), "cursor=400")
	assert.Contains(t, buf.String(), "trimmedThrough=900")
	require.Equal(t, 1, store.marks, "the page that could not carry the horizon asks for it")

	buf.Reset()
	require.Equal(t, scanIdle, dw.scan(context.Background()))
	assert.Equal(t, 2, store.marks, "and asks again, since retention moves on its own")
	assert.Empty(t, buf.String(), "but the boundary has not moved, so there is nothing new to say")
}

// A pass stops on its page budget with the backlog still unread, and retention can
// take the rest before the next pass runs. That pass reads an empty page, so the
// horizon a full page reported one pass ago says nothing about the boundary now.
func TestWakerReportsABacklogTrimmedBetweenPasses(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	store := &replayStore{rows: replayRows(2 * wakeFullBudget)}
	dw, _, _ := seededWaker(store, GroupKind{Kind: "Widget"})
	dw.bh.logger = logger

	require.Equal(t, scanMore, dw.scan(context.Background()), "the budget stopped this pass")
	require.EqualValues(t, wakeFullBudget, dw.watermark)

	// Retention takes everything this pass did not reach, between the two passes.
	store.rows, store.seed, store.trimmed = nil, 0, 2*wakeFullBudget

	require.Equal(t, scanIdle, dw.scan(context.Background()))
	assert.Contains(t, buf.String(), "the dependents of the changes in between",
		"the unread remainder was trimmed, and nothing else would say so")
}

// A failure streak is how a waker sits still long enough for retention to overtake
// it, and the watermark it holds still throughout says nothing about where the
// boundary moved meanwhile.
func TestWakerRereadsTheHorizonAfterAFailedScan(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	// The first read fails; by the second, retention has taken the whole backlog.
	store := &replayStore{seed: 400, trimmed: 900, err: errBoom, healFromCall: 2}
	dw, _, _ := seededWaker(store, GroupKind{Kind: "Widget"})
	dw.bh.logger = logger
	dw.watermark = 400

	require.Equal(t, scanFailed, dw.scan(context.Background()))
	buf.Reset() // the failure has its own warning

	require.Equal(t, scanIdle, dw.scan(context.Background()))
	assert.Contains(t, buf.String(), "trimmedThrough=900",
		"the stall the failure caused is exactly when retention overtakes a waker")
}

// A page that carried its own horizon needs no second read: it was read at the
// same instant as the rows the waker just consumed.
func TestWakerReadsNoHorizonWhenThePageCarriedIt(t *testing.T) {
	store := &replayStore{rows: replayRows(3), trimmed: 2}
	dw, _, _ := seededWaker(store, GroupKind{Kind: "Widget"})

	require.Equal(t, scanIdle, dw.scan(context.Background()))
	require.EqualValues(t, 3, dw.watermark)
	assert.Zero(t, store.marks, "a short page is not an empty one")
}

// A waker with no stored cursor starts at the log's head, so a trim that predates
// its seed skipped nothing it was ever going to scan.
func TestWakerReportsNothingWithoutAStoredCursor(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	store := &replayStore{seed: 0, trimmed: 900} // trimmed empty before this run began
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.bh.logger = logger

	require.Equal(t, scanIdle, dw.seed(context.Background()))
	require.Equal(t, scanIdle, dw.scan(context.Background()))

	assert.Empty(t, buf.String(), "nothing was skipped to report")
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
	require.NotEqual(t, scanFailed, dw.seed(context.Background()))
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

// The write-log read's own shutdown branch: seed's first read fails for the same
// reason, and reporting it would warn on every shutdown that overlapped a seed.
func TestWakerSeedReadFailureDuringShutdownIsQuiet(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	store := &replayStore{seed: 500, seedErr: errBoom}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.bh.logger = logger

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.Equal(t, scanFailed, dw.seed(ctx), "a cancelled read still leaves the waker unseeded")
	assert.False(t, dw.seeded)
	assert.Empty(t, buf.String(), "shutdown is not an outage to report")
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

	assert.Equal(t, scanFailed, dw.seed(ctx), "a cancelled read still leaves the waker unseeded")
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
	dw, _, _ := seededWaker(store, widget)

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

// The waker can be turned off outright. It is a supported choice — the
// reconcile_owed stamp still covers a dependency declared against a target that
// moved — so run must simply return.
func TestWakerDisabledByOption(t *testing.T) {
	store := &replayStore{rows: replayRows(3)}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	require.NoError(t, withDependencyWakerOff()(dw.bh))

	dw.prime(context.Background())
	dw.run(context.Background()) // returns immediately; a running waker would block

	assert.False(t, dw.seeded, "a disabled waker does not read the store to seed")
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
	clk := fakeClockOn(&first.now)
	require.NotEqual(t, scanFailed, first.seed(context.Background()))
	// A write lands while the first process is up; its scan finds and persists it.
	clk.advance(defaultWakePersistInterval) // a floor on from the seed's own write
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
	require.NotEqual(t, scanFailed, second.seed(context.Background()))
	assert.EqualValues(t, 20, second.watermark, "seeded from the stored cursor, not the write log's new max")

	second.scan(context.Background())
	assert.Equal(t, []ObjectID{9}, queuedIDs(rsSecond[widget].work),
		"the dependent of the write made while the process was down is woken on the first scan back")
}

// wakeFullBudget is how many rows one full-budget pass reads.
const wakeFullBudget = wakeScanPagesPerPass * wakeScanPageCap

// One tick reads at most wakeScanPagesPerPass pages, so a long backlog cannot
// monopolise the single connection the reconcile loops need too. The remainder
// is not lost: the cursor persists at whatever this tick reached, and the next
// tick resumes there rather than re-reading it.
func TestWakerStopsAtThePageBudget(t *testing.T) {
	total := wakeFullBudget + 5
	store := &cursorStore{replayStore: replayStore{rows: replayRows(total)}}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	dw.seeded = true

	dw.scan(context.Background())
	assert.Len(t, store.pages, wakeScanPagesPerPass, "the tick stops at the page budget")
	assert.EqualValues(t, wakeFullBudget, dw.watermark)
	assert.Equal(t, []int64{wakeFullBudget}, store.setCalls,
		"progress within the budget is still persisted")

	dw.scan(context.Background())
	assert.EqualValues(t, total, dw.watermark, "the next tick resumes at the budget, not from the start")
}

// A drain that has run for as long as the stale-dependents pass takes to sweep is
// re-deriving wakes that pass has already found, so it stops paging and jumps to
// the log's mark. The range it skipped is that pass's to deliver.
func TestWakerAbandonsADrainTheBackstopOvertook(t *testing.T) {
	const mark int64 = 9000
	const drains = 3
	store := &cursorStore{replayStore: replayStore{
		rows: replayRows(drains * wakeFullBudget), seed: mark,
	}}
	dw, clk, _ := seededWaker(store, GroupKind{Kind: "Widget"})
	dw.abandonAfter = (drains - 1) * defaultWakePersistInterval

	// Advanced by the persist floor, not the scan floor, so every pass's cursor
	// write lands and the last one is the jump's.
	for range drains - 1 {
		require.Equal(t, scanMore, dw.scan(context.Background()))
		clk.advance(defaultWakePersistInterval)
	}

	assert.Equal(t, scanIdle, dw.scan(context.Background()), "the drain gives up rather than paging on")
	assert.Equal(t, mark, dw.watermark, "and jumps to the log's mark")
	assert.Len(t, store.pages, drains*wakeScanPagesPerPass, "no page is read after the jump")
	assert.Equal(t, mark, store.setCalls[len(store.setCalls)-1],
		"the jump is persisted, so a restart does not re-drain what it skipped")
}

// Only continuous paging counts toward the threshold: a pass that caught up ends
// the drain, so a later one starts its own clock rather than inheriting an old
// drain's.
func TestWakerDrainStreakResetsOnAShortPage(t *testing.T) {
	store := &cursorStore{replayStore: replayStore{rows: replayRows(wakeFullBudget), seed: 9000}}
	dw, clk, _ := seededWaker(store, GroupKind{Kind: "Widget"})
	dw.abandonAfter = defaultStaleDependentsInterval

	require.Equal(t, scanMore, dw.scan(context.Background()), "a full budget starts a drain")
	store.rows = replayRows(wakeFullBudget + 5)
	require.Equal(t, scanIdle, dw.scan(context.Background()), "and a short page ends it")

	clk.advance(dw.abandonAfter)
	store.rows = replayRows(2*wakeFullBudget + 5)
	assert.Equal(t, scanMore, dw.scan(context.Background()), "so this drain is new, not overtaken")
	assert.EqualValues(t, 2*wakeFullBudget+5, dw.watermark, "the watermark paged rather than jumped")
}

// A failed page ends the drain too: the retry backoff paces what happens next, so
// nothing is holding the connection at the paging budget.
func TestWakerDrainStreakResetsOnAFailedPage(t *testing.T) {
	store := &cursorStore{replayStore: replayStore{
		rows: replayRows(wakeFullBudget), seed: 9000,
		// The first page of the second pass, and only that one.
		err: errBoom, failFromCall: wakeScanPagesPerPass + 1, healFromCall: wakeScanPagesPerPass + 2,
	}}
	dw, clk, _ := seededWaker(store, GroupKind{Kind: "Widget"})
	dw.abandonAfter = defaultStaleDependentsInterval

	require.Equal(t, scanMore, dw.scan(context.Background()))
	require.Equal(t, scanFailed, dw.scan(context.Background()))

	clk.advance(dw.abandonAfter)
	store.rows = replayRows(2 * wakeFullBudget)
	assert.Equal(t, scanMore, dw.scan(context.Background()), "the drain that failed does not count toward this one")
	assert.EqualValues(t, 2*wakeFullBudget, dw.watermark)
}

// withStaleDependentsInterval validates a positive interval, but only a Beehive
// from New goes through it — a whitebox test assembles the struct. A zero threshold
// there must mean "drain as it always did", not "shed the whole backlog on the
// second pass".
func TestWakerWithNoThresholdNeverAbandons(t *testing.T) {
	store := &cursorStore{replayStore: replayStore{rows: replayRows(3 * wakeFullBudget), seed: 9000}}
	dw, clk, _ := seededWaker(store, GroupKind{Kind: "Widget"})
	dw.abandonAfter = 0

	for i := range 3 {
		assert.Equal(t, scanMore, dw.scan(context.Background()), "pass %d keeps draining", i)
		clk.advance(time.Hour)
	}
	assert.EqualValues(t, 3*wakeFullBudget, dw.watermark, "every row was paged, none skipped")
}

// The mark read decides where to skip to, and no wake depends on it. So a failure
// there is not scanFailed — that would arm the retry backoff and drop the wakes
// arriving meanwhile over a read the drain does not need. The drain carries on, and
// the window restarts so the retry costs one read a window rather than one a pass.
func TestWakerAbandonRetriesAFailedMarkRead(t *testing.T) {
	const mark int64 = 9000
	store := &cursorStore{replayStore: replayStore{rows: replayRows(4 * wakeFullBudget), seed: mark}}
	dw, clk, _ := seededWaker(store, GroupKind{Kind: "Widget"})
	dw.abandonAfter = defaultStaleDependentsInterval

	require.Equal(t, scanMore, dw.scan(context.Background()))
	clk.advance(dw.abandonAfter)

	store.seedErr = errBoom
	assert.Equal(t, scanMore, dw.scan(context.Background()), "the drain continues rather than backing off")
	assert.EqualValues(t, 2*wakeFullBudget, dw.watermark, "having paged its budget as usual")

	store.seedErr = nil
	require.Equal(t, scanMore, dw.scan(context.Background()), "the next pass pages rather than re-reading the mark")

	clk.advance(dw.abandonAfter)
	assert.Equal(t, scanIdle, dw.scan(context.Background()), "and a window on, the skip is retried")
	assert.Equal(t, mark, dw.watermark)
}

// slowMarkStore fails the mark read after holding the clock for hold, which is what
// makes the restarted window's start instant observable.
type slowMarkStore struct {
	*cursorStore
	clk       *fakeClock
	hold      time.Duration
	markReads int
}

func (s *slowMarkStore) ObjectWritesMaxVersionAll(context.Context) (int64, int64, error) {
	s.markReads++
	s.clk.advance(s.hold)
	return 0, 0, errBoom
}

// The restarted window runs from after the failed read, not from before it: a read
// that blocked longer than a window would otherwise leave the retry unpaced, which
// is the per-pass re-reading the restart exists to stop.
func TestWakerAbandonPacesFromAfterTheFailedMarkRead(t *testing.T) {
	inner := &cursorStore{replayStore: replayStore{rows: replayRows(3 * wakeFullBudget), seed: 9000}}
	dw, clk, _ := seededWaker(inner, GroupKind{Kind: "Widget"})
	dw.abandonAfter = defaultStaleDependentsInterval
	store := &slowMarkStore{cursorStore: inner, clk: clk, hold: 2 * dw.abandonAfter}
	dw.bh.store = store

	require.Equal(t, scanMore, dw.scan(context.Background()))
	clk.advance(dw.abandonAfter)
	require.Equal(t, scanMore, dw.scan(context.Background()), "the failing read does not back off")
	require.Equal(t, 1, store.markReads)

	// The read itself consumed two windows, so a window dated before it would be
	// spent already and this pass would read the mark again.
	assert.Equal(t, scanMore, dw.scan(context.Background()))
	assert.Equal(t, 1, store.markReads, "the retry waits out a window measured from after the read")
}

// ObjectWritesMaxVersionAll is a bare MAX with no horizon folded in, so a trimmed
// log answers below the watermark — and a fully trimmed one answers 0. The jump
// must never take the watermark backwards onto rows it already scanned.
func TestWakerAbandonHoldsTheWatermarkWhenTheMarkIsLower(t *testing.T) {
	store := &cursorStore{replayStore: replayStore{rows: replayRows(2 * wakeFullBudget), seed: 5}}
	dw, clk, _ := seededWaker(store, GroupKind{Kind: "Widget"})
	dw.abandonAfter = defaultStaleDependentsInterval

	require.Equal(t, scanMore, dw.scan(context.Background()))
	clk.advance(dw.abandonAfter)

	assert.Equal(t, scanIdle, dw.scan(context.Background()), "the drain is still abandoned")
	assert.EqualValues(t, 2*wakeFullBudget, dw.watermark, "but the watermark holds")
}

// The threshold is the backstop's own cadence: past it, the sweep has found
// everything the drain is still working toward.
func TestWakerAbandonAfterIsTheBackstopCadence(t *testing.T) {
	dw, _ := wakerOver(&replayStore{}, GroupKind{Kind: "Widget"})
	dw.bh.staleDependentsInterval = 42 * time.Second

	assert.Equal(t, 42*time.Second, newWaker(dw.bh).abandonAfter)
}

// However far behind a stored cursor is, seed resumes from it: the distance is
// in resource_version units, which EventsAdd inflates without adding anything
// this scan would read, so no threshold over it could say whether the gap is
// worth draining. What bounds the cost instead is the page budget per pass, and
// abandonAfter once the drain has run long enough to be overtaken.
func TestWakerResumesAnEnormousBacklog(t *testing.T) {
	const mark = 50_000_000
	store := &cursorStore{
		replayStore: replayStore{seed: mark, rows: changedAt(10)},
		stored:      map[string]int64{cursorNameWaker: 0},
	}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})

	require.NotEqual(t, scanFailed, dw.seed(context.Background()))
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

// The cursor write is floored by a cadence of its own: it paces a write on the
// connection every commit needs, so it must not move when the scan floor does.
func TestWakerPersistFloorIsItsOwnCadence(t *testing.T) {
	dw, _ := wakerOver(&replayStore{}, GroupKind{Kind: "Widget"})
	dw.bh.wakeScanMinInterval = time.Hour

	assert.Equal(t, defaultWakePersistInterval, newWaker(dw.bh).persistGate.Interval())
}

// The cursor write is the waker's only write, and it lands on the connection
// every commit needs. A loop that turns ten times a second must not write the
// cursor ten times a second.
func TestWakerPersistsAtMostOncePerFloor(t *testing.T) {
	store := &cursorStore{replayStore: replayStore{rows: replayRows(1)}}
	dw, clk, _ := seededWaker(store, GroupKind{Kind: "Widget"})

	// Ten passes inside one floor, each with the watermark moving, so nothing
	// but the gate can be what holds the write back.
	for i := range 10 {
		store.rows = replayRows(i + 1)
		dw.scan(context.Background())
		clk.advance(defaultWakePersistInterval / 10)
	}
	assert.Equal(t, []int64{1}, store.setCalls, "one write per floor, whatever the pass rate")

	clk.advance(defaultWakePersistInterval)
	store.rows = replayRows(20)
	dw.scan(context.Background())
	assert.Equal(t, []int64{1, 20}, store.setCalls, "and the next floor writes the watermark it reached")
}

// A failed persist write is logged and leaves dw.persisted at its old value, so
// the next pass's guard (watermark > persisted) still holds and the write is
// retried — even on a pass that finds nothing new to scan, since it is
// persisted's staleness against watermark that drives the retry, not fresh
// pages.
func TestWakerRetriesPersistOnAFailedWrite(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	store := &cursorStore{replayStore: replayStore{rows: replayRows(3)}, setErr: errBoom}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	clk := fakeClockOn(&dw.now)
	dw.bh.logger = logger
	dw.seeded = true

	dw.scan(context.Background())
	assert.EqualValues(t, 3, dw.watermark, "the in-memory watermark still advances; only the durable write failed")
	assert.Empty(t, store.setCalls, "the failed write leaves no record of succeeding")
	assert.Zero(t, dw.persisted, "so persisted stays at its old baseline")
	assert.NotEmpty(t, buf.String(), "the failure is logged")

	store.setErr = nil
	clk.advance(defaultWakePersistInterval)
	dw.scan(context.Background()) // no rows above watermark=3, but persisted still lags it
	assert.Equal(t, []int64{3}, store.setCalls, "the next pass retries the write even though this scan found nothing new")
}

// A write that fails forever — a read-only or full database — must not become a
// doomed round trip and a warning every second. Holding persisted is what makes
// the retry happen at all, so nothing here stops retrying; the ladder is what
// paces it, and the streak is what keeps the log to one line about one cause.
func TestWakerBacksOffAFailingPersist(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	store := &cursorStore{replayStore: replayStore{rows: replayRows(3)}, setErr: errBoom}
	dw, _ := wakerOver(store, GroupKind{Kind: "Widget"})
	clk := fakeClockOn(&dw.now)
	dw.bh.logger = logger
	dw.seeded = true

	// A pass a floor apart, which is the shape a busy system produces: the
	// ladder is a delay, so most of these find the retry still closed.
	const ticks = 30
	for range ticks {
		dw.scan(context.Background())
		clk.advance(defaultWakePersistInterval)
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
		clk.advance(defaultWakePersistInterval)
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
	require.NotEqual(t, scanFailed, bh.waker.seed(context.Background()))
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
	bh := newTestBeehive(t, probe, fast(withDependencyWakerOff())...)

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
	calls  atomic.Int64
	issued int64
}

func (s *staleListErrorStore) ResourceVersionsMaxIssued(context.Context) (int64, error) {
	return s.issued, nil
}

func (s *staleListErrorStore) DependentsListStaleSince(_ context.Context, _ []GroupKind, after StalePos, _ int64, _ int) ([]ObjectRef, StalePos, error) {
	s.calls.Add(1)
	return nil, after, errBoom
}

// staleSweepStore serves the cursor-form listing one page at a time and records
// what the sweep asked for, stamped, and persisted.
type staleSweepStore struct {
	fakeStore
	issued    int64
	issuedErr error
	stampErr  error
	pages     [][]ObjectRef
	asked     []StalePos
	throughs  []int64
	stamped   [][]ObjectRef
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

// sweeperOver builds a stale-dependents sweeper over a store double, mirroring
// what staleDependentsRun assembles.
func sweeperOver(store Store) *staleDependents {
	return &staleDependents{
		bh:    &Beehive{store: store, logger: slog.New(slog.DiscardHandler)},
		kinds: []GroupKind{clientTestGK},
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

// TestStaleDependentsSweepAdvancesToThePreScanMark: the cursor moves to the mark
// read *before* the scan, and the next scan starts above it. A target written
// while the sweep runs sits above that mark, so the next sweep still finds it.
func TestStaleDependentsSweepAdvancesToThePreScanMark(t *testing.T) {
	store := &staleSweepStore{issued: 500}
	sd := sweeperOver(store)
	sd.cursor = 200

	sd.sweep(context.Background())

	assert.Equal(t, []StalePos{{TargetVersion: 201}}, store.asked, "above the version already consumed")
	assert.Equal(t, []int64{500}, store.throughs, "and bounded at the mark it read first")
	assert.EqualValues(t, 500, sd.cursor)
}

// TestStaleDependentsSweepSkipsAQuietStore: no version issued since the last
// sweep means no target moved, so no row can be stale. The listing is skipped,
// leaving one read per tick.
func TestStaleDependentsSweepSkipsAQuietStore(t *testing.T) {
	store := &staleSweepStore{issued: 500}
	sd := sweeperOver(store)
	sd.cursor = 500

	sd.sweep(context.Background())

	assert.Empty(t, store.asked, "nothing has moved, so nothing is listed")
}

// TestStaleDependentsSweepStartsEveryProcessAtTheBeginning is why the cursor is
// not persisted. A reconcile clears the owed count in one statement and records
// its watermark in another, so a process killed between them leaves a dependent
// stale with nothing durable naming it — the count gone, the object settled, its
// target quiet. Only re-derivation finds it, and every process does one.
func TestStaleDependentsSweepStartsEveryProcessAtTheBeginning(t *testing.T) {
	store := &staleSweepStore{issued: 500}
	ctx := context.Background()

	first := sweeperOver(store)
	first.sweep(ctx)
	require.EqualValues(t, 500, first.cursor, "this process will not rescan what it covered")

	store.asked = nil
	second := sweeperOver(store)
	second.sweep(ctx)

	assert.Equal(t, []StalePos{{TargetVersion: 1}}, store.asked,
		"a fresh process starts from nothing, so its first sweep scans the whole graph")
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

// TestStaleDependentsSweepLeavesADurableFinding pins that a finding reaches the
// row and not only the queue, so the owed pass names it on the way back up. This
// is not what makes the cursor sound — the process-local cursor is (see
// TestStaleDependentsSweepStartsEveryProcessAtTheBeginning). It is what keeps the
// guarantee off the queue's drop policy.
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

// TestStaleDependentsSweepHoldsTheCursorOnStampFailure: a page that could not be
// stamped was never enqueued either, so counting it as covered would leave those
// dependents to the next process's re-derivation — a whole restart of latency for
// a store that is merely failing writes.
func TestStaleDependentsSweepHoldsTheCursorOnStampFailure(t *testing.T) {
	page := []ObjectRef{{ID: 1, Group: clientTestGK.Group, Kind: clientTestGK.Kind}}
	store := &staleSweepStore{issued: 500, pages: [][]ObjectRef{page}, stampErr: errBoom}
	logger, logs := captureLogger(slog.LevelWarn)
	sd := sweeperOver(store)
	sd.bh.logger = logger

	sd.sweep(context.Background())

	assert.Zero(t, sd.cursor, "the cursor does not move past an unrecorded finding")
	assert.Contains(t, logs.String(), "stamping stale dependents failed")
}

// TestStaleDependentsSweepRepairsALostFindingAfterRestart walks what the
// process-local cursor exists for. The sweep stamps the dependent; that stamp is
// drained but the dependent is never reconciled, and its target never moves
// again — so only the next process's re-derivation can find it. A lost watermark
// *write* is a different case and needs no repair: see
// docs/adr/2026-08-03-stale-dependents-cursor.md.
func TestStaleDependentsSweepRepairsALostFindingAfterRestart(t *testing.T) {
	ctx := context.Background()
	store := newClientTestStore(t)
	spec := []byte(`{}`)
	dep, err := store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: spec})
	require.NoError(t, err)
	target, err := store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{Name: uniqueName(), Spec: spec})
	require.NoError(t, err)
	require.NoError(t, addEdge(ctx, store, dep.ID, target.ID, RelationDependsOn))

	sweeperOver(store).sweep(ctx)
	owed, err := store.ObjectsGet(ctx, dep.ID)
	require.NoError(t, err)
	require.EqualValues(t, 1, owed.ReconcileOwed, "the sweep recorded the finding")

	// Drained, then lost: the reconcile it dispatched never ran.
	require.NoError(t, store.ReconcileOwedDecrement(ctx, clientTestGK, dep.ID, 1))
	owed, err = store.ObjectsGet(ctx, dep.ID)
	require.NoError(t, err)
	require.Zero(t, owed.ReconcileOwed, "nothing durable names the dependent now")

	sweeperOver(store).sweep(ctx)

	owed, err = store.ObjectsGet(ctx, dep.ID)
	require.NoError(t, err)
	assert.EqualValues(t, 1, owed.ReconcileOwed, "the new process re-derives and finds it again")
}

// TestStaleDependentsSweepHoldsTheCursorOnListFailure pins the failure contract:
// a sweep that cannot read gives up on this pass, says so, and leaves the cursor
// where it was. Holding it is the repair inside a live process: advancing past a
// page that was never read leaves every dependent in it to the next restart,
// because this process re-derives nothing for a target that has gone quiet.
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

// nextTimer is the loop's whole timer policy: never push a pending deadline
// later, and stop what is armed once a pass reports nothing to come back for.
// The race it exists for is not reproducible on demand — a wake and a ready
// timer arriving together — so the rule is pinned here rather than in run.
func TestNextTimer(t *testing.T) {
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	armed := now.Add(time.Second)

	cases := []struct {
		name     string
		next     time.Duration
		armedFor time.Time
		want     timerAction
	}{
		{"idle stops a timer left armed", wakeIdle, armed, timerStop},
		{"idle with nothing armed stays idle", wakeIdle, time.Time{}, timerStop},
		{"nothing armed arms", time.Second, time.Time{}, timerArm},
		{"a sooner deadline wins", 100 * time.Millisecond, armed, timerArm},
		{"a later one leaves the pending timer alone", 2 * time.Second, armed, timerKeep},
		{"the same deadline changes nothing", time.Second, armed, timerKeep},
		{"a drain with no throttle arms immediately", 0, armed, timerArm},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, nextTimer(c.next, now, c.armedFor))
		})
	}
}
