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

// An event watch opened before its object exists waits for it rather than
// erroring. The kind check needs a row to read, and "not there yet" is ordinary
// for a watch opened ahead of the thing it is about.
func TestEventsWatchWaitsForAnObjectThatDoesNotExistYet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, cc := watchFixture(t)
	first := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	// The id the store will assign next: no row holds it yet, so the kind check
	// finds nothing on every tick.
	next := first.ID + 1
	ch, err := client.EventsWatch(ctx, next)
	require.NoError(t, err)
	waitClosed(t, chanAfter(store.metaRead, 2), "polls while the id is unassigned")

	later := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "b"})
	require.Equal(t, next, later.ID, "the store assigns ids in order")
	require.NoError(t, cc.EventsAdd(ctx, later.ID, EventSpec{Type: EventNormal, Reason: "Started"}))

	assert.Equal(t, "Started", recv(t, ch).Reason, "the stream picks the object up once it exists")
}

// An event watch is kind-scoped like the object watches: an unscoped log read
// would let another kind's id stream its events through this client.
func TestEventsWatchIsKindScoped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, bh, client, _ := watchFixture(t)
	other := GroupKind{Kind: "Other"}
	otherCC, err := Register(bh, other, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	foreign := mustCreate(t, ctx, NewClient[cSpec, cStatus](bh, other), "foreign", cSpec{Val: "foreign"})
	require.NoError(t, otherCC.EventsAdd(ctx, foreign.ID, EventSpec{Type: EventNormal, Reason: "Started"}))

	ch, err := client.EventsWatch(ctx, foreign.ID)
	require.NoError(t, err)
	waitClosed(t, chanAfter(store.metaRead, 1), "the scoping read against the foreign id")

	// The verdict is final, so the poll ends rather than ticking on: an id's
	// group and kind are fixed at insert and its id is never reused, so
	// "foreign" cannot become false, and both re-reading and waking to decide
	// not to would buy nothing. Use an object watch as the clock, since its
	// ticks are independent of this stream's.
	_, _, err = client.WatchList(ctx)
	require.NoError(t, err)
	waitClosed(t, chanAfter(store.polled, 3), "three ticks after the foreign id resolved")
	assert.Empty(t, store.metaRead, "a foreign id must be re-read no more than once")

	select {
	case ev := <-ch:
		t.Fatalf("another kind's log must not stream through this client, got %+v", ev)
	default:
	}

	// A stream whose poll has ended still belongs to its caller: it stays open
	// until the context goes, and then it closes.
	cancel()
	waitClosed(t, closedWhenDrained(ch), "the foreign stream to close on cancellation")
}

// Every read an event watch makes happens per tick, so any one of them failing
// costs a tick rather than the stream — the same contract the object watches keep.
func TestEventsWatchSurvivesReadFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, bh, client, cc := watchFixture(t)
	logger, buf := captureLogger(slog.LevelWarn)
	bh.logger = logger

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	// A run the gate can see, so the failing log read below is actually reached.
	require.NoError(t, cc.EventsAdd(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Probing"}))

	store.metaErr.Store(true) // the kind check fails first
	ch, err := client.EventsWatch(ctx, obj.ID)
	require.NoError(t, err)
	waitClosed(t, chanAfter(store.metaRead, 2), "a poll while the kind check fails")

	// Each handoff arms the next fault *before* clearing the current one, so no tick
	// can slip through a moment where every read succeeds: the log holds a run from
	// here on, so such a tick would list it and then block in the send while the test
	// is parked in waitClosed, and the stream would never tick again.
	store.markErr.Store(true) // then the gate read
	store.metaErr.Store(false)
	waitClosed(t, chanAfter(store.eventsMarked, 2), "a poll while the gate read fails")

	store.eventsErr.Store(true) // and then the log read
	store.markErr.Store(false)
	waitClosed(t, chanAfter(store.eventsFailed, 2), "a poll while the log read fails")
	store.eventsErr.Store(false)

	require.NoError(t, cc.EventsAdd(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Recovered"}))
	// A failed listing does not advance the cursor, so the run it missed is still
	// owed: both arrive, oldest first.
	assert.Equal(t, "Probing", recv(t, ch).Reason, "the run the failed listing missed is not lost")
	assert.Equal(t, "Recovered", recv(t, ch).Reason, "the stream outlived every failure")
	assert.Contains(t, buf.String(), "event watch poll failed", "the skipped polls are reported")
}

// A run that has not moved since it was reported is not sent again, even on a tick
// that does list. Without that comparison a listing forced by an event the watch
// filters out would re-deliver the whole window, and a subscriber could not tell a
// new observation from a re-listing of an old one.
func TestEventsWatchEmitsOnlyWhatChanged(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	probe := EventSpec{Category: "probe", Type: EventNormal, Reason: "Probing"}
	require.NoError(t, cc.EventsAdd(ctx, obj.ID, probe))

	ch, err := client.EventsWatch(ctx, obj.ID, WithEventCategory("probe"))
	require.NoError(t, err)
	require.Equal(t, "Probing", recv(t, ch).Reason)

	// The gate spans every category, so events in another one move it and force the
	// listing — which re-reads the unchanged "probe" run. If any of those re-sent it,
	// the receive below would return "Probing" again instead of the new run.
	for range 3 {
		require.NoError(t, cc.EventsAdd(ctx, obj.ID,
			EventSpec{Category: "other", Type: EventNormal, Reason: "Noise"}))
		waitClosed(t, chanAfter(store.eventsListed, 1), "a poll that re-lists the unchanged run")
	}

	require.NoError(t, cc.EventsAdd(ctx, obj.ID,
		EventSpec{Category: "probe", Type: EventNormal, Reason: "Connected"}))
	assert.Equal(t, "Connected", recv(t, ch).Reason, "only the new run is delivered")
}

// A quiet tick costs one scalar read: the log's high-water mark gates the listing,
// which is the whole cost of the watch when nothing is happening.
func TestEventsWatchSkipsTheListingWhileTheLogIsQuiet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	ch, err := client.EventsWatch(ctx, obj.ID)
	require.NoError(t, err)

	// An empty log reads as the zero mark the stream starts with, so not even the
	// first tick lists: there is nothing in it to report.
	waitClosed(t, chanAfter(store.eventsMarked, 3), "ticks over an empty log")
	assert.Empty(t, store.eventsListed, "an empty log is never listed")

	require.NoError(t, cc.EventsAdd(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Probing"}))
	require.Equal(t, "Probing", recv(t, ch).Reason)
	waitClosed(t, chanAfter(store.eventsListed, 1), "the listing the new run forced")

	// And the ticks after it go quiet again: the mark has not moved since that
	// listing, so nothing pays for another one.
	waitClosed(t, chanAfter(store.eventsMarked, 3), "quiet ticks after the run was delivered")
	assert.Empty(t, store.eventsListed, "a quiet tick reads the mark and stops")
}

// An event send is abandoned on cancellation like any other, so a subscriber that
// stops reading cannot strand the goroutine polling on its behalf.
func TestEventsWatchAbandonsASendOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store, _, client, cc := watchFixture(t)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	require.NoError(t, cc.EventsAdd(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Probing"}))

	ch, err := client.EventsWatch(ctx, obj.ID)
	require.NoError(t, err)

	// Nobody reads ch. Waiting for the log read to return puts the cancellation
	// after it and before the send, which is the only place left to observe it.
	waitClosed(t, chanAfter(store.eventsListed, 1), "the log read that found the run")
	cancel()

	waitClosed(t, closedWhenDrained(ch), "the stream to close on cancellation")
}

// The watch surface falls back to the default interval for a Beehive assembled
// field by field, as tests do. New itself cannot produce one: withWatchPollInterval
// rejects a non-positive value.
func TestWatchPollFallsBackToTheDefault(t *testing.T) {
	assert.Equal(t, defaultWatchPollInterval, (&Beehive{}).watchPoll(),
		"an unset interval reads as the default rather than as disabled")

	bh := newTestBeehive(t, &fakeStore{}, withWatchPollInterval(fastTick))
	assert.Equal(t, fastTick, bh.watchPoll(), "a configured interval is used as given")
}
