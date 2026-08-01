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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/amorey/beehive/internal/storeapi"
)

// flakyListStore fails the first n ObjectsList calls and then succeeds, so a test
// can prove a poll failure costs one tick rather than the whole stream.
type flakyListStore struct {
	Store
	failures atomic.Int64
}

func (s *flakyListStore) ObjectsList(ctx context.Context, gk GroupKind) ([]*RawObject, error) {
	if s.failures.Add(-1) >= 0 {
		return nil, errBoom
	}
	return s.Store.ObjectsList(ctx, gk)
}

// A poll that fails is skipped, not fatal. Tearing the stream down would turn a
// transient store error into a subscriber that silently stops receiving — the
// failure mode a level-triggered surface exists to avoid, since there is no
// resubscribe for a consumer to notice it needs.
func TestWatchPollFailureCostsOneTickNotTheStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &flakyListStore{Store: newClientTestStore(t)}
	logger, buf := captureLogger(slog.LevelWarn)
	bh, err := New(store, fast(WithLogger(logger))...)
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)
	require.Equal(t, obj.ID, recv(t, ch).Object.ID, "the snapshot reports the object")

	// The next two listings fail. Only a tick that has something to list reaches
	// them, so give it a change to find.
	store.failures.Store(2)
	_, err = client.UpdateByID(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)

	ev := recv(t, ch)
	assert.Equal(t, Modified, ev.Type, "the stream survives the failed polls and reports the change")
	assert.Equal(t, "b", ev.Object.Spec.Val)
	assert.Contains(t, buf.String(), "watch poll failed", "the skipped polls are reported")
}

// An object that has not changed since it was reported emits nothing. This is
// what makes the steady state silent: without the version comparison every tick
// would re-send the whole kind, and a subscriber could not tell a change from a
// heartbeat.
func TestWatchEmitsNothingWhileNothingChanges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newClientTestStore(t)
	bh, err := New(store, fast()...)
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)
	require.Equal(t, Added, recv(t, ch).Type)

	// A real change is the barrier. Many ticks pass while the object is untouched;
	// if any of them re-sent it, that Modified would arrive carrying the old spec
	// and this assertion would see it instead of the new one.
	_, err = client.UpdateByID(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)

	ev := recv(t, ch)
	assert.Equal(t, Modified, ev.Type)
	assert.Equal(t, "b", ev.Object.Spec.Val, "the only delivery after the snapshot is the real change")
}

// A physical removal is derived from the row's absence, not from a tombstone the
// store kept: the delete draws no version, so nothing in the write log records it.
// The change carries the object's last known state, which is its final state —
// nothing was written after it.
func TestWatchDerivesDeletedFromAbsence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newClientTestStore(t)
	bh, err := New(store, fast()...)
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "gone"})

	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)
	require.Equal(t, Added, recv(t, ch).Type)

	require.NoError(t, store.ObjectsDelete(ctx, obj.ID))

	ev := recv(t, ch)
	assert.Equal(t, Deleted, ev.Type)
	require.NotNil(t, ev.Object)
	assert.Equal(t, obj.ID, ev.Object.ID)
	assert.Equal(t, "gone", ev.Object.Spec.Val, "the tombstone carries the row's last known state")
}

// A single-object watch is kind-scoped like Get: another kind's id reads as
// absent rather than streaming that kind's rows through this client, where they
// would be decoded as this Spec.
func TestWatchSingleObjectIsKindScoped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newClientTestStore(t)
	bh, err := New(store, fast()...)
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	other := GroupKind{Kind: "Other"}
	_, err = Register(bh, other, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)

	foreign := mustCreate(t, ctx, NewClient[cSpec, cStatus](bh, other), "foreign", cSpec{Val: "foreign"})

	ch, err := NewClient[cSpec, cStatus](bh, clientTestGK).ObjectsWatch(ctx, foreign.ID)
	require.NoError(t, err)

	// The barrier is this client's own object: it is created after the foreign one,
	// so anything the foreign id produced would have to arrive first.
	mine := mustCreate(t, ctx, NewClient[cSpec, cStatus](bh, clientTestGK), "mine", cSpec{Val: "mine"})
	mineCh, err := NewClient[cSpec, cStatus](bh, clientTestGK).ObjectsWatchList(ctx)
	require.NoError(t, err)
	require.Equal(t, mine.ID, recv(t, mineCh).Object.ID)

	select {
	case ev := <-ch:
		t.Fatalf("a foreign id must stream nothing, got %+v", ev)
	default:
	}
}

// SchedulesWatch is a gauge, so it repeats nothing: the same state on two
// consecutive polls is one delivery. A subscriber reads it to learn when the next
// requeue is, and a stream that re-sent an unchanged schedule every tick would be a
// heartbeat rather than a schedule.
//
// An object watch over the same Beehive is the clock. It polls the shared interval
// and signals the store on every tick, so a token proves another poll came round —
// which is what lets this assert on deliveries that must *not* happen without
// timing anything itself.
func TestSchedulesWatchEmitsOnlyOnChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, bh, client, _ := watchFixture(t)
	r, ok := bh.reconcilerFor(clientTestGK)
	require.True(t, ok)
	_, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)

	ch, err := client.SchedulesWatch(ctx, 1)
	require.NoError(t, err)

	// The zero Schedule is a real gauge value, not an absence, so it is delivered.
	assert.True(t, recv(t, ch).NextRequeueAt.IsZero(), "an unscheduled id reads as the zero Schedule")

	// Three more polls pass with the id still unscheduled. Nothing may arrive.
	waitClosed(t, chanAfter(store.polled, 3), "three polls over the unscheduled id")
	select {
	case sc := <-ch:
		t.Fatalf("an unchanged schedule must not be re-sent, got %+v", sc)
	default:
	}

	// Queued for immediate dispatch: due now, reported once. The queue answers this
	// state with the time the id became due rather than with time.Now(), which is
	// what keeps "still queued" a repeated value instead of a moving one — otherwise
	// an id waiting behind a slow reconcile emits on every tick forever.
	r.work.add(1)
	due := recv(t, ch)
	require.False(t, due.NextRequeueAt.IsZero(), "a queued id carries a due-now time")
	assert.False(t, due.NextRequeueAt.After(time.Now()), "due-now is not in the future")

	waitClosed(t, chanAfter(store.polled, 3), "three polls while it sits queued")
	select {
	case sc := <-ch:
		t.Fatalf("a queued id must not re-report on every tick, got %+v", sc)
	default:
	}

	// Dispatch clears the queued state, which is a change like any other.
	drainQueue(r.work)
	assert.True(t, recv(t, ch).NextRequeueAt.IsZero(), "dispatch emits the unscheduled zero")
}

// pollProbeStore signals each time the watch surface reads, so a test can wait for
// a poll it expects rather than assuming one has happened. Errors are injected per
// call site, which is what lets a test drive one failure branch at a time.
type pollProbeStore struct {
	Store
	// polled fires once per object-watch tick. It hangs off the store-wide cursor
	// read because that is the one call every tick makes: the write-log probe and the
	// listings past it are exactly what a quiet tick skips.
	polled chan struct{}
	// listed fires after a listing returns, so a test can cancel knowing the read
	// already succeeded and the goroutine is on its way to the send.
	listed chan struct{}
	// eventsListed is the event watch's equivalent of listed; metaRead, eventsMarked
	// and eventsFailed cover the reads a tick makes before it gets there. A quiet
	// tick stops at eventsMarked, which is what makes it the event watch's clock.
	eventsListed chan struct{}
	eventsMarked chan struct{}
	metaRead     chan struct{}
	eventsFailed chan struct{}
	listErr      atomic.Bool
	listIDsErr   atomic.Bool
	getErr       atomic.Bool
	eventsErr    atomic.Bool
	markErr      atomic.Bool
	metaErr      atomic.Bool
}

func (s *pollProbeStore) ObjectWritesMaxVersion(ctx context.Context) (int64, error) {
	at, err := s.Store.ObjectWritesMaxVersion(ctx)
	probeSignal(s.polled)
	return at, err
}

// ObjectsList signals *after* the read returns, which is the seam the cancellation
// tests need: past it the only thing left that can observe a cancelled context is
// the send itself.
func (s *pollProbeStore) ObjectsList(ctx context.Context, gk GroupKind) ([]*RawObject, error) {
	if s.listErr.Load() {
		return nil, errBoom
	}
	out, err := s.Store.ObjectsList(ctx, gk)
	probeSignal(s.listed)
	return out, err
}

func (s *pollProbeStore) ObjectsListIDs(ctx context.Context, gk GroupKind) ([]ObjectID, error) {
	if s.listIDsErr.Load() {
		return nil, errBoom
	}
	return s.Store.ObjectsListIDs(ctx, gk)
}

func (s *pollProbeStore) ObjectsGet(ctx context.Context, id ObjectID) (*RawObject, error) {
	if s.getErr.Load() {
		return nil, errBoom
	}
	return s.Store.ObjectsGet(ctx, id)
}

// ObjectsGetMeta is the event watch's per-tick read, so it signals on every call —
// error or not — which is what a test waiting out a failing phase needs.
func (s *pollProbeStore) ObjectsGetMeta(ctx context.Context, id ObjectID) (*RawObject, error) {
	defer probeSignal(s.metaRead)
	if s.metaErr.Load() {
		return nil, errBoom
	}
	return s.Store.ObjectsGetMeta(ctx, id)
}

// EventsMaxVersion is the event watch's gate read, so it signals on every call —
// error or not — the way ObjectsGetMeta does. It is the only store read a quiet
// tick makes, so a test counting it is counting ticks.
func (s *pollProbeStore) EventsMaxVersion(ctx context.Context, id ObjectID) (int64, error) {
	defer probeSignal(s.eventsMarked)
	if s.markErr.Load() {
		return 0, errBoom
	}
	return s.Store.EventsMaxVersion(ctx, id)
}

func (s *pollProbeStore) EventsList(ctx context.Context, id ObjectID, q storeapi.EventQuery) ([]RawEvent, error) {
	if s.eventsErr.Load() {
		probeSignal(s.eventsFailed)
		return nil, errBoom
	}
	out, err := s.Store.EventsList(ctx, id, q)
	probeSignal(s.eventsListed)
	return out, err
}

// watchFixture wires a Beehive with one registered kind over a probe store, which
// is the shape every test below needs. The ControllerClient comes back because the
// event watches need something that can write to a log.
func watchFixture(t *testing.T) (*pollProbeStore, *Beehive, Client[cSpec, cStatus], ControllerClient[cStatus]) {
	t.Helper()
	store := &pollProbeStore{
		Store:        newClientTestStore(t),
		polled:       make(chan struct{}, 256),
		listed:       make(chan struct{}, 256),
		eventsListed: make(chan struct{}, 256),
		eventsMarked: make(chan struct{}, 256),
		metaRead:     make(chan struct{}, 256),
		eventsFailed: make(chan struct{}, 256),
	}
	bh, err := New(store, fast()...)
	require.NoError(t, err)
	cc, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	return store, bh, NewClient[cSpec, cStatus](bh, clientTestGK), cc
}

// A single-object watch survives a store error the same way the list watch does.
// The read is per-tick, so a failure costs that tick; ending the stream would leave
// the subscriber with no way to learn it should resubscribe.
func TestWatchSingleObjectSurvivesAReadFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, bh, client, _ := watchFixture(t)
	logger, buf := captureLogger(slog.LevelWarn)
	bh.logger = logger

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	ch, err := client.ObjectsWatch(ctx, obj.ID)
	require.NoError(t, err)
	require.Equal(t, Added, recv(t, ch).Type)

	// A change to find, and a read that fails while it tries. Wait for ticks that
	// come *after* the failure is armed, so the recovery below is the stream
	// outliving a failure rather than never meeting one.
	store.getErr.Store(true)
	_, err = client.UpdateByID(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)
	drainProbe(store.polled)
	waitClosed(t, chanAfter(store.polled, 2), "polls while the read fails")
	store.getErr.Store(false)

	ev := recv(t, ch)
	assert.Equal(t, Modified, ev.Type)
	assert.Equal(t, "b", ev.Object.Spec.Val)
	assert.Contains(t, buf.String(), "watch poll failed", "the skipped poll is reported")
}

// The cheap half of the poll can fail too. It runs only when the write cursor has
// not moved, so a failure there is the same one-tick cost as a failed listing —
// not a reason to end the stream.
func TestWatchSurvivesADeleteCheckFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, bh, client, _ := watchFixture(t)
	logger, buf := captureLogger(slog.LevelWarn)
	bh.logger = logger

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)
	require.Equal(t, Added, recv(t, ch).Type)

	// With the object reported and no further writes, the cursor stops moving, so
	// every tick from here consults the id listing — which now fails.
	store.listIDsErr.Store(true)
	drainProbe(store.polled)
	waitClosed(t, chanAfter(store.polled, 2), "polls while the delete check fails")
	store.listIDsErr.Store(false)

	// A real change proves the stream is still live and still diffing.
	_, err = client.UpdateByID(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)
	assert.Equal(t, "b", recv(t, ch).Object.Spec.Val)
	assert.Contains(t, buf.String(), "watch poll failed")
}

// A watch over an empty kind reports nothing and reads almost nothing: with no
// object ever reported there is no id set to compare, so the delete check answers
// without touching the store at all.
func TestWatchOverAnEmptyKindStaysQuiet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, _ := watchFixture(t)
	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)
	waitClosed(t, chanAfter(store.polled, 3), "three polls over the empty kind")

	select {
	case ev := <-ch:
		t.Fatalf("an empty kind must stream nothing, got %+v", ev)
	default:
	}
}

// A subscriber that stops reading must not wedge its poll goroutine. Cancelling is
// the only way out for a send nobody is receiving, so the stream has to abandon it
// and close rather than hold the value forever.
func TestWatchAbandonsASendWhenTheSubscriberGoesAway(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store, _, client, _ := watchFixture(t)

	mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)

	// Nobody reads ch, so the poll goroutine parks in the send. Waiting for the
	// listing that produced the change is what puts the cancellation after the read
	// and before the send, which is the only place left that can observe it.
	waitClosed(t, chanAfter(store.listed, 1), "the listing that found the object")
	cancel()

	waitClosed(t, closedWhenDrained(ch), "the stream to close on cancellation")
}

// The same holds for a tombstone: a Deleted change is a send like any other, so a
// subscriber that has gone away must not strand the goroutine that derived it.
func TestWatchAbandonsATombstoneSendOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store, _, client, _ := watchFixture(t)

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)
	require.Equal(t, Added, recv(t, ch).Type)

	// Remove the row outright and stop reading: the next poll finds the id set has
	// shrunk, lists, derives the tombstone, and parks in the send. Draining first is
	// what makes the wait below answer to that listing rather than to the snapshot's.
	drainProbe(store.listed)
	require.NoError(t, store.ObjectsDelete(ctx, obj.ID))
	waitClosed(t, chanAfter(store.listed, 1), "the listing that observes the removal")
	cancel()

	waitClosed(t, closedWhenDrained(ch), "the stream to close on cancellation")
}

// A row that never decoded has no body to tombstone, so its removal is silent. The
// alternative would be a Deleted change carrying a zero-valued object the
// subscriber was never shown in the first place.
func TestWatchDoesNotTombstoneARowItCouldNeverDecode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, _ := watchFixture(t)
	poison, err := store.ObjectsCreate(ctx, clientTestGK, ObjectsCreateInput{
		Name: uniqueName(),
		Spec: []byte(`not json`),
	})
	require.NoError(t, err)

	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)
	waitClosed(t, chanAfter(store.polled, 2), "the poll that quarantines the poison row")

	require.NoError(t, store.ObjectsDelete(ctx, poison.ID))

	// A good object created after the removal is the barrier: it can only arrive
	// after the poll that dropped the poison row from the stream's own bookkeeping.
	good := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "good"})

	ev := recv(t, ch)
	assert.Equal(t, Added, ev.Type, "the poison row's removal produced no tombstone")
	assert.Equal(t, good.ID, ev.Object.ID)
}

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

	// The verdict is latched, so later ticks re-read nothing. An id's group and kind
	// are fixed at insert and its id is never reused, so "foreign" cannot become
	// false — re-reading would cost one row per tick, forever, to learn the same
	// thing. Use an object watch as the clock: its ticks are independent of this
	// stream's, so several of them passing proves the event watch also ticked.
	_, err = client.ObjectsWatchList(ctx)
	require.NoError(t, err)
	waitClosed(t, chanAfter(store.polled, 3), "three ticks after the foreign id resolved")
	assert.Empty(t, store.metaRead, "a foreign id must be re-read no more than once")

	select {
	case ev := <-ch:
		t.Fatalf("another kind's log must not stream through this client, got %+v", ev)
	default:
	}
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

// TestWatchStaysQuietThroughEventWrites pins what the quiet-tick gate asks. The
// event log draws its resource_version from the same sequence the objects do, so a
// gate that compared the sequence itself would be defeated by a single
// EventsAdd anywhere in the store — and a controller that records an event per
// reconcile, the shape examples/events encourages, would defeat it permanently,
// turning every subscriber into a full blob-bearing listing per tick.
func TestWatchStaysQuietThroughEventWrites(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, cc := watchFixture(t)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)
	require.Equal(t, Added, recv(t, ch).Type, "the create is reported once")
	drainProbe(store.listed)

	// An event write bumps the shared sequence and no objects row.
	require.NoError(t, cc.EventsAdd(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Probed"}))

	waitClosed(t, chanAfter(store.polled, 3), "three polls after the event write")
	select {
	case <-store.listed:
		t.Fatal("an event write must not make the object watch pay for a listing")
	default:
	}
	select {
	case ev := <-ch:
		t.Fatalf("an event write is not an object change, got %+v", ev)
	default:
	}
}

// TestWatchSingleObjectFindsADeleteWithoutListingTheKind pins the cheap half of a
// single-object poll. The watch's liveness probe is scoped to the one id it
// tracks, not to the kind: asking for every id of the kind would make the cheap
// half the expensive half and scale it with the kind rather than with the watch.
//
// The removal under test draws no version and does not move the write log's
// high-water mark — another object was written after the deletion mark — so the
// quiet path is the only thing that can notice it. The kind-wide listing is wired
// to fail throughout, so a probe that reached for it could not answer at all.
func TestWatchSingleObjectFindsADeleteWithoutListingTheKind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The fixture starts nothing, so every step below is the test's own: no sweeper
	// and no reconcile loop can collect the row out from under the ordering.
	store, bh, client, _ := watchFixture(t)
	store.listIDsErr.Store(true)
	watched := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "watched"})
	newer := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "newer"})

	ch, err := client.ObjectsWatch(ctx, watched.ID)
	require.NoError(t, err)
	require.Equal(t, Added, recv(t, ch).Type)

	// The deletion mark is an ordinary write, so it arrives as a Modified.
	require.NoError(t, client.DeleteByID(ctx, watched.ID))
	require.Equal(t, Modified, recv(t, ch).Type)

	// Put another object's write above that mark, and let the stream take it in, so
	// the collect below cannot move the high-water mark it compares against.
	_, err = client.UpdateByID(ctx, newer.ID, cSpec{Val: "newest"})
	require.NoError(t, err)
	// Wait for a *quiet* tick, not merely for ticks. The liveness probe only runs on
	// a tick that found the high-water mark unmoved, so a token here proves the
	// cursor has caught up to that write — and until it has, the collect below would
	// be found by an ordinary listing rather than by the probe.
	drainProbe(store.metaRead)
	waitClosed(t, chanAfter(store.metaRead, 2), "a quiet poll after the newer write")

	gone, err := bh.gcCollect(ctx, watched.ID)
	require.NoError(t, err)
	require.True(t, gone, "the row is finalizer-free, so the collect removes it")

	ev := recv(t, ch)
	assert.Equal(t, Deleted, ev.Type)
	assert.Equal(t, watched.ID, ev.Object.ID)
	assert.Equal(t, "watched", ev.Object.Spec.Val, "the tombstone carries the last known state")
}

// The liveness probe can fail like any other read, and costs the same one tick:
// ending the stream would leave the subscriber with no way to learn it should
// resubscribe.
func TestWatchSingleObjectSurvivesALivenessProbeFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, bh, client, _ := watchFixture(t)
	logger, buf := captureLogger(slog.LevelWarn)
	bh.logger = logger

	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})
	ch, err := client.ObjectsWatch(ctx, obj.ID)
	require.NoError(t, err)
	require.Equal(t, Added, recv(t, ch).Type)

	// With the object reported and no further writes, every tick from here consults
	// the liveness probe — which now fails.
	store.metaErr.Store(true)
	drainProbe(store.polled)
	waitClosed(t, chanAfter(store.polled, 2), "polls while the liveness probe fails")
	store.metaErr.Store(false)

	// A real change proves the stream is still live and still diffing.
	_, err = client.UpdateByID(ctx, obj.ID, cSpec{Val: "b"})
	require.NoError(t, err)
	assert.Equal(t, "b", recv(t, ch).Object.Spec.Val)
	assert.Contains(t, buf.String(), "watch poll failed")
}

// TestWatchTakesItsSnapshotBeforeReturning pins the guarantee that makes
// "subscribe, then act" safe: the stream reads current state before the
// subscribing call returns, so a change the caller makes next is measured against
// a snapshot that already exists.
//
// Without it the snapshot is taken by the first tick, and everything between
// subscribing and that tick is invisible — an object created and collected in the
// window is never reported at all, so a caller waiting for its Deleted waits
// forever. The poll interval here is an hour: nothing but the synchronous first
// read can deliver anything.
func TestWatchTakesItsSnapshotBeforeReturning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bh, err := New(newClientTestStore(t), withWatchPollInterval(time.Hour))
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)
	obj := mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	t.Run("list watch", func(t *testing.T) {
		ch, err := client.ObjectsWatchList(ctx)
		require.NoError(t, err)
		ev := recv(t, ch)
		assert.Equal(t, Added, ev.Type)
		assert.Equal(t, obj.ID, ev.Object.ID)
	})

	t.Run("single-object watch", func(t *testing.T) {
		ch, err := client.ObjectsWatch(ctx, obj.ID)
		require.NoError(t, err)
		assert.Equal(t, Added, recv(t, ch).Type)
	})
}

// TestWatchReportsAFailedFirstRead pins the other half of the subscribe-then-act
// guarantee: a stream is handed back only when it holds a snapshot. A failed first
// read returns an error instead, because the alternative is a watch whose guarantee
// is quietly void — it would hold no state to compare against, so an object deleted
// next would never be reported, and the caller would wait for a tombstone that
// cannot come. Every *later* failure costs one tick, since the last good poll's
// state is still there.
func TestWatchReportsAFailedFirstRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, _ := watchFixture(t)
	mustCreate(t, ctx, client, uniqueName(), cSpec{Val: "a"})

	store.listErr.Store(true)
	ch, err := client.ObjectsWatchList(ctx)
	require.ErrorIs(t, err, errBoom, "the caller learns the snapshot failed")
	assert.Nil(t, ch, "and gets no stream to wait on")
	assert.Contains(t, err.Error(), "initial read failed")

	// It is the read that failed, not the subscription: with the store answering
	// again, subscribing works.
	store.listErr.Store(false)
	ch, err = client.ObjectsWatchList(ctx)
	require.NoError(t, err)
	assert.Equal(t, Added, recv(t, ch).Type)
}

// A context already cancelled at subscribe is the same story told by the store:
// the snapshot read fails, so there is no stream to hand back.
func TestWatchOnACancelledContextDoesNotSubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, client, _ := watchFixture(t)
	_, err := client.ObjectsWatchList(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestPollFailedSeparatesShutdownFromFailure pins the two answers a failed read
// gets, directly rather than through a stream. Reaching them from a live watch
// means cancelling a context while a read is in flight, which is a race the test
// would sometimes lose — and a coverage gate turns a lost race into a red build
// with no defect behind it.
//
// The distinction itself is the point: a store error is a fault worth reporting
// and worth one more tick, while a cancelled context is this stream shutting down
// and neither. Warning there would put a line in the log on every clean
// unsubscribe.
func TestPollFailedSeparatesShutdownFromFailure(t *testing.T) {
	logger, buf := captureLogger(slog.LevelWarn)
	c := &clientImpl[cSpec, cStatus]{bh: &Beehive{logger: logger}, gk: clientTestGK}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, c.pollFailed(ctx, "watch", errBoom), "a cancelled read ends the loop")
	assert.Empty(t, buf.String(), "shutdown is not a fault to report")

	assert.True(t, c.pollFailed(context.Background(), "watch", errBoom),
		"a store error costs the tick, not the stream")
	assert.Contains(t, buf.String(), "watch poll failed")
	assert.Contains(t, buf.String(), errBoom.Error())
}

// The watch surface falls back to the default interval for a Beehive assembled
// field by field, as tests do. New itself cannot produce one: withWatchPollInterval
// rejects a non-positive value.
func TestWatchPollFallsBackToTheDefault(t *testing.T) {
	assert.Equal(t, defaultWatchPollInterval, (&Beehive{}).watchPoll(),
		"an unset interval reads as the default rather than as disabled")

	bh, err := New(&fakeStore{}, withWatchPollInterval(fastTick))
	require.NoError(t, err)
	assert.Equal(t, fastTick, bh.watchPoll(), "a configured interval is used as given")
}

// sendOrDone reports the send it could not make. Without it a subscriber that
// stopped reading would wedge its own poll goroutine, which then never observes
// the cancellation that was meant to release it.
func TestSendOrDoneReportsACancelledSend(t *testing.T) {
	out := make(chan int) // unbuffered, and nobody is reading

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, sendOrDone(ctx, out, 1), "a cancelled context abandons the send")

	// A reader waiting is the other half: the send lands and is reported as landed.
	got := make(chan int, 1)
	go func() { got <- <-out }()
	assert.True(t, sendOrDone(context.Background(), out, 7), "a send with a reader lands")
	assert.Equal(t, 7, <-got)
}

// Stream tests. These go through SchedulesWatch and never Peek: the stream
// goroutine is the receiver's consumer, so a Peek racing it proves nothing.
//
// Every one of them runs with the poll turned off, so a value a test observes is
// provably the hub's. Without that they would pass on the poll alone and say
// nothing about push.

// pushOnlyClient builds a Beehive whose watch poll is set an hour out, so no
// tick can fire inside a test and the only path from the queue to a stream is
// the hub. The option refuses a non-positive interval, so this is how the poll
// is taken out of the picture.
func pushOnlyClient(t *testing.T) (context.Context, *Beehive, Client[cSpec, cStatus], *reconciler) {
	t.Helper()
	bh, err := New(newClientTestStore(t), withWatchPollInterval(time.Hour))
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	r, ok := bh.reconcilerFor(clientTestGK)
	require.True(t, ok)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx, bh, NewClient[cSpec, cStatus](bh, clientTestGK), r
}

// The snapshot is read at subscribe and delivered first, so a subscriber never
// waits for a change to learn the current schedule.
func TestScheduleStreamDeliversTheSnapshotFirst(t *testing.T) {
	ctx, _, client, r := pushOnlyClient(t)
	r.work.addAfter(1, time.Hour)

	ch, err := client.SchedulesWatch(ctx, 1)
	require.NoError(t, err)

	got := recv(t, ch)
	assert.True(t, got.NextRequeueAt.After(time.Now().Add(time.Minute)), "the pending alarm, got %+v", got)
}

// The zero Schedule is a real gauge value rather than an absence.
func TestScheduleStreamDeliversTheZeroSnapshot(t *testing.T) {
	ctx, _, client, _ := pushOnlyClient(t)

	ch, err := client.SchedulesWatch(ctx, 1)
	require.NoError(t, err)

	assert.True(t, recv(t, ch).NextRequeueAt.IsZero(), "an unscheduled id reads as the zero Schedule")
}

// This is the point of the whole change: a move reaches a subscriber with no
// tick, and here with no poll in the process at all.
func TestScheduleStreamDeliversWithoutATick(t *testing.T) {
	ctx, _, client, r := pushOnlyClient(t)
	ch, err := client.SchedulesWatch(ctx, 1)
	require.NoError(t, err)
	require.True(t, recv(t, ch).NextRequeueAt.IsZero())

	r.work.add(1)

	got := recv(t, ch)
	assert.False(t, got.NextRequeueAt.IsZero(), "the queued id must arrive without a tick")
	assert.False(t, got.NextRequeueAt.After(time.Now()), "due now")
}

// The delivery-side twin of the bus-boundary stale-send test. Neither alone
// shows the two halves are wired together.
func TestScheduleStreamNeverShowsAStaleValue(t *testing.T) {
	ctx, _, client, r := pushOnlyClient(t)
	ch, err := client.SchedulesWatch(ctx, 1)
	require.NoError(t, err)
	require.True(t, recv(t, ch).NextRequeueAt.IsZero())

	newer := stamped{Schedule: Schedule{NextRequeueAt: time.Now().Add(time.Hour)}, Seq: 100}
	older := stamped{Schedule: Schedule{NextRequeueAt: time.Now().Add(time.Minute)}, Seq: 99}
	require.NoError(t, r.work.schedules.Send(1, newer))
	require.NoError(t, r.work.schedules.Send(1, older))

	assert.Equal(t, newer.Schedule.NextRequeueAt, recv(t, ch).NextRequeueAt)
	assertQuiet(t, ch, "the older value must never be delivered")
}

// A gauge repeats nothing. The queue can move away and back while nobody reads,
// and the stream would otherwise report the same value twice.
func TestScheduleStreamRepeatsNothing(t *testing.T) {
	ctx, _, client, r := pushOnlyClient(t)
	ch, err := client.SchedulesWatch(ctx, 1)
	require.NoError(t, err)
	require.True(t, recv(t, ch).NextRequeueAt.IsZero())

	r.work.add(1)
	require.False(t, recv(t, ch).NextRequeueAt.IsZero())

	// A second add moves nothing: the id is already queued.
	r.work.add(1)
	assertQuiet(t, ch, "an unchanged gauge must not be re-sent")
}

// requeueNow clears an alarm and queues the id in one critical section, so the
// intermediate "nothing scheduled" never reaches a subscriber.
func TestScheduleStreamHidesTheRequeueNowIntermediate(t *testing.T) {
	ctx, _, client, r := pushOnlyClient(t)
	r.work.addAfter(1, time.Hour)
	ch, err := client.SchedulesWatch(ctx, 1)
	require.NoError(t, err)
	require.False(t, recv(t, ch).NextRequeueAt.IsZero(), "snapshot: the pending alarm")

	r.work.requeueNow(1)

	got := recv(t, ch)
	assert.False(t, got.NextRequeueAt.After(time.Now()), "due now, not the cancelled future time")
	assertQuiet(t, ch, "no phantom deschedule")
}

// Two subscribers on one id both receive, and one that stops reading does not
// delay the other.
func TestScheduleStreamFansOut(t *testing.T) {
	ctx, _, client, r := pushOnlyClient(t)
	first, err := client.SchedulesWatch(ctx, 1)
	require.NoError(t, err)
	second, err := client.SchedulesWatch(ctx, 1)
	require.NoError(t, err)
	require.True(t, recv(t, first).NextRequeueAt.IsZero())
	require.True(t, recv(t, second).NextRequeueAt.IsZero())

	r.work.add(1)

	assert.False(t, recv(t, first).NextRequeueAt.IsZero())
	assert.False(t, recv(t, second).NextRequeueAt.IsZero())
}

// A stream watching one id is untouched by another id's moves.
func TestScheduleStreamIgnoresAnotherID(t *testing.T) {
	ctx, _, client, r := pushOnlyClient(t)
	ch, err := client.SchedulesWatch(ctx, 1)
	require.NoError(t, err)
	require.True(t, recv(t, ch).NextRequeueAt.IsZero())

	r.work.add(2)
	assertQuiet(t, ch, "another id's move must not reach this stream")

	r.work.add(1)
	assert.False(t, recv(t, ch).NextRequeueAt.IsZero(), "its own id must reach it")
}

// assertQuiet fails if anything arrives on ch before a short grace period. It is
// the one place a duration appears in these tests: proving a *negative* about a
// push path has no signal to wait on.
func assertQuiet(t *testing.T, ch <-chan Schedule, msg string) {
	t.Helper()
	select {
	case got, open := <-ch:
		if open {
			t.Fatalf("%s: got %+v", msg, got)
		}
		t.Fatalf("%s: stream closed", msg)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestScheduleStreamMakesNoPeriodicRead pins that the poll is gone: a stream on
// a quiet queue reports its snapshot and then nothing, even though the watch
// poll interval is short enough that a retained poll would have ticked many
// times.
func TestScheduleStreamMakesNoPeriodicRead(t *testing.T) {
	bh, err := New(newClientTestStore(t), fast()...)
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	ch, err := client.SchedulesWatch(ctx, 1)
	require.NoError(t, err)
	require.True(t, recv(t, ch).NextRequeueAt.IsZero(), "the snapshot")

	assertQuiet(t, ch, "a quiet queue must produce nothing without a move")
}

// Shutdown delivers the final value and then ends the stream. Both halves: a
// test that only asserted the end would pass with the final value dropped.
//
// This is also the tripwire for Hub.Close. Reintroduce it and the final value
// races the teardown, so this test goes flaky rather than red — run it under
// -race in a loop when the shutdown path changes.
func TestScheduleStreamShutdownDeliversThenEnds(t *testing.T) {
	ctx, bh, client, r := pushOnlyClient(t)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	r.work.addAfter(1, time.Hour)

	ch, err := client.SchedulesWatch(ctx, 1)
	require.NoError(t, err)
	require.False(t, recv(t, ch).NextRequeueAt.IsZero(), "the pending alarm")

	require.NoError(t, stop(context.Background()))

	assert.True(t, recv(t, ch).NextRequeueAt.IsZero(), "the final value: nothing scheduled")
	select {
	case _, open := <-ch:
		assert.False(t, open, "the stream ends after the final value")
	case <-time.After(testTimeout):
		t.Fatal("the stream must end when the beehive stops")
	}
}

// A publish that races the sender close gets ErrClosed. Client.Requeue is
// public and reaches the queue from a user goroutine at any time, so beehive
// cannot promise the teardown is quiesced. The publish must not panic.
//
// add is the route a user takes: the queue is stopped, so the add itself is a
// no-op, but a queue stopped *between* the gauge move and the Send is the real
// shape and reaches the same closed sender.
func TestSchedulePublishAfterCloseIsNotFatal(t *testing.T) {
	ctx, bh, _, r := pushOnlyClient(t)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	require.NoError(t, stop(context.Background()))

	assert.NotPanics(t, func() { r.work.add(1) })
}

// The stream compares each delivered value against the last it reported, and
// this is the case that comparison exists for: the gauge moved away and back
// while nobody was reading, so the bus coalesced the pair and hands back a value
// the subscriber already has.
//
// TestScheduleStreamRepeatsNothing does not reach it. There the gauge suppresses
// the duplicate before it ever reaches the bus, so the stream never sees one.
// Here the publish is made directly, which is also what a coarse clock produces:
// two adds inside one tick stamp the same due-now time.
func TestScheduleStreamDropsACoalescedRepeat(t *testing.T) {
	ctx, _, client, r := pushOnlyClient(t)

	ch, err := client.SchedulesWatch(ctx, 1)
	require.NoError(t, err)

	// Published before the snapshot is read, so it lands in the receiver's slot
	// while the stream is still blocked delivering that snapshot. Same Schedule
	// as the seed, higher Seq, so Accept takes it.
	require.NoError(t, r.work.schedules.Send(1, stamped{Seq: 100}))

	assert.True(t, recv(t, ch).NextRequeueAt.IsZero(), "the snapshot")
	assertQuiet(t, ch, "a value equal to the last reported must not be re-sent")

	// The stream is still live and still delivers a change. Published the same
	// way: an injected Seq sits above anything the gauge will produce, so a real
	// enqueue would now be rejected by Accept — which is a property of this test's
	// injection, not of the queue.
	due := Schedule{NextRequeueAt: time.Now()}
	require.NoError(t, r.work.schedules.Send(1, stamped{Schedule: due, Seq: 101}))
	assert.Equal(t, due, recv(t, ch))
}

// A subscriber that cancels before reading its snapshot ends the stream rather
// than parking the goroutine on a send nobody will take.
func TestScheduleStreamCancelBeforeTheSnapshotIsRead(t *testing.T) {
	_, _, client, _ := pushOnlyClient(t)
	ctx, cancel := context.WithCancel(context.Background())

	ch, err := client.SchedulesWatch(ctx, 1)
	require.NoError(t, err)
	cancel() // the snapshot is still unread

	assertChanClosed(t, ch)
}

// The same for a later value: the stream is blocked delivering a move when the
// caller cancels.
func TestScheduleStreamCancelWhileDeliveringAMove(t *testing.T) {
	_, _, client, r := pushOnlyClient(t)
	ctx, cancel := context.WithCancel(context.Background())

	ch, err := client.SchedulesWatch(ctx, 1)
	require.NoError(t, err)
	require.True(t, recv(t, ch).NextRequeueAt.IsZero(), "the snapshot")

	r.work.add(1) // delivered, but never read
	cancel()

	assertChanClosed(t, ch)
}
