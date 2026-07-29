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

	obj, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)

	store.failures.Store(2) // the first two polls fail
	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)

	ev := recv(t, ch)
	assert.Equal(t, Added, ev.Type, "the stream survives the failed polls and reports the object")
	assert.Equal(t, obj.ID, ev.Object.ID)
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

	obj, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)

	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)
	require.Equal(t, Added, recv(t, ch).Type)

	// A real change is the barrier. Many ticks pass while the object is untouched;
	// if any of them re-sent it, that Modified would arrive carrying the old spec
	// and this assertion would see it instead of the new one.
	_, err = client.Update(ctx, obj.ID, cSpec{Val: "b"})
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

	obj, err := client.Create(ctx, cSpec{Val: "gone"})
	require.NoError(t, err)

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

	foreign, err := NewClient[cSpec, cStatus](bh, other).Create(ctx, cSpec{Val: "foreign"})
	require.NoError(t, err)

	ch, err := NewClient[cSpec, cStatus](bh, clientTestGK).ObjectsWatch(ctx, foreign.ID)
	require.NoError(t, err)

	// The barrier is this client's own object: it is created after the foreign one,
	// so anything the foreign id produced would have to arrive first.
	mine, err := NewClient[cSpec, cStatus](bh, clientTestGK).Create(ctx, cSpec{Val: "mine"})
	require.NoError(t, err)
	mineCh, err := NewClient[cSpec, cStatus](bh, clientTestGK).ObjectsWatchList(ctx)
	require.NoError(t, err)
	require.Equal(t, mine.ID, recv(t, mineCh).Object.ID)

	select {
	case ev := <-ch:
		t.Fatalf("a foreign id must stream nothing, got %+v", ev)
	default:
	}
}

// SchedulesWatch is a gauge, so it repeats nothing: the same value on two
// consecutive polls is one delivery. A subscriber reads it to learn when the next
// requeue is, and a stream that re-sent an unchanged time every tick would be a
// heartbeat rather than a schedule.
func TestSchedulesWatchEmitsOnlyOnChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bh, err := New(newClientTestStore(t), fast()...)
	require.NoError(t, err)
	_, err = Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
	require.NoError(t, err)
	r, ok := bh.reconcilerFor(clientTestGK)
	require.True(t, ok)

	client := NewClient[cSpec, cStatus](bh, clientTestGK)

	// A queued id reports time.Now() on every poll, so its schedule differs every
	// tick. That makes a second watch a usable clock: each value it delivers proves
	// another poll of the shared interval has come round.
	r.work.add(2)
	clock, err := client.SchedulesWatch(ctx, 2)
	require.NoError(t, err)

	ch, err := client.SchedulesWatch(ctx, 1)
	require.NoError(t, err)

	// The zero Schedule is a real gauge value, not an absence, so it is delivered.
	assert.True(t, recv(t, ch).NextRequeueAt.IsZero(), "an unscheduled id reads as the zero Schedule")

	// Three more polls pass with id 1 still unscheduled. Nothing may arrive for it,
	// though the clock keeps ticking throughout.
	for range 3 {
		recv(t, clock)
	}
	select {
	case s := <-ch:
		t.Fatalf("an unchanged schedule must not be re-sent, got %+v", s)
	default:
	}

	// A real change is the next thing the subscriber sees.
	r.work.addAfter(1, testTimeout)
	assert.False(t, recv(t, ch).NextRequeueAt.IsZero(), "the next delivery is the change, not a repeat")
}

// pollProbeStore signals each time the watch surface reads, so a test can wait for
// a poll it expects rather than assuming one has happened. Errors are injected per
// call site, which is what lets a test drive one failure branch at a time.
type pollProbeStore struct {
	Store
	// polled fires once per object-watch tick. It hangs off the store-wide cursor
	// read because that is the one call every tick makes: the listings past it are
	// exactly what a quiet tick skips.
	polled chan struct{}
	// listed fires after a listing returns, so a test can cancel knowing the read
	// already succeeded and the goroutine is on its way to the send.
	listed chan struct{}
	// eventsListed is the event watch's equivalent of listed; metaRead and
	// eventsFailed cover the two reads a tick makes before it gets there.
	eventsListed chan struct{}
	metaRead     chan struct{}
	eventsFailed chan struct{}
	listIDsErr   atomic.Bool
	getErr       atomic.Bool
	eventsErr    atomic.Bool
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

	obj, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)

	store.getErr.Store(true)
	ch, err := client.ObjectsWatch(ctx, obj.ID)
	require.NoError(t, err)

	// Wait for a tick to have failed before letting reads succeed, so the recovery
	// below is the stream outliving a failure rather than never meeting one.
	waitClosed(t, chanAfter(store.polled, 2), "a poll while the read fails")
	store.getErr.Store(false)
	ev := recv(t, ch)
	assert.Equal(t, Added, ev.Type)
	assert.Equal(t, obj.ID, ev.Object.ID)
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

	obj, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)

	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)
	require.Equal(t, Added, recv(t, ch).Type)

	// With the object reported and no further writes, the cursor stops moving, so
	// every tick from here consults the id listing — which now fails.
	store.listIDsErr.Store(true)
	waitClosed(t, chanAfter(store.polled, 2), "polls while the delete check fails")
	store.listIDsErr.Store(false)

	// A real change proves the stream is still live and still diffing.
	_, err = client.Update(ctx, obj.ID, cSpec{Val: "b"})
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

	_, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)

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

	obj, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)

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
	poison, err := store.ObjectsCreate(ctx, &RawObject{
		Group: clientTestGK.Group, Kind: clientTestGK.Kind, Spec: []byte(`not json`),
	})
	require.NoError(t, err)

	ch, err := client.ObjectsWatchList(ctx)
	require.NoError(t, err)
	waitClosed(t, chanAfter(store.polled, 2), "the poll that quarantines the poison row")

	require.NoError(t, store.ObjectsDelete(ctx, poison.ID))

	// A good object created after the removal is the barrier: it can only arrive
	// after the poll that dropped the poison row from the stream's own bookkeeping.
	good, err := client.Create(ctx, cSpec{Val: "good"})
	require.NoError(t, err)

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
	first, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)

	// The id the store will assign next: no row holds it yet, so the kind check
	// finds nothing on every tick.
	next := first.ID + 1
	ch, err := client.EventsWatch(ctx, next)
	require.NoError(t, err)
	waitClosed(t, chanAfter(store.metaRead, 2), "polls while the id is unassigned")

	later, err := client.Create(ctx, cSpec{Val: "b"})
	require.NoError(t, err)
	require.Equal(t, next, later.ID, "the store assigns ids in order")
	require.NoError(t, cc.EventsRecord(ctx, later.ID, EventSpec{Type: EventNormal, Reason: "Started"}))

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

	foreign, err := NewClient[cSpec, cStatus](bh, other).Create(ctx, cSpec{Val: "foreign"})
	require.NoError(t, err)
	require.NoError(t, otherCC.EventsRecord(ctx, foreign.ID, EventSpec{Type: EventNormal, Reason: "Started"}))

	ch, err := client.EventsWatch(ctx, foreign.ID)
	require.NoError(t, err)
	waitClosed(t, chanAfter(store.metaRead, 3), "polls against the foreign id")

	select {
	case ev := <-ch:
		t.Fatalf("another kind's log must not stream through this client, got %+v", ev)
	default:
	}
}

// Both reads an event watch makes happen per tick, so either one failing costs a
// tick rather than the stream — the same contract the object watches keep.
func TestEventsWatchSurvivesReadFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, bh, client, cc := watchFixture(t)
	logger, buf := captureLogger(slog.LevelWarn)
	bh.logger = logger

	obj, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)

	store.metaErr.Store(true) // the kind check fails first
	ch, err := client.EventsWatch(ctx, obj.ID)
	require.NoError(t, err)
	waitClosed(t, chanAfter(store.metaRead, 2), "a poll while the kind check fails")

	store.metaErr.Store(false)
	store.eventsErr.Store(true) // and then the log read
	waitClosed(t, chanAfter(store.eventsFailed, 2), "a poll while the log read fails")
	store.eventsErr.Store(false)

	require.NoError(t, cc.EventsRecord(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Recovered"}))
	assert.Equal(t, "Recovered", recv(t, ch).Reason, "the stream outlived both failures")
	assert.Contains(t, buf.String(), "event watch poll failed", "the skipped polls are reported")
}

// A run that has not moved since it was reported is not sent again. Without that
// comparison every tick would re-deliver the whole window, and a subscriber could
// not tell a new observation from a re-listing of an old one.
func TestEventsWatchEmitsOnlyWhatChanged(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _, client, cc := watchFixture(t)
	obj, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)
	require.NoError(t, cc.EventsRecord(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Probing"}))

	ch, err := client.EventsWatch(ctx, obj.ID)
	require.NoError(t, err)
	require.Equal(t, "Probing", recv(t, ch).Reason)

	// Several polls re-list that run while nothing changes. If any of them re-sent
	// it, the receive below would return "Probing" again instead of the new run.
	waitClosed(t, chanAfter(store.eventsListed, 3), "polls that re-list the unchanged run")

	require.NoError(t, cc.EventsRecord(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Connected"}))
	assert.Equal(t, "Connected", recv(t, ch).Reason, "only the new run is delivered")
}

// An event send is abandoned on cancellation like any other, so a subscriber that
// stops reading cannot strand the goroutine polling on its behalf.
func TestEventsWatchAbandonsASendOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store, _, client, cc := watchFixture(t)

	obj, err := client.Create(ctx, cSpec{Val: "a"})
	require.NoError(t, err)
	require.NoError(t, cc.EventsRecord(ctx, obj.ID, EventSpec{Type: EventNormal, Reason: "Probing"}))

	ch, err := client.EventsWatch(ctx, obj.ID)
	require.NoError(t, err)

	// Nobody reads ch. Waiting for the log read to return puts the cancellation
	// after it and before the send, which is the only place left to observe it.
	waitClosed(t, chanAfter(store.eventsListed, 1), "the log read that found the run")
	cancel()

	waitClosed(t, closedWhenDrained(ch), "the stream to close on cancellation")
}
