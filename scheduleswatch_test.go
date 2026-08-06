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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	_, _, err := client.WatchList(ctx)
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

// pushOnlyClient builds a Beehive whose watch poll is set an hour out, so no
// tick can fire inside a test and the only path from the queue to a stream is
// the hub. The option refuses a non-positive interval, so this is how the tick
// is taken out of the picture.
func pushOnlyClient(t *testing.T) (context.Context, *Beehive, Client[cSpec, cStatus], *reconciler) {
	t.Helper()
	bh := newTestBeehive(t, newStore(t), withWatchFloorInterval(time.Hour))
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
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
	r.work.addAfter(1, time.Hour, alarmRequeueAfter)

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

	newer := gaugeValue{Schedule: Schedule{NextRequeueAt: time.Now().Add(time.Hour)}, Seq: 100}
	older := gaugeValue{Schedule: Schedule{NextRequeueAt: time.Now().Add(time.Minute)}, Seq: 99}
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
	r.work.addAfter(1, time.Hour, alarmRequeueAfter)
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
	bh := newTestBeehive(t, newStore(t), fast()...)
	_, err := Register(bh, clientTestGK, &noopController[cSpec, cStatus]{})
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
	r.work.addAfter(1, time.Hour, alarmRequeueAfter)

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
	require.NoError(t, r.work.schedules.Send(1, gaugeValue{Seq: 100}))

	assert.True(t, recv(t, ch).NextRequeueAt.IsZero(), "the snapshot")
	assertQuiet(t, ch, "a value equal to the last reported must not be re-sent")

	// The stream is still live and still delivers a change. Published the same
	// way: an injected Seq sits above anything the gauge will produce, so a real
	// enqueue would now be rejected by Accept — which is a property of this test's
	// injection, not of the queue.
	due := Schedule{NextRequeueAt: time.Now()}
	require.NoError(t, r.work.schedules.Send(1, gaugeValue{Schedule: due, Seq: 101}))
	assert.Equal(t, due, recv(t, ch))
}

// A subscriber that cancels before reading its snapshot ends the stream rather
// than parking the goroutine on a send nobody will take.
func TestScheduleStreamCancelBeforeTheSnapshotIsRead(t *testing.T) {
	_, _, client, _ := pushOnlyClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already given up before the stream produces anything

	ch, err := client.SchedulesWatch(ctx, 1)
	require.NoError(t, err)

	// The stream ends without delivering the snapshot. sendOrDone checks the
	// context before it offers the value, so this holds however the reader and
	// the stream goroutine interleave.
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

// A registered reconciler always has a work queue, because Register builds the
// queue and its hub together. The guard exists so that this path agrees with
// reconciler.scheduleAt behind SchedulesGet, which has kept a nil-queue branch
// for reconcilers built directly in tests: both report having no scheduling
// machinery rather than one answering and its sibling panicking.
func TestScheduleStreamNilQueueReportsNoController(t *testing.T) {
	ctx, _, client, r := pushOnlyClient(t)
	r.work = nil

	_, err := client.SchedulesWatch(ctx, 1)

	assert.ErrorIs(t, err, ErrNoController)
}
