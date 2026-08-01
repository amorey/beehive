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

// A client-only kind has no reconciler, so no queue, no hub and no stream.
func TestScheduleStreamRequiresAController(t *testing.T) {
	ctx, bh, _, _ := pushOnlyClient(t)
	other := NewClient[cSpec, cStatus](bh, GroupKind{Group: "other", Kind: "Thing"})

	_, err := other.SchedulesWatch(ctx, 1)
	assert.ErrorIs(t, err, ErrNoController)
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
func TestSchedulePublishAfterCloseIsNotFatal(t *testing.T) {
	ctx, bh, _, r := pushOnlyClient(t)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	require.NoError(t, stop(context.Background()))

	assert.NotPanics(t, func() { r.work.publish(keyed{ID: 1}) })
}
