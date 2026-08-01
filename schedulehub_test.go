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
	"testing"
	"time"

	"github.com/amorey/gobus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Bus-boundary tests. Each holds a receiver from the hub and starts no stream on
// it, because Peek is a single-consumer read: a live SchedulesWatch takes a
// value the instant it lands, so a Peek racing it reports ErrEmpty on a
// coin-flip and proves nothing.
//
// Peek is what makes these tests mean what they say. Reading alone cannot
// distinguish "Accept rejected the value" from "the value arrived and the
// stream's equality check swallowed it", and the second passes with the hub
// wired without Accept at all.

// hubQueue builds a queue wired to a real hub, as Register does.
func hubQueue() *workQueue {
	q := newWorkQueue()
	h := newScheduleHub()
	q.schedules = h.Sender()
	q.hub = h
	return q
}

func TestHubStaleSendNeverReachesTheSlot(t *testing.T) {
	q := hubQueue()
	rx, _ := q.watchSchedule(1)
	defer rx.Close()

	// Two moves for one id, published in the reverse of the order they became
	// true — the shape two racing publishes produce.
	newer := stamped{Schedule: Schedule{NextRequeueAt: time.Now().Add(time.Hour)}, Seq: 9}
	older := stamped{Schedule: Schedule{NextRequeueAt: time.Now().Add(time.Minute)}, Seq: 8}
	require.NoError(t, q.schedules.Send(1, newer))
	require.NoError(t, q.schedules.Send(1, older))

	ev, err := rx.Peek()
	require.NoError(t, err)
	assert.Equal(t, newer.Schedule, ev.Value.Schedule, "Accept must reject the older Seq")

	ev, err = rx.Recv()
	require.NoError(t, err)
	assert.Equal(t, newer.Schedule, ev.Value.Schedule)
}

// The same shape with the newer value still unread. An earlier draft of the
// upstream package replaced a waiting value on arrival order alone, which would
// have let the stale send evict it.
func TestHubStaleSendDoesNotDisplaceAnUnreadValue(t *testing.T) {
	q := hubQueue()
	rx, _ := q.watchSchedule(1)
	defer rx.Close()

	newer := stamped{Schedule: Schedule{NextRequeueAt: time.Now().Add(time.Hour)}, Seq: 5}
	require.NoError(t, q.schedules.Send(1, newer))
	// newer is now unread in the slot.
	require.NoError(t, q.schedules.Send(1, stamped{Seq: 4}))

	ev, err := rx.Peek()
	require.NoError(t, err)
	assert.Equal(t, newer.Schedule, ev.Value.Schedule)
}

// The value read at subscribe is the prev of the first Accept call, so a publish
// that predates it is rejected by the same rule that rejects a reordered one.
// This is what closes the subscribe seam.
func TestHubSubscribeBaselineRejectsADuplicate(t *testing.T) {
	q := hubQueue()
	q.add(1) // the move completes...

	rx, _ := q.watchSchedule(1) // ...and the subscriber reads it as its baseline
	defer rx.Close()

	// A publish carrying that same move now lands late.
	require.NoError(t, q.schedules.Send(1, q.scheduleAt(1)))

	_, err := rx.Peek()
	assert.ErrorIs(t, err, gobus.ErrEmpty, "the duplicate must never enter the slot")
}

// A receiver watching one id holds nothing for another. The second half is what
// makes the first mean "B did not reach A" rather than "this receiver never
// receives anything", and both fit on one receiver because Peek takes nothing.
func TestHubKeyScope(t *testing.T) {
	q := hubQueue()
	rx, _ := q.watchSchedule(1)
	defer rx.Close()

	q.add(2)
	_, err := rx.Peek()
	assert.ErrorIs(t, err, gobus.ErrEmpty, "another id's move must not reach this receiver")

	q.add(1)
	ev, err := rx.Peek()
	require.NoError(t, err, "this id's move must reach it")
	assert.False(t, ev.Value.Schedule.NextRequeueAt.IsZero())
}

// stop publishes the final values before the sender closes, and a receiver
// holding an unread value is not yet terminal — so the last word is readable.
func TestHubFinalValueIsQueuedBeforeTheSenderCloses(t *testing.T) {
	q := hubQueue()
	q.addAfter(1, time.Hour)
	rx, _ := q.watchSchedule(1)
	defer rx.Close()

	q.stop()
	q.hub.Sender().Close()

	ev, err := rx.Peek()
	require.NoError(t, err, "the final value is unread, so the receiver is not terminal")
	assert.True(t, ev.Value.Schedule.NextRequeueAt.IsZero(), "nothing scheduled")
}

// watchSchedule reads the gauge and registers the watch in one critical section,
// so the baseline is the value current at registration.
func TestHubWatchScheduleSeedsFromTheGauge(t *testing.T) {
	q := hubQueue()
	q.addAfter(1, time.Hour)
	rx, want := q.watchSchedule(1)
	defer rx.Close()

	// Nothing has superseded the baseline, so there is nothing to read...
	_, err := rx.Peek()
	assert.ErrorIs(t, err, gobus.ErrEmpty)

	// ...but the baseline is the prev, so an older publish is still rejected.
	require.NoError(t, q.schedules.Send(1, stamped{Seq: want.Seq - 1}))
	_, err = rx.Peek()
	assert.ErrorIs(t, err, gobus.ErrEmpty)
}
